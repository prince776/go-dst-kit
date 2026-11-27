package wal_test

import (
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/wal"
	"github.com/prince776/go-dst-kit/realworld"
)

// harness runs a machine that appends records forever and checks, at each
// boot and after a final crash, that every acknowledged record is there,
// in order, with nothing else in between.
func harness(bugs wal.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		var acked []string // acknowledged records, in order
		var written []string
		boots := 0
		check := func(got [][]byte) error {
			var have []string
			for _, r := range got {
				have = append(have, string(r))
			}
			// Recovered records are a prefix of what was written...
			if len(have) > len(written) || !slices.Equal(have, written[:len(have)]) {
				return fmt.Errorf("recovered %d records that aren't a prefix of the %d written", len(have), len(written))
			}
			// ...that includes everything acknowledged.
			if len(have) < len(acked) {
				return fmt.Errorf("lost acknowledged records: recovered %d, %d were acknowledged", len(have), len(acked))
			}
			return nil
		}
		db := h.Machine("db", func(w *dst.World) {
			boots++
			// The directory must be durable too: create it and sync its
			// parent. (The first version of this harness didn't, and the
			// simulator caught the log vanishing with its directory.) A
			// full disk or an I/O error fails these steps for a while: try
			// again, as a service retries its startup.
			var l *wal.Log
			for {
				err := w.FS().MkdirAll("data", 0o755)
				if err == nil {
					err = dst.SyncDir(w.FS(), ".")
				}
				if err == nil {
					l, err = wal.Open(w.FS(), "data/wal", bugs)
				}
				if err == nil {
					break
				}
				h.Sometimes("opening the log failed")
				w.Sleep(50 * time.Millisecond)
			}
			if l.TornTail {
				h.Sometimes("recovered a torn tail")
			}
			if err := check(l.Records()); err != nil {
				h.Failf("boot %d: %v", boots, err)
				return
			}
			// Unacknowledged records that didn't survive are gone for good.
			written = written[:len(l.Records())]
			acked = acked[:min(len(acked), len(written))]
			for i := 0; ; i++ {
				rec := fmt.Sprintf("boot%d-%d", boots, i)
				written = append(written, rec)
				if _, err := l.Append([]byte(rec)); err != nil {
					// A failed Append's record never appears.
					written = written[:len(written)-1]
					h.Sometimes("an append failed")
					w.Sleep(time.Duration(1+w.Rand().IntN(30)) * time.Millisecond)
					continue
				}
				acked = append(acked, rec)
				w.Sleep(time.Duration(1+w.Rand().IntN(30)) * time.Millisecond)
			}
		})
		h.Final("acked records survive a last crash", func(w *dst.World) error {
			db.Crash()
			got, err := wal.ReadWith(db.World().FS(), "data/wal", bugs)
			if err != nil && !errors.Is(err, fs.ErrNotExist) { // a vanished log is an empty one
				return fmt.Errorf("read: %v", err)
			}
			return check(got)
		})
		h.Expect("recovered a torn tail")
	}
}

var opts = dsttest.Options{
	Seeds:         300,
	Faults:        dsttest.Faults{Crash: true, Disk: true},
	FaultPhase:    5 * time.Second,
	RecoveryPhase: time.Second,
}

// Verifies: [acked-is-durable], [recovery-is-a-prefix]
// Fails if: the correct log loses or invents records under crashes, torn
// writes and power loss inside I/O.
func TestWAL(t *testing.T) {
	dsttest.Run(t, opts, harness(wal.Bugs{}))
}

// Each planted bug must be caught.
func TestPlantedBugs(t *testing.T) {
	for name, bugs := range map[string]wal.Bugs{
		"NoFsync":            {NoFsync: true},
		"NoDirSync":          {NoDirSync: true},
		"NoTruncateTornTail": {NoTruncateTornTail: true},
		"ChecksumDataOnly":   {ChecksumDataOnly: true}, // found only with holes (out-of-order write-back)
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			err := rep.Failed[0].Err.Error()
			if !strings.Contains(err, "lost acknowledged records") && !strings.Contains(err, "aren't a prefix") {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, err)
		})
	}
}

// diskErrors adds full disks and I/O errors (a failed fsync's data lost
// for good) to the crash faults.
var diskErrors = func() dsttest.Options {
	o := opts
	o.Faults.DiskErrors = true
	return o
}()

// Verifies: [acked-is-durable], [recovery-is-a-prefix] under disk errors,
// and [failed-append-vanishes].
// Fails if: the correct log loses or invents records when the disk fills
// and fsyncs, writes and reads fail; or NoRepairAfterError, which appends
// after a failed one, isn't caught.
func TestWALDiskErrors(t *testing.T) {
	dsttest.Run(t, diskErrors, func(h *dsttest.Harness) {
		harness(wal.Bugs{})(h)
		h.Expect("an append failed")
	})
	o := diskErrors
	o.DeterminismChecks = -1
	rep := dsttest.Sweep(o, harness(wal.Bugs{NoRepairAfterError: true}))
	if len(rep.Failed) == 0 {
		t.Fatalf("%d seeds never caught NoRepairAfterError", rep.Runs)
	}
	err := rep.Failed[0].Err.Error()
	if !strings.Contains(err, "lost acknowledged records") && !strings.Contains(err, "aren't a prefix") {
		t.Fatalf("NoRepairAfterError caught for the wrong reason: %v", err)
	}
	t.Logf("NoRepairAfterError caught on %d of %d seeds, first %d: %v", len(rep.Failed), rep.Runs, rep.Failed[0].Seed, err)
}

// Verifies: [real-and-simulated]
// Fails if: the log doesn't work on the real filesystem.
func TestRealFilesystem(t *testing.T) {
	w := realworld.New(realworld.Options{Dir: t.TempDir()})
	l, err := wal.Open(w.FS(), "wal", wal.Bugs{})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := l.Append([]byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	l, err = wal.Open(w.FS(), "wal", wal.Bugs{})
	if err != nil || len(l.Records()) != 3 || string(l.Records()[2]) != "2" {
		t.Fatalf("reopened: %v %q", err, l.Records())
	}
}
