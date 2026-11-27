package twophase_test

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/twophase"
	"github.com/prince776/go-dst-kit/examples/wal"
)

var parts = []string{"p1:4000", "p2:4000"}

func harness(bugs twophase.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		h.Machine("coord", func(w *dst.World) {
			c, err := twophase.StartCoordinator(w, parts, bugs)
			for transient(err) { // a full disk or an I/O error: retry the startup
				h.Sometimes("a startup failed on the disk")
				w.Sleep(50 * time.Millisecond)
				c, err = twophase.StartCoordinator(w, parts, bugs)
			}
			if err != nil {
				h.Failf("coordinator: %v", err)
				return
			}
			ln, err := w.Net().Listen("tcp", "coord:4000")
			if err != nil {
				h.Failf("listen: %v", err)
				return
			}
			c.Serve(ln)
		})
		var machines []*dsttest.Machine
		for _, name := range []string{"p1", "p2"} {
			machines = append(machines, h.Machine(name, func(w *dst.World) {
				p, err := twophase.StartParticipant(w, "coord:4000", bugs)
				for transient(err) {
					h.Sometimes("a startup failed on the disk")
					w.Sleep(50 * time.Millisecond)
					p, err = twophase.StartParticipant(w, "coord:4000", bugs)
				}
				if err != nil {
					h.Failf("%s: %v", name, err)
					return
				}
				ln, err := w.Net().Listen("tcp", name+":4000")
				if err != nil {
					h.Failf("listen: %v", err)
					return
				}
				p.Serve(ln)
			}))
		}
		told := map[int64]string{} // what clients were told
		for range 2 {
			h.Go(func(w *dst.World) {
				// Load during the fault phase only, so the workload is done
				// when recovery ends.
				for range 15 {
					if h.Phase() != dsttest.Faulty {
						return
					}
					from := w.Rand().IntN(2)
					tx, d, err := twophase.Transfer(w, "coord:4000", from, 1-from, int64(1+w.Rand().IntN(40)))
					if err != nil {
						h.Sometimes("transfer outcome unknown")
					} else {
						told[tx] = d
						h.Sometimes(map[string]string{"commit": "transfer committed", "abort": "transfer aborted"}[d])
					}
					w.Sleep(time.Duration(w.Rand().IntN(100)) * time.Millisecond)
				}
			})
		}
		h.Final("transfers are atomic and conserve money", func(w *dst.World) error {
			total := int64(0)
			var outcomes []map[int64]string
			for i, m := range machines {
				m.Crash() // judge the durable state
				recs, err := wal.Read(m.World().FS(), twophase.LogFile)
				if err != nil {
					return err
				}
				bal, inDoubt, out := twophase.Replay(recs)
				if len(inDoubt) > 0 {
					return fmt.Errorf("p%d still in doubt about %v", i+1, slices.Sorted(maps.Keys(inDoubt)))
				}
				if bal < 0 {
					return fmt.Errorf("p%d has balance %d", i+1, bal)
				}
				total += bal
				outcomes = append(outcomes, out)
			}
			txs := map[int64]bool{}
			for _, o := range outcomes {
				for tx := range o {
					txs[tx] = true
				}
			}
			for tx := range told {
				txs[tx] = true
			}
			for _, tx := range slices.Sorted(maps.Keys(txs)) {
				c1, c2 := outcomes[0][tx] == "commit", outcomes[1][tx] == "commit"
				if c1 != c2 {
					return fmt.Errorf("tx %d committed at p1: %v, at p2: %v", tx, c1, c2)
				}
				if d, ok := told[tx]; ok && (d == "commit") != c1 {
					return fmt.Errorf("client told %q for tx %d, but committed is %v", d, tx, c1)
				}
			}
			if total != 2*twophase.Initial {
				return fmt.Errorf("total balance %d, want %d", total, 2*twophase.Initial)
			}
			return nil
		})
		h.Expect("transfer committed", "transfer aborted", "transfer outcome unknown")
	}
}

var opts = dsttest.Options{
	Seeds:         300,
	Faults:        dsttest.AllFaults,
	FaultPhase:    5 * time.Second,
	RecoveryPhase: 3 * time.Second,
}

// Verifies: [atomic], [conserves], [resolves]
// Fails if: a transaction commits at one participant only, money appears
// or vanishes, a client is told the wrong outcome, or a participant stays
// in doubt after faults stop.
func TestTwoPhase(t *testing.T) {
	dsttest.Run(t, opts, harness(twophase.Bugs{}))
}

// Each planted bug must be caught as a broken transaction.
func TestPlantedBugs(t *testing.T) {
	for name, bugs := range map[string]twophase.Bugs{
		"DecideAlone":   {DecideAlone: true},
		"SendBeforeLog": {SendBeforeLog: true},
		"NoPrepareLog":  {NoPrepareLog: true},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			err := rep.Failed[0].Err.Error()
			if !strings.Contains(err, "committed at p1") && !strings.Contains(err, "total balance") && !strings.Contains(err, "client told") {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, err)
		})
	}
}

// transient reports whether err is a disk error that passes: a full disk
// (until the faults stop) or an I/O error.
func transient(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EIO)
}
