package dsttest_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// ---- a tiny durable log, with a planted bug ----

// logServer appends records to a log file, acking each one. With fsync it
// acks only after Sync; without, before (the bug: acked data can be lost).
func logServer(fsync bool, acked *[]string) func(w *dst.World) {
	return func(w *dst.World) {
		fsys := w.FS()
		f, err := fsys.OpenFile("log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			panic(err)
		}
		if err := dst.SyncDir(fsys, "."); err != nil {
			panic(err)
		}
		for i := 0; ; i++ {
			rec := fmt.Sprintf("%d-%d\n", w.Now().UnixNano(), i)
			if _, err := f.Write([]byte(rec)); err != nil {
				panic(err)
			}
			if fsync {
				if err := f.Sync(); err != nil {
					panic(err)
				}
			}
			*acked = append(*acked, rec)
			w.Sleep(time.Duration(1+w.Rand().IntN(20)) * time.Millisecond)
		}
	}
}

func logSetup(fsync bool) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		var acked []string
		db := h.Machine("db", logServer(fsync, &acked))
		h.Final("acked records survive", func(w *dst.World) error {
			b, err := dst.ReadFile(db.World().FS(), "log")
			if err != nil {
				return err
			}
			for _, rec := range acked {
				if !bytes.Contains(b, []byte(rec)) {
					return fmt.Errorf("acked record %q is gone", strings.TrimSpace(rec))
				}
			}
			return nil
		})
	}
}

var crashes = dsttest.Options{Seeds: 100, Faults: dsttest.Faults{Crash: true}, FaultPhase: 2 * time.Second, RecoveryPhase: time.Second}

// Verifies: [finds-planted-bugs], [failures-replay]
// Fails if: the runner misses a log that acks before fsync, flags the
// correct log, or a failing seed doesn't replay identically.
func TestFindsLostWrites(t *testing.T) {
	if rep := dsttest.Sweep(crashes, logSetup(true)); !rep.OK() {
		t.Fatalf("the correct log failed: %+v", rep.Failed)
	}
	rep := dsttest.Sweep(crashes, logSetup(false))
	if len(rep.Failed) == 0 {
		t.Fatal("100 seeds never caught a log that acks before fsync")
	}
	f := rep.Failed[0]
	var fe *dsttest.FinalError
	if !errors.As(f.Err, &fe) {
		t.Fatalf("seed %d failed with %v, want a failed final check", f.Seed, f.Err)
	}
	again := dsttest.RunSeed(f.Seed, crashes, logSetup(false), nil)
	if again.Err == nil || again.Err.Error() != f.Err.Error() || again.Fingerprint != f.Fingerprint {
		t.Fatalf("seed %d didn't replay: %v vs %v", f.Seed, f.Err, again.Err)
	}
	if len(rep.Nondeterministic) > 0 {
		t.Fatal(rep.Nondeterministic[0])
	}
}

// Verifies: [finds-planted-bugs]
// Fails if: a goroutine pool that can deadlock passes 100 seeds.
func TestFindsDeadlock(t *testing.T) {
	pool := func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) {
			jobs := dst.MakeChan[int](w, 2)
			results := dst.MakeChan[int](w, 2)
			for range 3 {
				w.Go(func() {
					for j := range jobs.All() {
						w.Sleep(time.Duration(w.Rand().IntN(5)) * time.Millisecond)
						if w.Rand().IntN(3) == 0 {
							jobs.Send(j) // retry by re-enqueueing: can deadlock
							continue
						}
						results.Send(j)
					}
				})
			}
			w.Go(func() {
				for i := range 20 {
					jobs.Send(i)
				}
			})
			for range 20 {
				results.Recv()
			}
		})
	}
	rep := dsttest.Sweep(dsttest.Options{Seeds: 100, FaultPhase: time.Second, RecoveryPhase: time.Second}, pool)
	if len(rep.Failed) == 0 || !dsttest.IsDeadlock(rep.Failed[0].Err) {
		t.Fatalf("want a deadlock, got %+v", rep.Failed)
	}
}

// Verifies: [nondeterminism-reported]
// Fails if: behaviour decided by map order isn't reported.
func TestNondeterminismReported(t *testing.T) {
	leaky := func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) {
			for k := range map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true} {
				w.Sleep(time.Duration(k) * time.Millisecond)
			}
		})
	}
	rep := dsttest.Sweep(dsttest.Options{Seeds: 10, DeterminismChecks: 10, FaultPhase: time.Second, RecoveryPhase: time.Second}, leaky)
	if len(rep.Nondeterministic) == 0 {
		t.Fatal("map-order nondeterminism wasn't reported")
	}
	// Negative: deterministic code isn't.
	det := func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) { w.Sleep(time.Millisecond) })
	}
	if rep := dsttest.Sweep(dsttest.Options{Seeds: 10, DeterminismChecks: 10}, det); len(rep.Nondeterministic) > 0 {
		t.Fatal(rep.Nondeterministic[0])
	}
}

// Verifies: [always-checked]
// Fails if: a broken invariant isn't caught at the step it breaks.
func TestAlwaysChecked(t *testing.T) {
	r := dsttest.RunSeed(1, dsttest.Options{}, func(h *dsttest.Harness) {
		n := 0
		h.Go(func(w *dst.World) {
			for {
				n++
				w.Sleep(time.Millisecond)
			}
		})
		h.Always("n stays under 50", func() error {
			if n >= 50 {
				return fmt.Errorf("n = %d", n)
			}
			return nil
		})
	}, nil)
	var ie *dsttest.InvariantError
	if !errors.As(r.Err, &ie) || !strings.Contains(r.Err.Error(), "n = 50") || r.SimTime > 60*time.Millisecond {
		t.Fatalf("got %v at %v", r.Err, r.SimTime)
	}
}

// Verifies: [recovery-phase]
// Fails if: a machine is down or the network cut during recovery, or no
// faults happened before it.
func TestRecoveryPhase(t *testing.T) {
	rep := dsttest.Sweep(dsttest.Options{Seeds: 30, Faults: dsttest.AllFaults, FaultPhase: 5 * time.Second, RecoveryPhase: 2 * time.Second}, func(h *dsttest.Harness) {
		for _, n := range []string{"a", "b", "c"} {
			// Synced writes in a loop: power loss inside I/O must also stop
			// for the recovery phase (it once didn't).
			h.Machine(n, func(w *dst.World) {
				f, err := w.FS().OpenFile("f", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
				if err != nil {
					panic(err)
				}
				for {
					f.Write([]byte("x"))
					f.Sync()
					w.Sleep(time.Millisecond)
				}
			})
		}
		h.Always("everything is up during recovery", func() error {
			if h.Phase() < dsttest.Recovery {
				return nil
			}
			for _, m := range h.Machines() {
				if m.Dead() {
					return fmt.Errorf("%s is down in phase %v", m.Name(), h.Phase())
				}
			}
			return nil
		})
		h.Expect("machine crashed", "network partitioned")
	})
	if !rep.OK() {
		t.Fatalf("%+v %v %v", rep.Failed, rep.Nondeterministic, rep.NeverHappened)
	}
}

// Verifies: [expected-coverage]
// Fails if: a behaviour that never happened isn't reported.
func TestExpectedCoverage(t *testing.T) {
	rep := dsttest.Sweep(dsttest.Options{Seeds: 5}, func(h *dsttest.Harness) {
		h.Expect("this never happens", "this does")
		h.Sometimes("this does")
	})
	if len(rep.NeverHappened) != 1 || rep.NeverHappened[0] != "this never happens" {
		t.Fatalf("NeverHappened = %v", rep.NeverHappened)
	}
}

// recorder captures what Run reports instead of failing the real test.
type recorder struct {
	testing.TB
	errs, logs []string
}

func (r *recorder) Helper()                      {}
func (r *recorder) Name() string                 { return "TestStore/sub" }
func (r *recorder) Errorf(f string, args ...any) { r.errs = append(r.errs, fmt.Sprintf(f, args...)) }
func (r *recorder) Error(args ...any)            { r.errs = append(r.errs, fmt.Sprint(args...)) }
func (r *recorder) Logf(f string, args ...any)   { r.logs = append(r.logs, fmt.Sprintf(f, args...)) }
func (r *recorder) Fatalf(f string, args ...any) { panic(fmt.Sprintf(f, args...)) }

// Run reports a failing seed with its fault mix and the replay command.
func TestRunReportsReplay(t *testing.T) {
	rec := &recorder{TB: t}
	dsttest.Run(rec, crashes, logSetup(false))
	if len(rec.errs) == 0 {
		t.Fatal("no failure reported")
	}
	msg := rec.errs[0]
	for _, want := range []string{"failed: final check", "faults: crash every", "replay: go test -run '^TestStore$' -dst.seed="} {
		if !strings.Contains(msg, want) {
			t.Fatalf("report lacks %q:\n%s", want, msg)
		}
	}
	rec = &recorder{TB: t}
	dsttest.Run(rec, crashes, logSetup(true))
	if len(rec.errs) > 0 || !strings.Contains(strings.Join(rec.logs, "\n"), "machine crashed") {
		t.Fatalf("errors %v, logs %v", rec.errs, rec.logs)
	}
}

// Verifies: [workload-finishes]
// Fails if: a goroutine leaked by a workload goroutine goes unreported, or
// a background goroutine started from setup is.
func TestLeakedGoroutineReported(t *testing.T) {
	leak := func(h *dsttest.Harness) {
		h.World().Go(func() { // background: allowed to run forever
			for {
				h.World().Sleep(time.Millisecond)
			}
		})
		h.Go(func(w *dst.World) {
			ch := dst.MakeChan[int](w, 0)
			w.GoNamed("leaker", func() { ch.Send(1) }) // nobody receives
		})
	}
	r := dsttest.RunSeed(1, dsttest.Options{FaultPhase: time.Second, RecoveryPhase: time.Second}, leak, nil)
	var se *dsttest.StuckError
	if !errors.As(r.Err, &se) || len(se.Goroutines) != 1 || !strings.Contains(se.Goroutines[0], "leaker (started by workload1)") {
		t.Fatalf("got %v", r.Err)
	}
}
