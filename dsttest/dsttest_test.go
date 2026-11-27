package dsttest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/realworld"
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
	if !errors.As(r.Err, &se) || len(se.Stuck) != 1 || len(se.Slow) != 0 || !strings.Contains(se.Stuck[0], "leaker (started by workload1)") {
		t.Fatalf("got %v", r.Err)
	}
}

// Verifies: [slow-is-not-stuck]
// Fails if: a goroutine that finishes late is called stuck, one blocked
// forever is called slow, or the lateness isn't reported.
func TestSlowIsNotStuck(t *testing.T) {
	r := dsttest.RunSeed(1, dsttest.Options{FaultPhase: time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) { w.Sleep(2500 * time.Millisecond) }) // ends 0.5s after recovery
		h.Go(func(w *dst.World) {
			dst.MakeChan[int](w, 0).Recv() // never
		})
		h.Go(func(w *dst.World) { w.Sleep(time.Millisecond) }) // finishes in time
	}, nil)
	var se *dsttest.StuckError
	if !errors.As(r.Err, &se) {
		t.Fatalf("got %v", r.Err)
	}
	if len(se.Slow) != 1 || !strings.Contains(se.Slow[0], "workload1: finished ~500ms after") {
		t.Fatalf("slow = %q", se.Slow)
	}
	if len(se.Stuck) != 1 || !strings.Contains(se.Stuck[0], "workload2") {
		t.Fatalf("stuck = %q", se.Stuck)
	}
	if !dsttest.IsDeadlock(r.Err) {
		t.Fatal("a stuck goroutine isn't reported as a deadlock")
	}
	// Only slow: not a deadlock.
	r = dsttest.RunSeed(1, dsttest.Options{FaultPhase: time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) { w.Sleep(2500 * time.Millisecond) })
	}, nil)
	if !errors.As(r.Err, &se) || len(se.Stuck) != 0 || len(se.Slow) != 1 || dsttest.IsDeadlock(r.Err) {
		t.Fatalf("got %v", r.Err)
	}
}

// Verifies: [never-crash]
// Fails if: the runner crashes a NeverCrash machine, or stops crashing the
// others.
func TestNeverCrash(t *testing.T) {
	crashes := map[string]int{}
	var mu sync.Mutex
	rep := dsttest.Sweep(dsttest.Options{Seeds: 50, Faults: dsttest.Faults{Crash: true, Disk: true}, FaultPhase: 5 * time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		boots := map[string]int{}
		for _, n := range []string{"safe", "a", "b"} {
			var opts []dsttest.MachineOption
			if n == "safe" {
				opts = append(opts, dsttest.NeverCrash)
			}
			h.Machine(n, func(w *dst.World) {
				boots[n]++
				f, _ := w.FS().OpenFile("f", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
				for {
					f.Write([]byte("x"))
					f.Sync()
					w.Sleep(10 * time.Millisecond)
				}
			}, opts...)
		}
		h.Final("count", func(*dst.World) error {
			mu.Lock()
			defer mu.Unlock()
			for n, b := range boots {
				crashes[n] += b - 1
			}
			return nil
		})
	})
	if !rep.OK() {
		t.Fatalf("%+v", rep.Failed)
	}
	if crashes["safe"] != 0 || crashes["a"] == 0 || crashes["b"] == 0 {
		t.Fatalf("restarts per machine: %v", crashes)
	}
}

// firstSeedHelper reports the sweep's first seed when run as a subprocess.
func TestFirstSeedHelper(t *testing.T) {
	if os.Getenv("DSTTEST_HELPER") != "1" {
		t.Skip("subprocess helper for TestFirstSeedFlag")
	}
	rep := dsttest.Sweep(dsttest.Options{Seeds: 1}, func(h *dsttest.Harness) {})
	fmt.Printf("first=%d\n", rep.FirstSeed)
}

// Verifies: [fresh-seeds]
// Fails if: -dst.first=random repeats the same seed, or -dst.first=N is
// ignored.
func TestFirstSeedFlag(t *testing.T) {
	first := func(arg string) string {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFirstSeedHelper$", "-dst.first="+arg)
		cmd.Env = append(os.Environ(), "DSTTEST_HELPER=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %s", err, out)
		}
		for _, l := range strings.Split(string(out), "\n") {
			if v, ok := strings.CutPrefix(l, "first="); ok {
				return v
			}
		}
		t.Fatalf("no first seed in %s", out)
		return ""
	}
	if got := first("77"); got != "77" {
		t.Fatalf("-dst.first=77 started at %s", got)
	}
	a, b := first("random"), first("random")
	if a == b || a == "1" {
		t.Fatalf("random first seeds %s and %s", a, b)
	}
}

// Verifies: [misuse-reported] (from an invariant)
// Fails if: an Always invariant that uses the World isn't reported as such.
func TestAlwaysMisuse(t *testing.T) {
	r := dsttest.RunSeed(1, dsttest.Options{FaultPhase: time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) { w.Sleep(time.Millisecond) })
		h.Always("sleeps", func() error {
			h.World().Sleep(time.Millisecond) // wrong: invariants only read state
			return nil
		})
	}, nil)
	if r.Err == nil || !strings.Contains(r.Err.Error(), "an invariant (Always)") || !strings.Contains(r.Err.Error(), "dsttest_test.go") {
		t.Fatalf("got %v", r.Err)
	}
}

// Verifies: [skew-explores]
// Fails if: a clock leaves its bounds, fast, slow or at-bound clocks never
// occur, or Synchronized leaves any skew.
func TestSkewExplores(t *testing.T) {
	type clk struct {
		off  time.Duration
		rate float64
	}
	collect := func(c dsttest.ClockOptions) []clk {
		var mu sync.Mutex
		var all []clk
		dsttest.Sweep(dsttest.Options{Seeds: 40, Clock: c, FaultPhase: time.Millisecond, RecoveryPhase: time.Millisecond}, func(h *dsttest.Harness) {
			for _, n := range []string{"a", "b", "c"} {
				m := h.Machine(n, func(w *dst.World) {})
				off, rate := m.Clock()
				mu.Lock()
				all = append(all, clk{off, rate})
				mu.Unlock()
			}
		})
		return all
	}
	const maxOff, maxDrift = 5 * time.Millisecond, 0.01
	var fast, slow, atBound bool
	for _, c := range collect(dsttest.ClockOptions{MaxOffset: maxOff, MaxDrift: maxDrift}) {
		if c.off < -maxOff || c.off > maxOff || c.rate < 1-maxDrift || c.rate > 1+maxDrift {
			t.Fatalf("clock %+v outside the bounds", c)
		}
		fast = fast || c.rate > 1
		slow = slow || c.rate < 1
		atBound = atBound || c.rate == 1+maxDrift || c.rate == 1-maxDrift || c.off == maxOff || c.off == -maxOff
	}
	if !fast || !slow || !atBound {
		t.Fatalf("fast %v, slow %v, at a bound %v", fast, slow, atBound)
	}
	for _, c := range collect(dsttest.ClockOptions{Synchronized: true}) {
		if c.off != 0 || c.rate != 1 {
			t.Fatalf("Synchronized gave clock %+v", c)
		}
	}
}

// Verifies: [pauses-explore]
// Fails if: Pause faults never pause a machine, or one is still paused in
// the recovery phase.
func TestPausesExplore(t *testing.T) {
	rep := dsttest.Sweep(dsttest.Options{Seeds: 30, Faults: dsttest.Faults{Pause: true}, FaultPhase: 5 * time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		for _, n := range []string{"a", "b"} {
			h.Machine(n, func(w *dst.World) {
				for {
					w.Sleep(10 * time.Millisecond)
				}
			})
		}
		h.Always("nothing paused during recovery", func() error {
			if h.Phase() < dsttest.Recovery {
				return nil
			}
			for _, m := range h.Machines() {
				if m.Paused() {
					return fmt.Errorf("%s is paused in phase %v", m.Name(), h.Phase())
				}
			}
			return nil
		})
		h.Expect("machine paused")
	})
	if !rep.OK() {
		t.Fatalf("%+v %v %v", rep.Failed, rep.Nondeterministic, rep.NeverHappened)
	}
}

// pointHarness calls svc.op, other.op and the Buggify point svc.knob every
// millisecond from a machine, recording the phase of each injection (in
// seen, shared across seeds: sweep with Parallel 1).
func pointHarness(seen map[string]map[dsttest.Phase]int) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		note := func(what string) {
			if seen[what] == nil {
				seen[what] = map[dsttest.Phase]int{}
			}
			seen[what][h.Phase()]++
		}
		h.Machine("m", func(w *dst.World) {
			for {
				for _, p := range []string{"svc.op", "other.op"} {
					if f := dst.Inject(w, p); f != dst.None {
						note(p + " " + f.String())
					}
				}
				if w.Buggify("svc.knob") {
					note("svc.knob")
				}
				w.Sleep(time.Millisecond)
			}
		}, dsttest.NeverCrash)
	}
}

// Verifies: [points-only-in-fault-phase]
// Fails if: a fault point fails or a Buggify point fires outside the fault
// phase, or with Faults.Points and Faults.Services off; or (negative) they
// never do in the fault phase.
func TestPointsOnlyInFaultPhase(t *testing.T) {
	points := map[string]dsttest.Point{"svc.": {Fail: 0.3, FailAfter: 0.3}, "svc.knob": {Fire: 0.5}}
	for _, on := range []bool{true, false} {
		seen := map[string]map[dsttest.Phase]int{}
		rep := dsttest.Sweep(dsttest.Options{Seeds: 5, Parallel: 1, Faults: dsttest.Faults{Points: on, Services: on}, Points: points,
			FaultPhase: time.Second, RecoveryPhase: time.Second}, pointHarness(seen))
		if !rep.OK() {
			t.Fatalf("%+v", rep.Failed)
		}
		for what, phases := range seen {
			for ph, n := range phases {
				if !on || ph != dsttest.Faulty {
					t.Errorf("points on=%v: %s happened %d times in phase %v", on, what, n, ph)
				}
			}
		}
		if on && (seen["svc.op Fail"][dsttest.Faulty] == 0 || seen["svc.op FailAfter"][dsttest.Faulty] == 0 || seen["svc.knob"][dsttest.Faulty] == 0) {
			t.Errorf("points on: the fault phase saw %v", seen)
		}
	}
}

// Verifies: [outages-explore]
// Fails if: with Services faults no seed has an outage, or an outage
// fails a point of a service that isn't down (checked through a quiet
// configuration: then only outages fail calls, a whole service at once).
func TestOutagesExplore(t *testing.T) {
	seen := map[string]map[dsttest.Phase]int{}
	rep := dsttest.Sweep(dsttest.Options{Seeds: 20, Parallel: 1, Faults: dsttest.Faults{Services: true}, FaultPhase: 5 * time.Second, RecoveryPhase: time.Second},
		func(h *dsttest.Harness) {
			pointHarness(seen)(h)
			h.Machine("pair", func(w *dst.World) {
				for { // two points of one service: down together or not at all
					a, b := dst.Inject(w, "pair.a"), dst.Inject(w, "pair.b")
					if a != b {
						h.Failf("pair.a %v but pair.b %v at the same moment", a, b)
					}
					w.Sleep(time.Millisecond)
				}
			}, dsttest.NeverCrash)
			h.Expect("service outage")
		})
	if !rep.OK() {
		t.Fatalf("%+v %v", rep.Failed, rep.NeverHappened)
	}
	if seen["svc.op Fail"][dsttest.Faulty] == 0 || seen["other.op Fail"][dsttest.Faulty] == 0 {
		t.Errorf("outages over 20 seeds: %v", seen)
	}
	if seen["svc.op FailAfter"] != nil || seen["svc.knob"] != nil {
		t.Errorf("with only outages on, saw %v", seen)
	}
}

// Verifies: [real-never-injects]
// Fails if: the real World injects a fault or fires a Buggify point, or
// Do or Call doesn't run the operation there; or (negative) a simulated World set
// to always fail doesn't.
func TestRealNeverInjects(t *testing.T) {
	w := realworld.New(realworld.Options{})
	ran := 0
	for range 1000 {
		if dst.Inject(w, "svc.op") != dst.None || w.Buggify("svc.knob") {
			t.Fatal("the real World injected a fault")
		}
		if err := dst.Do(w, "svc.op", func() error { ran++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if ran != 1000 {
		t.Fatalf("Do ran its operation %d of 1000 times", ran)
	}
	if v, err := dst.Call(context.Background(), w, "svc.op", dst.Errors{}, func() (int, error) { return 7, nil }); v != 7 || err != nil {
		t.Fatalf("Call in the real World: %d, %v", v, err)
	}
	var fails, afterRan int
	dsttest.Sweep(dsttest.Options{Seeds: 1, Faults: dsttest.Faults{Points: true}, Points: map[string]dsttest.Point{"svc.": {FailAfter: 1}},
		FaultPhase: 100 * time.Millisecond, RecoveryPhase: 100 * time.Millisecond}, func(h *dsttest.Harness) {
		h.Machine("m", func(w *dst.World) {
			for {
				if errors.Is(dst.Do(w, "svc.op", func() error { afterRan++; return nil }), dst.ErrInjected) {
					fails++
				}
				w.Sleep(time.Millisecond)
			}
		})
	})
	if fails == 0 || afterRan < fails {
		t.Errorf("simulated, always FailAfter: %d injected, op ran %d times", fails, afterRan)
	}
}

// Verifies: [fault-classes-independent]
// Fails if: turning on pauses, service outages, fault points or losses
// changes when a seed's
// crashes and partitions happen; or (negative) a different seed's schedule
// is the same.
func TestFaultClassesIndependent(t *testing.T) {
	schedule := func(seed uint64, f dsttest.Faults) string {
		var b strings.Builder
		dsttest.RunSeed(seed, dsttest.Options{Faults: f, FaultPhase: 5 * time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
			for _, n := range []string{"a", "b", "c"} {
				h.Machine(n, func(w *dst.World) {
					for {
						dst.Inject(w, n+".op")
						w.Sleep(time.Millisecond)
					}
				})
			}
			// Losses scheduled, none allowed: their draws must not move
			// the crashes (a lost machine could no longer crash).
			h.OnLoss(func(*dsttest.Machine) bool { return false }, func(*dsttest.Machine) {})
		}, func(format string, args ...any) {
			line := fmt.Sprintf(format, args...)
			if strings.Contains(line, "nemesis: crash") || strings.Contains(line, "nemesis: restart") || strings.Contains(line, "partition") || strings.Contains(line, "heal") {
				b.WriteString(line + "\n")
			}
		})
		return b.String()
	}
	base := dsttest.Faults{Crash: true, Partition: true}
	more := base
	more.Pause, more.Services, more.Points, more.Loss = true, true, true, true
	compared := 0
	for seed := uint64(1); seed <= 10; seed++ {
		a, b := schedule(seed, base), schedule(seed, more)
		if !strings.Contains(a, "nemesis: crash") {
			continue // crashes off for this seed: little to compare
		}
		if a != b {
			t.Fatalf("seed %d: crashes and partitions moved when pauses and outages were turned on:\n%s---\n%s", seed, a, b)
		}
		if a == schedule(seed+100, base) {
			t.Fatalf("seeds %d and %d have the same schedule: the comparison can't tell schedules apart", seed, seed+100)
		}
		compared++
	}
	if compared < 3 {
		t.Fatalf("only %d of 10 seeds had crashes to compare", compared)
	}
}

// Verifies: [call-fail-never-runs], [call-fail-after-runs], [call-latency-honours-ctx]
// Fails if: Call runs op on Fail, doesn't run it exactly once on FailAfter,
// returns the wrong error or value, waits out injected latency after the
// context is done, or skips op when a context cuts a request short that
// wasn't refused.
func TestCall(t *testing.T) {
	errRefused, errLost := errors.New("refused"), errors.New("lost reply")
	errs := dst.Errors{Before: func() error { return errRefused }, After: func() error { return errLost }}
	var cut, uncut int
	rep := dsttest.Sweep(dsttest.Options{Seeds: 3, Parallel: 1, Faults: dsttest.Faults{Points: true},
		Points: map[string]dsttest.Point{
			"svc.fail":  {Fail: 1},
			"svc.after": {FailAfter: 1},
			"svc.slow":  {Slow: 1, Latency: 20 * time.Millisecond},
		},
		FaultPhase: 5 * time.Second, RecoveryPhase: 100 * time.Millisecond}, func(h *dsttest.Harness) {
		h.Machine("m", func(w *dst.World) {
			w.Sleep(time.Millisecond) // into the fault phase
			bg := context.Background()
			for range 50 {
				ran := 0
				op := func() (int, error) { ran++; return 42, nil }
				if v, err := dst.Call(bg, w, "svc.fail", errs, op); v != 0 || err != errRefused || ran != 0 {
					h.Failf("Fail: got %d, %v; op ran %d times, want 0", v, err, ran)
				}
				if v, err := dst.Call(bg, w, "svc.after", errs, op); v != 0 || err != errLost || ran != 1 {
					h.Failf("FailAfter: got %d, %v; op ran %d times, want 1", v, err, ran)
				}
				if _, err := dst.Call(bg, w, "svc.after", dst.Errors{}, op); !errors.Is(err, dst.ErrInjected) {
					h.Failf("FailAfter with no Errors: %v, want ErrInjected", err)
				}
				ran = 0
				done, stop := dst.WithCancel(w, bg)
				stop()
				if _, err := dst.Call(done, w, "svc.after", errs, op); !errors.Is(err, context.Canceled) || ran != 0 {
					h.Failf("Call with a done context: %v; op ran %d times, want 0 (never sent)", err, ran)
				}
				ctx, cancel := dst.WithTimeout(w, bg, 10*time.Millisecond)
				start := w.Now()
				v, err := dst.Call(ctx, w, "svc.slow", errs, op)
				took := w.Since(start)
				cancel()
				switch {
				case err == nil:
					uncut++
					if v != 42 || ran != 1 || took >= 10*time.Millisecond {
						h.Failf("slow call within the deadline: %d, op ran %d, took %v", v, ran, took)
					}
				case errors.Is(err, context.DeadlineExceeded):
					cut++
					if took > 10*time.Millisecond+time.Microsecond || ran != 1 {
						h.Failf("slow call cut by the deadline took %v (want 10ms); op ran %d times, want 1", took, ran)
					}
				default:
					h.Failf("slow call: %v", err)
				}
			}
		})
	})
	if !rep.OK() {
		t.Fatalf("%v", rep.Failed[0].Err)
	}
	if cut == 0 || uncut == 0 {
		t.Errorf("%d slow calls were cut by their deadline and %d not; want some of each", cut, uncut)
	}
}

// Verifies: [raw-is-the-channel]
// Fails if: Raw in the real World isn't the channel values flow through,
// or Raw of a simulated channel doesn't panic.
func TestChanRaw(t *testing.T) {
	w := realworld.New(realworld.Options{})
	c := dst.MakeChan[int](w, 1)
	c.Send(7)
	if v := <-c.Raw(); v != 7 {
		t.Fatalf("received %d through Raw, want 7", v)
	}
	if dst.RecvOnly(c.Raw()).Raw() != c.Raw() {
		t.Fatal("RecvOnly(c.Raw()).Raw() isn't the same channel")
	}
	var panicked bool
	dsttest.Sweep(dsttest.Options{Seeds: 1, FaultPhase: time.Millisecond, RecoveryPhase: time.Millisecond}, func(h *dsttest.Harness) {
		func() {
			defer func() { panicked = recover() != nil }()
			dst.MakeChan[int](h.World(), 1).Raw()
		}()
	})
	if !panicked {
		t.Fatal("Raw on a simulated channel didn't panic")
	}
}

// Rand.Stream: a named stream is independent of the others (drawing from
// it never changes what another draws), continues across calls, and
// differs by name; the negative case (drawing from the main stream
// itself) shows the check can fail.
func TestRandStream(t *testing.T) {
	// draws runs one seed: extra draws from the stream named extraFrom
	// ("" is the main stream) between the main stream's values.
	draws := func(extraFrom string) (main [3]uint64, other uint64) {
		dsttest.RunSeed(7, dsttest.Options{FaultPhase: time.Millisecond, RecoveryPhase: time.Millisecond}, func(h *dsttest.Harness) {
			h.Machine("m", func(w *dst.World) {
				r := w.Rand()
				for i := range main {
					main[i] = r.Uint64()
					extra := r
					if extraFrom != "" {
						extra = r.Stream(extraFrom)
					}
					for range 50 {
						extra.Uint64()
					}
				}
				other = r.Stream("other").Uint64()
			})
		}, nil)
		return
	}
	base, baseOther := draws("crypto")
	if again, _ := draws("library"); again != base {
		t.Errorf("draws from one stream changed the main stream: %v vs %v", again, base)
	}
	if shifted, _ := draws(""); shifted == base {
		t.Errorf("drawing from the main stream itself didn't shift it: the check can't fail")
	}
	if _, o := draws("library"); o != baseOther {
		t.Errorf("stream %q changed with another stream's use: %d vs %d", "other", o, baseOther)
	}

	dsttest.RunSeed(7, dsttest.Options{FaultPhase: time.Millisecond, RecoveryPhase: time.Millisecond}, func(h *dsttest.Harness) {
		h.Machine("m", func(w *dst.World) {
			a1 := w.Rand().Stream("a").Uint64()
			a2 := w.Rand().Stream("a").Uint64()
			b := w.Rand().Stream("b").Uint64()
			if a1 == a2 {
				t.Errorf("the same name restarted its stream: %d twice", a1)
			}
			if a1 == b {
				t.Errorf("streams a and b drew the same first value %d", a1)
			}
			nested := w.Rand().Stream("a").Stream("x").Uint64()
			if nested == a1 || nested == b {
				t.Errorf("a nested stream repeated another's value")
			}
		})
	}, nil)

	if r := realworld.New(realworld.Options{}).Rand().Stream("x"); r.IntN(10) < 0 {
		t.Error("the real World's stream is unusable")
	}
}

// Start times are drawn per seed: they differ, and some runs straddle a
// midnight, a month boundary and a new year; Options.Start pins them.
// Fails if every seed starts at the same instant or no run crosses a
// boundary.
func TestStartTimes(t *testing.T) {
	const run = 2 * time.Second
	starts := map[time.Time]bool{}
	var day, month, year int
	for seed := uint64(1); seed <= 300; seed++ {
		var start time.Time
		dsttest.RunSeed(seed, dsttest.Options{FaultPhase: run / 2, RecoveryPhase: run / 2}, func(h *dsttest.Harness) {
			h.Machine("m", func(w *dst.World) { start = w.Now() })
		}, nil)
		starts[start.Truncate(time.Millisecond)] = true
		end := start.Add(run).UTC()
		s := start.UTC()
		switch {
		case s.Year() != end.Year():
			year++
		case s.Month() != end.Month():
			month++
		case s.Day() != end.Day():
			day++
		}
	}
	if len(starts) < 250 {
		t.Errorf("only %d distinct start times in 300 seeds", len(starts))
	}
	if day == 0 || month == 0 || year == 0 {
		t.Errorf("runs crossing a midnight %d, a month %d, a year %d: want each", day, month, year)
	}
	pin := time.Date(2031, 6, 1, 12, 0, 0, 0, time.UTC)
	dsttest.RunSeed(5, dsttest.Options{FaultPhase: run / 2, RecoveryPhase: run / 2, Start: pin}, func(h *dsttest.Harness) {
		h.Machine("m", func(w *dst.World) {
			if got := w.Now(); got.Sub(pin).Abs() > time.Second {
				t.Errorf("Options.Start %v: the run started at %v", pin, got)
			}
		})
	}, nil)
}

// With crashes on and fault points off, the swarm slows fault points in
// some seeds (so crashes land between a request's steps) and never fails
// them; with crashes off too, points are quiet. Fails if crash latency
// never happens, or if it ever injects a failure.
func TestCrashLatency(t *testing.T) {
	sweep := func(crash bool) (slow, fail int) {
		rep := dsttest.Sweep(dsttest.Options{Seeds: 60, Faults: dsttest.Faults{Crash: crash}, FaultPhase: time.Second, RecoveryPhase: time.Second},
			func(h *dsttest.Harness) {
				h.Machine("svc", func(w *dst.World) {
					for {
						for _, p := range []string{"store.get", "store.put", "kms.wrap"} {
							dst.Do(w, p, func() error { return nil })
						}
						w.Sleep(time.Millisecond)
					}
				})
			})
		for k, n := range rep.Coverage {
			switch {
			case strings.HasSuffix(k, ": slow"):
				slow += n
			case strings.HasSuffix(k, ": fail"), strings.HasSuffix(k, ": fail-after"), strings.HasSuffix(k, ": outage"):
				fail += n
			}
		}
		return
	}
	if slow, fail := sweep(true); slow == 0 || fail != 0 {
		t.Errorf("crashes on, points off: %d slowed calls (want some), %d failures (want none)", slow, fail)
	}
	if slow, fail := sweep(false); slow != 0 || fail != 0 {
		t.Errorf("crashes and points off: %d slowed, %d failed (want none)", slow, fail)
	}
}

// namedRecorder is a recorder that keeps the real test's name, so the
// cross-process check reruns this very test.
type namedRecorder struct{ recorder }

func (r *namedRecorder) Name() string { return r.TB.Name() }

// Run reruns a seed in a new process of the test binary: a run that
// depends on something made per process (here the process ID) is
// reported, and a deterministic one is not. Fails if either goes wrong.
func TestRunCrossProcessReplay(t *testing.T) {
	opts := dsttest.Options{Seeds: 3, FaultPhase: 100 * time.Millisecond, RecoveryPhase: 100 * time.Millisecond}
	setup := func(perProcess bool) func(h *dsttest.Harness) {
		return func(h *dsttest.Harness) {
			h.Go(func(w *dst.World) {
				d := time.Duration(1+w.Rand().IntN(5)) * time.Millisecond
				if perProcess {
					d = time.Duration(os.Getpid()) * time.Microsecond // differs in every process
				}
				w.Sleep(d)
			})
		}
	}
	t.Run("deterministic", func(t *testing.T) {
		rec := &namedRecorder{recorder{TB: t}}
		dsttest.Run(rec, opts, setup(false))
		if len(rec.errs) > 0 {
			t.Fatalf("a deterministic run was reported: %v", rec.errs)
		}
	})
	t.Run("per-process", func(t *testing.T) {
		if os.Getenv("DSTTEST_REPLAY_CHILD") != "" {
			dsttest.Run(t, opts, setup(true)) // the rerun: record the seed
			return
		}
		rec := &namedRecorder{recorder{TB: t}}
		dsttest.Run(rec, opts, setup(true))
		if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], "plays differently in a new process") {
			t.Fatalf("a run using the process ID wasn't reported: %v", rec.errs)
		}
	})
}

// Feature: on in about half the seeds, the same for a seed every time it
// is asked and every time the seed runs, independent between names,
// pinned by Options.Features, and listed in a failing seed's report.
// Fails if a feature is constant, flips within a run, two features always
// agree, or a pin is ignored.
func TestFeature(t *testing.T) {
	opts := dsttest.Options{FaultPhase: 10 * time.Millisecond, RecoveryPhase: 10 * time.Millisecond}
	on, agree := 0, 0
	for seed := uint64(1); seed <= 400; seed++ {
		var a, b bool
		r := dsttest.RunSeed(seed, opts, func(h *dsttest.Harness) {
			a, b = h.Feature("a"), h.Feature("b")
			if h.Feature("a") != a {
				t.Fatalf("seed %d: feature a changed within the run", seed)
			}
		}, nil)
		again := dsttest.RunSeed(seed, opts, func(h *dsttest.Harness) {
			h.Feature("b") // asked in another order
			if h.Feature("a") != a {
				t.Fatalf("seed %d: feature a differs between runs of the seed", seed)
			}
		}, nil)
		if r.Features["a"] != a || again.Features["a"] != a {
			t.Fatalf("seed %d: Result.Features %v, want a %v", seed, r.Features, a)
		}
		if a {
			on++
		}
		if a == b {
			agree++
		}
	}
	if on < 160 || on > 240 {
		t.Errorf("feature a on in %d of 400 seeds, want about half", on)
	}
	if agree < 160 || agree > 240 {
		t.Errorf("features a and b agree in %d of 400 seeds, want about half (independent)", agree)
	}

	all := opts
	all.Features = map[string]bool{"*": false, "b": true}
	for seed := uint64(1); seed <= 50; seed++ {
		dsttest.RunSeed(seed, all, func(h *dsttest.Harness) {
			if h.Feature("a") || h.Feature("c") || !h.Feature("b") {
				t.Fatalf("seed %d: \"*\" didn't pin the features not named, or overrode a named one", seed)
			}
		}, nil)
	}
	opts.Features = map[string]bool{"a": true}
	for seed := uint64(1); seed <= 50; seed++ {
		dsttest.RunSeed(seed, opts, func(h *dsttest.Harness) {
			if !h.Feature("a") {
				t.Fatalf("seed %d: the pinned feature is off", seed)
			}
		}, nil)
	}

	rec := &recorder{TB: t}
	dsttest.Run(rec, dsttest.Options{Seeds: 5, FaultPhase: 10 * time.Millisecond, RecoveryPhase: 10 * time.Millisecond, Features: map[string]bool{"z": false}},
		func(h *dsttest.Harness) {
			h.Feature("z")
			h.Go(func(w *dst.World) { h.Fail(errors.New("boom")) })
		})
	if len(rec.errs) == 0 || !strings.Contains(rec.errs[0], "\nfeatures: z off\n") {
		t.Fatalf("the report lacks the features: %v", rec.errs)
	}
}

// Verifies: [losses-are-final]
// Fails if: a lost machine boots again (also when it was lost while down,
// with its restart already scheduled), the runner loses a machine the
// test refused or loses none, a loss isn't reported exactly once, or
// Faults.Loss without OnLoss doesn't fail the seed.
func TestLosses(t *testing.T) {
	var mu sync.Mutex
	lostDown, lost := 0, 0
	rep := dsttest.Sweep(dsttest.Options{Seeds: 60, Faults: dsttest.Faults{Crash: true, Loss: true}, FaultPhase: 10 * time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		bootAt := map[string][]time.Time{}
		lostAt := map[string]time.Time{}
		reported := map[string]int{}
		var ms []*dsttest.Machine
		for _, n := range []string{"keep", "a", "b", "c"} {
			ms = append(ms, h.Machine(n, func(w *dst.World) {
				bootAt[n] = append(bootAt[n], h.World().Now())
				for {
					w.Sleep(10 * time.Millisecond)
				}
			}))
		}
		wasDown := map[string]bool{} // as each candidate was when the runner asked
		h.OnLoss(func(m *dsttest.Machine) bool {
			wasDown[m.Name()] = m.Dead()
			return m.Name() != "keep" && len(lostAt) < 2
		}, func(m *dsttest.Machine) {
			if wasDown[m.Name()] {
				mu.Lock()
				lostDown++
				mu.Unlock()
			}
			if !m.Lost() || !m.Dead() {
				h.Fail(fmt.Errorf("%s reported lost but Lost %v, Dead %v", m.Name(), m.Lost(), m.Dead()))
			}
			if _, again := lostAt[m.Name()]; again {
				h.Fail(fmt.Errorf("%s lost twice", m.Name()))
			}
			reported[m.Name()]++
			lostAt[m.Name()] = h.World().Now()
		})
		h.Final("losses", func(*dst.World) error {
			for _, m := range ms {
				at, ok := lostAt[m.Name()]
				if m.Lost() != ok || (ok && reported[m.Name()] != 1) {
					return fmt.Errorf("%s: Lost %v, reported %d times", m.Name(), m.Lost(), reported[m.Name()])
				}
				if !ok {
					continue
				}
				if m.Name() == "keep" {
					return fmt.Errorf("lost a machine the test refused")
				}
				boots := bootAt[m.Name()]
				if last := boots[len(boots)-1]; last.After(at) {
					return fmt.Errorf("%s booted at %v after it was lost at %v", m.Name(), last, at)
				}
				if !m.Dead() {
					return fmt.Errorf("%s is lost but running", m.Name())
				}
			}
			mu.Lock()
			defer mu.Unlock()
			lost += len(lostAt)
			return nil
		})
	})
	if !rep.OK() {
		t.Fatalf("%v", rep.Failed[0].Err)
	}
	if lost == 0 || lostDown == 0 {
		t.Fatalf("losses %d (of which while down %d) in %d seeds: want both", lost, lostDown, rep.Runs)
	}
	t.Logf("%d losses in %d seeds", lost, rep.Runs)
	// Negative: Loss without OnLoss.
	r := dsttest.RunSeed(3, dsttest.Options{Faults: dsttest.Faults{Loss: true}, FaultPhase: 10 * time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		h.Machine("m", func(w *dst.World) { w.Sleep(time.Hour) })
	}, nil)
	if r.Err == nil || !strings.Contains(r.Err.Error(), "OnLoss") {
		t.Fatalf("Faults.Loss without OnLoss: %v", r.Err)
	}
}

// Verifies: [boots-are-no-ones-work]
// Fails if: a machine crashed and restarted by a workload goroutine is
// reported as that goroutine's unfinished work.
func TestRestartFromWorkload(t *testing.T) {
	rep := dsttest.Sweep(dsttest.Options{Seeds: 5, FaultPhase: time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		m := h.Machine("server", func(w *dst.World) {
			for {
				w.Sleep(10 * time.Millisecond)
			}
		})
		h.Go(func(w *dst.World) {
			w.Sleep(100 * time.Millisecond)
			m.Crash()
			m.Restart()
		})
	})
	if !rep.OK() {
		t.Fatalf("%v", rep.Failed[0].Err)
	}
}
