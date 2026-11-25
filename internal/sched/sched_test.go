package sched_test

import (
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/internal/worldtest"
)

// everything runs the whole contract suite as one program: goroutines,
// channels, select, timers, locks and contexts together.
func everything(w *dst.World) {
	for _, c := range worldtest.Checks {
		c.Fn(w)
	}
}

// leaky decides what to send by map iteration order, which Go randomizes
// per run: nondeterminism the simulator doesn't control.
func leaky(w *dst.World) {
	c := dst.MakeChan[int](w, 0)
	w.Go(func() {
		for k := range map[int]bool{1: true, 2: true, 3: true, 4: true} {
			c.Send(k)
		}
		c.Close()
	})
	for v := range c.All() {
		if v%2 == 0 {
			w.Sleep(time.Millisecond)
		}
	}
}

// Verifies: [seed-replays]
// Fails if: a seed's run differs between runs or between GOMAXPROCS
// settings; or the fingerprint misses real nondeterminism.
func TestSeedReplays(t *testing.T) {
	sums := func() []uint64 {
		var out []uint64
		for seed := uint64(1); seed <= 20; seed++ {
			r := simworld.Run(simworld.Options{Seed: seed}, everything)
			if r.Err != nil {
				t.Fatalf("seed %d: %v", seed, r.Err)
			}
			out = append(out, r.Fingerprint)
			out = append(out, r.Checkpoints...)
		}
		return out
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	runtime.GOMAXPROCS(1)
	a := sums()
	runtime.GOMAXPROCS(8)
	b, c := sums(), sums()
	if !slices.Equal(a, b) || !slices.Equal(b, c) {
		t.Fatal("the same seeds gave different runs")
	}
	// Negative: a leak of real nondeterminism changes the fingerprint.
	seen := map[uint64]bool{}
	for range 30 {
		seen[simworld.Run(simworld.Options{Seed: 7}, leaky).Fingerprint] = true
	}
	if len(seen) < 2 {
		t.Fatal("map-order nondeterminism didn't change the fingerprint in 30 runs")
	}
}

// Verifies: [leak-reported]
// Fails if: a goroutine blocked outside the World hangs the run instead of
// failing it with the goroutine's name.
func TestLeakReported(t *testing.T) {
	raw := make(chan int)
	defer close(raw)
	r := simworld.Run(simworld.Options{Seed: 1, Watchdog: 200 * time.Millisecond}, func(w *dst.World) {
		w.GoNamed("leaky", func() { <-raw })
		w.Sleep(time.Second)
	})
	if r.Err == nil || !strings.Contains(r.Err.Error(), "leaky") || !strings.Contains(r.Err.Error(), "outside the World") {
		t.Fatalf("got %v", r.Err)
	}
}

// Verifies: [panic-reported]
// Fails if: a panic isn't reported, or its stack doesn't start at the
// user's code.
func TestPanicReported(t *testing.T) {
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		c := dst.MakeChan[int](w, 0)
		c.Close()
		w.GoNamed("closer", func() { boom(c) })
		w.Sleep(time.Second)
	})
	var pe *sched.PanicError
	if !errors.As(r.Err, &pe) || !strings.Contains(pe.Task, "closer") || pe.Value != "send on closed channel" {
		t.Fatalf("got %v", r.Err)
	}
	if first := strings.SplitN(pe.Stack, "\n", 2)[0]; !strings.Contains(first, "sched_test.boom") {
		t.Fatalf("stack starts at %q, want the user's boom:\n%s", first, pe.Stack)
	}
}

func boom(c dst.Chan[int]) { c.Send(1) }

// Verifies: [crash-is-final]
// Fails if: a crashed machine's goroutine runs again, or its deferred
// functions run at the crash.
func TestCrashIsFinal(t *testing.T) {
	s := simworld.New(simworld.Options{Seed: 3})
	a := s.Machine("a")
	var ticks, deferred atomic.Int32
	afterSelfCrash := false
	r := s.Run(func(w *dst.World) {
		a.World.GoNamed("ticker", func() {
			defer deferred.Add(1)
			for {
				ticks.Add(1)
				a.World.Sleep(time.Millisecond)
			}
		})
		w.Sleep(10 * time.Millisecond)
		if ticks.Load() == 0 {
			panic("the ticker never ran before the crash")
		}
		s.S.Crash(a.M)
		n := ticks.Load()
		w.Sleep(10 * time.Millisecond)
		if ticks.Load() != n || deferred.Load() != 0 {
			panic("a crashed machine's goroutine ran")
		}
		// A goroutine that crashes its own machine never continues.
		s.S.Restart(a.M)
		a.World.GoNamed("self", func() {
			s.S.Crash(a.M)
			afterSelfCrash = true
		})
		w.Sleep(10 * time.Millisecond)
	})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if afterSelfCrash {
		t.Fatal("code after a self-crash ran")
	}
}

// Verifies: [nothing-leaks]
// Fails if: goroutines of a finished run (blocked, crashed, or never
// started) are still alive.
func TestNothingLeaks(t *testing.T) {
	before := runtime.NumGoroutine()
	for seed := uint64(1); seed <= 20; seed++ {
		s := simworld.New(simworld.Options{Seed: seed})
		a := s.Machine("a")
		s.Run(func(w *dst.World) {
			c := dst.MakeChan[int](w, 0)
			for range 5 {
				w.Go(func() { c.Recv() }) // blocked forever
				a.World.Go(func() { a.World.Sleep(time.Hour) })
			}
			w.Sleep(time.Millisecond)
			s.S.Crash(a.M)
			w.Go(func() {}) // never runs: main returns first
		})
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Fatalf("%d goroutines before, %d after", before, n)
	}
}

// Verifies: [one-at-a-time]
// Fails if: two simulated goroutines execute at the same moment.
func TestOneAtATime(t *testing.T) {
	var inside, worst atomic.Int32
	section := func() {
		n := inside.Add(1)
		for m := worst.Load(); n > m && !worst.CompareAndSwap(m, n); m = worst.Load() {
		}
		for range 200 {
			runtime.Gosched()
		}
		inside.Add(-1)
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(8))
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		var wg dst.WaitGroup
		for range 8 {
			wg.Go(w, func() {
				for range 20 {
					section()
					w.Sleep(0)
				}
			})
		}
		wg.Wait()
	})
	if r.Err != nil || worst.Load() != 1 {
		t.Fatalf("err %v, %d at once", r.Err, worst.Load())
	}
	// Negative: the detector sees real goroutines overlap.
	worst.Store(0)
	start := make(chan struct{})
	done := make(chan struct{})
	for range 8 {
		go func() {
			<-start
			for range 20 {
				section()
			}
			done <- struct{}{}
		}()
	}
	close(start)
	for range 8 {
		<-done
	}
	if worst.Load() < 2 {
		t.Fatal("the detector never saw real goroutines overlap")
	}
}
