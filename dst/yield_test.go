package dst_test

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

// spin waits for flag with w.Yield, as code spinning on runtime.Gosched
// does, and returns how many times it yielded.
func spin(w *dst.World, flag *atomic.Bool) int {
	n := 0
	for !flag.Load() {
		w.Yield()
		n++
	}
	return n
}

// Verifies: [yield-waits-for-progress]
// Fails if: a goroutine spinning on Yield while the one it waits for
// sleeps doesn't end at the sleeper's wake-up (time can't advance under
// a spin: the run hits its step limit), spins more than a handful of
// times, or ends before the flag is set; a spin with a busy third
// goroutine doesn't let that one run too; or a spin that nothing can end
// isn't reported as a deadlock naming Yield (it loops to the step limit).
func TestYieldWaitsForProgress(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		var flag atomic.Bool
		var spins int
		var at time.Duration
		var start time.Time
		res := simworld.Run(simworld.Options{Seed: seed, MaxSteps: 10_000, PCTDepth: int(seed % 3), PCTHorizon: 100}, func(w *dst.World) {
			start = w.Now()
			var wg dst.WaitGroup
			wg.Go(w, func() {
				w.Sleep(10 * time.Millisecond)
				flag.Store(true)
			})
			wg.Go(w, func() { // busy, but never sets the flag
				for range 20 {
					w.Sleep(0)
				}
			})
			spins = spin(w, &flag)
			at = w.Since(start)
			wg.Wait()
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if at < 10*time.Millisecond {
			t.Fatalf("seed %d: the spin ended at %v, before the flag was set", seed, at)
		}
		if spins > 30 {
			t.Fatalf("seed %d: %d spins for one sleeper and 20 steps of another goroutine", seed, spins)
		}
	}

	// Nobody will ever set the flag: a deadlock, not an endless loop.
	var flag atomic.Bool
	res := simworld.Run(simworld.Options{Seed: 1, MaxSteps: 10_000}, func(w *dst.World) { spin(w, &flag) })
	var dl *sched.DeadlockError
	if !errors.As(res.Err, &dl) || !strings.Contains(res.Err.Error(), "Yield") {
		t.Fatalf("a spin no one can end: got %v, want a deadlock naming Yield", res.Err)
	}

	// The real World: Yield returns (it is runtime.Gosched).
	w := realworld.New(realworld.Options{})
	flag.Store(false)
	go func() { time.Sleep(time.Millisecond); flag.Store(true) }()
	spin(w, &flag)
}

// Verifies: [gosched-stays-runnable]
// Fails if: a loop that makes its own progress and calls Gosched waits
// for time to advance (it would finish at the next timer, an hour on,
// as with Yield), or Gosched never lets another runnable goroutine run
// before it goes on.
func TestGoschedStaysRunnable(t *testing.T) {
	otherFirst := false
	for seed := uint64(1); seed <= 50; seed++ {
		var took time.Duration
		var otherRan, otherAtYield bool
		res := simworld.Run(simworld.Options{Seed: seed, MaxSteps: 10_000}, func(w *dst.World) {
			var wg dst.WaitGroup
			wg.Go(w, func() { w.Sleep(time.Hour) }) // the next timer is far off
			w.Sleep(time.Millisecond)               // the sleeper is asleep now
			start := w.Now()
			otherRan = false
			wg.Go(w, func() { otherRan = true })
			ranAtSpawn := otherRan
			for i := 0; i < 10; i++ { // makes its own progress
				w.Gosched()
				if i == 0 && !ranAtSpawn && otherRan {
					otherAtYield = true
				}
			}
			took = w.Since(start)
			wg.Wait()
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if took != 0 {
			t.Fatalf("seed %d: a self-progressing loop on Gosched took %v of simulated time, want 0", seed, took)
		}
		otherFirst = otherFirst || otherAtYield
	}
	if !otherFirst {
		t.Fatal("in 50 seeds, no runnable goroutine ever ran at a Gosched before the loop went on")
	}
}
