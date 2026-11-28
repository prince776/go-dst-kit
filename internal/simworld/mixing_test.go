package simworld_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Mixing rule (a): primitives a simulation used, contended there, work
// for real goroutines once it has ended (run under -race: the hand-over
// is clean).
// Fails if a primitive carries simulated state into the real World: a
// lock that stays held or a group that doesn't count, a Once that runs
// again, a Cond that loses a real Signal.
func TestPrimitivesAfterASimulation(t *testing.T) {
	var (
		mu   dst.Mutex
		rw   dst.RWMutex
		wg   dst.WaitGroup
		once dst.Once
		cond = dst.NewCond(&mu)
		lit  = &dst.Cond{L: &mu}
		n    int
		runs int
	)
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		for range 4 {
			wg.Go(w, func() {
				for range 10 {
					mu.Lock()
					n++
					cond.Broadcast()
					mu.Unlock()
					rw.RLock()
					rw.RUnlock()
					once.Do(func() { runs++ })
				}
			})
		}
		mu.Lock()
		for n < 40 {
			cond.Wait()
		}
		mu.Unlock()
		wg.Wait()
	})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	var real sync.WaitGroup // the test's own, to check dst's against
	for _, c := range []*dst.Cond{cond, lit} {
		ready := make(chan struct{})
		real.Add(1)
		go func() {
			defer real.Done()
			mu.Lock()
			close(ready)
			for n < 41 {
				c.Wait()
			}
			mu.Unlock()
		}()
		<-ready
		mu.Lock()
		n++
		c.Broadcast()
		mu.Unlock()
		real.Wait()
		n--
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				mu.Lock()
				n++
				mu.Unlock()
				rw.Lock()
				rw.Unlock()
				once.Do(func() { runs++ })
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the real World's WaitGroup.Wait never returned")
	}
	if n != 40+800 || runs != 1 {
		t.Fatalf("n = %d, Once ran %d times; want %d and 1", n, runs, 40+800)
	}
}

// Mixing rule (c): a simulated goroutine waiting for a dst.Mutex a real
// goroutine holds isn't woken by the real Unlock (the real path doesn't
// read the simulation's state); the run fails as a deadlock naming the
// lock, rather than hanging.
// Fails if the run hangs, passes, or fails without naming the lock.
func TestRealUnlockOfASimulatedWaiterIsADeadlock(t *testing.T) {
	var mu dst.Mutex
	var unlocked atomic.Bool
	mu.Lock() // held by the test's goroutine, a real one
	locking := make(chan struct{})
	go func() {
		<-locking
		time.Sleep(20 * time.Millisecond) // the simulated goroutine is parked by then
		mu.Unlock()
		unlocked.Store(true)
	}()
	done := make(chan error, 1)
	go func() {
		r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
			w.Go(func() { // keeps the run going past the real Unlock
				for !unlocked.Load() {
					w.Sleep(time.Microsecond)
				}
			})
			close(locking) // (a raw close: it doesn't block)
			mu.Lock()
			mu.Unlock()
		})
		done <- r.Err
	}()
	select {
	case err := <-done:
		var d *sched.DeadlockError
		if !errors.As(err, &d) || !strings.Contains(err.Error(), "lock") {
			t.Fatalf("want a deadlock naming the lock, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the run hung")
	}
}

// sharedMu is a package-level lock, as dstmigrate makes of a global
// sync.Mutex: parallel seeds of a sweep all use it.
var sharedMu dst.Mutex

// sharedErr reports whether err is the misuse a primitive shared
// between simulations running at once fails a run with.
func sharedErr(err error) bool {
	var m *sched.MisuseError
	return errors.As(err, &m) && (strings.Contains(err.Error(), "shared between simulations running at once") &&
		strings.Contains(err.Error(), "Parallel: 1") || strings.Contains(err.Error(), "held by a goroutine of another simulation, which has ended"))
}

// A goroutine of one simulation waiting on a package-level lock another
// running simulation holds fails its run at once as misuse naming the
// sharing and the remedy, not as a deadlock (or never), for each
// primitive that waits; so does one using a lock another running
// simulation used, though neither ever waits for it.
// Fails if the run reports a generic deadlock, passes, or the holder's
// simulation is failed instead.
func TestSharedBetweenSimulationsIsMisuse(t *testing.T) {
	var (
		mu   dst.Mutex
		free dst.Mutex
		rw   dst.RWMutex
		wg   dst.WaitGroup
		cmu  dst.Mutex
		cond = dst.NewCond(&cmu)
	)
	cases := []struct {
		name       string
		hold, wait func(w *dst.World, release func() bool)
	}{
		{"Mutex", func(w *dst.World, release func() bool) {
			mu.Lock()
			for !release() {
				w.Sleep(time.Microsecond)
			}
			mu.Unlock()
		}, func(*dst.World, func() bool) { mu.Lock(); mu.Unlock() }},
		{"Mutex never contended", func(w *dst.World, release func() bool) {
			free.Lock()
			free.Unlock()
			for !release() {
				w.Sleep(time.Microsecond)
			}
		}, func(*dst.World, func() bool) { free.Lock(); free.Unlock() }},
		{"RWMutex", func(w *dst.World, release func() bool) {
			rw.Lock()
			for !release() {
				w.Sleep(time.Microsecond)
			}
			rw.Unlock()
		}, func(*dst.World, func() bool) { rw.RLock(); rw.RUnlock() }},
		{"WaitGroup", func(w *dst.World, release func() bool) {
			wg.Add(1)
			w.Go(func() { wg.Wait() }) // a waiter of this simulation
			for !release() {
				w.Sleep(time.Microsecond)
			}
			wg.Done()
		}, func(*dst.World, func() bool) { wg.Wait() }},
		{"Cond", func(w *dst.World, release func() bool) {
			w.Go(func() { cmu.Lock(); cond.Wait(); cmu.Unlock() })
			for !release() {
				w.Sleep(time.Microsecond)
			}
			cmu.Lock()
			cond.Broadcast()
			cmu.Unlock()
		}, func(*dst.World, func() bool) { cmu.Lock(); cond.Wait(); cmu.Unlock() }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var holding, waited atomic.Bool
			holderDone := make(chan error, 1)
			go func() {
				r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
					c.hold(w, func() bool { holding.Store(true); return waited.Load() })
				})
				holderDone <- r.Err
			}()
			for !holding.Load() {
				time.Sleep(time.Millisecond)
			}
			r := simworld.Run(simworld.Options{Seed: 2}, func(w *dst.World) {
				w.Go(func() { // marks the wait as made, once it is
					w.Sleep(time.Millisecond)
					waited.Store(true)
				})
				c.wait(w, nil)
			})
			waited.Store(true)
			if !sharedErr(r.Err) {
				t.Errorf("the waiting simulation: %v, want misuse naming the sharing", r.Err)
			}
			if err := <-holderDone; err != nil && !sharedErr(err) {
				t.Errorf("the holding simulation: %v", err)
			}
		})
	}
}

// Many simulations at once on one package-level lock (a sweep's
// parallel seeds): each run passes or fails with the sharing misuse;
// none is mistaken for another simulation's running task, none ends in
// a generic deadlock, and nothing races (-race).
// Fails if a run fails any other way, or the race detector fires: the
// scheduler a lock was last used under answering "it's my task" for
// another simulation's goroutine, or being read without synchronization.
func TestParallelSimulationsShareAGlobalLock(t *testing.T) {
	resetShared()
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for seed := range uint64(4) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := simworld.Run(simworld.Options{Seed: seed + 1}, func(w *dst.World) {
				var g dst.WaitGroup
				for range 3 {
					g.Go(w, func() {
						for range 300 {
							sharedMu.Lock()
							w.Gosched()
							sharedMu.Unlock()
						}
					})
				}
				g.Wait()
			})
			errs <- r.Err
		}()
	}
	wg.Wait()
	close(errs)
	shared := 0
	for err := range errs {
		if err != nil && !sharedErr(err) {
			t.Fatalf("want success or the sharing misuse, got %v", err)
		}
		if err != nil {
			shared++
		}
	}
	t.Logf("%d of 4 runs failed for the sharing", shared)
}

// sharedWG and sharedCond are package-level too: every Wait on them
// checks the run they were last waited on in.
var (
	sharedWG   dst.WaitGroup
	sharedCond = dst.NewCond(&sharedMu)
)

// resetShared gives a test fresh package-level primitives: a run that
// ended in misuse leaves its goroutines parked, holding what they held.
func resetShared() {
	sharedMu = dst.Mutex{}
	sharedWG = dst.WaitGroup{}
	sharedCond = dst.NewCond(&sharedMu)
}

// Seeds run one after another (Parallel: 1) may share package-level
// primitives: a finished simulation's use of them isn't sharing, even
// one that ended in a misuse (its parked goroutines never run again).
// Fails if a later run is failed for a primitive an ended run used.
func TestSequentialSimulationsShareAGlobalLock(t *testing.T) {
	// A run the watchdog aborts first, leaving a goroutine parked in
	// sharedCond.Wait (sharedMu released) that never runs again.
	resetShared()
	poison := simworld.Run(simworld.Options{Seed: 9, Watchdog: 100 * time.Millisecond}, func(w *dst.World) {
		inWait := false
		w.Go(func() { sharedMu.Lock(); inWait = true; sharedCond.Wait(); sharedMu.Unlock() })
		for !inWait { // it parks in Wait before it yields again
			w.Gosched()
		}
		<-make(chan struct{}) // a raw wait: the watchdog ends the run
	})
	if poison.Err == nil {
		t.Fatal("the watchdog didn't end the first run")
	}
	for seed := range uint64(3) {
		r := simworld.Run(simworld.Options{Seed: seed + 1}, func(w *dst.World) {
			var g dst.WaitGroup
			for range 3 {
				g.Go(w, func() {
					for range 50 {
						sharedMu.Lock()
						w.Gosched()
						sharedMu.Unlock()
					}
				})
			}
			sharedWG.Add(1)
			w.Go(func() { sharedWG.Done() })
			sharedWG.Wait()
			signalled := false
			w.Go(func() { sharedMu.Lock(); signalled = true; sharedCond.Broadcast(); sharedMu.Unlock() })
			sharedMu.Lock()
			for !signalled {
				sharedCond.Wait()
			}
			sharedMu.Unlock()
			g.Wait()
		})
		if r.Err != nil {
			t.Fatalf("seed %d: %v", seed+1, r.Err)
		}
	}
}

// Goroutines churn on recycled gs: a simulation's tasks start and finish
// by the thousand while real goroutines start and exit, each using a
// lock the simulation's tasks use (TryRLock: it never waits, so neither
// side blocks the other). A real goroutine on the g of a task that just
// finished must never be taken for the running task: it would act on
// the simulation's scheduler (misuse, or a race under -race).
// Fails if the run fails, or the race detector fires.
func TestRecycledGsAreNeverTheHolder(t *testing.T) {
	var rw dst.RWMutex
	var stop atomic.Bool
	var real sync.WaitGroup
	for range 4 {
		real.Add(1)
		go func() {
			defer real.Done()
			for !stop.Load() {
				var g sync.WaitGroup
				for range 8 {
					g.Add(1)
					go func() { // short-lived, on a g another just freed
						defer g.Done()
						if rw.TryRLock() {
							rw.RUnlock()
						}
					}()
				}
				g.Wait()
			}
		}()
	}
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		for range 500 {
			var g dst.WaitGroup
			for range 4 {
				g.Go(w, func() { rw.RLock(); rw.RUnlock() })
			}
			g.Wait()
		}
	})
	stop.Store(true)
	real.Wait()
	if r.Err != nil {
		t.Fatal(r.Err)
	}
}
