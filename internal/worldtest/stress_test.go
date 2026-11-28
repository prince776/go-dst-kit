package worldtest_test

// Stress tests of the real World's primitives: production code runs on
// them, so each is driven the way real code drives it (many goroutines,
// reuse straight after a wait, unlocks in defers during panics) for
// thousands of rounds, meant to run under -race. Every test runs twice:
// alone in the process, and with a simulation running alongside, which
// sends the real World through its slow paths (a primitive checks
// whether a simulated goroutine is calling), as in an adopter's test
// binary where real-World tests run next to simulation tests.
//
// DST_STRESS multiplies the rounds (default 1, a few seconds under -race
// in all; DST_STRESS=50 for a long run).
//
// Several of these tests fail only through -race, and the race detector
// reports a given race once per process: under -count=50 a race that
// happens in every run fails one run of fifty and looks like a rare
// flake. To count failures, run separate processes:
//
//	go test -race -c -o /tmp/wt.test ./internal/worldtest
//	for i in $(seq 50); do /tmp/wt.test -test.run Stress >/dev/null 2>&1 || echo fail; done

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

var stress = func() int {
	n, _ := strconv.Atoi(os.Getenv("DST_STRESS"))
	return max(n, 1)
}()

// rounds is n scaled by DST_STRESS.
func rounds(n int) int { return n * stress }

// bothModes runs fn alone in the process, then with a simulation running
// alongside (so sched.Active() is true and the real World takes its slow
// paths).
func bothModes(t *testing.T, fn func(t *testing.T, w *dst.World)) {
	t.Run("alone", func(t *testing.T) {
		if sched.Active() {
			t.Fatal("a simulation is running: the alone mode needs a process without one")
		}
		fn(t, realworld.New(realworld.Options{Dir: t.TempDir()}))
	})
	t.Run("sim-alongside", func(t *testing.T) {
		stop := simAlongside(t)
		defer stop()
		fn(t, realworld.New(realworld.Options{Dir: t.TempDir()}))
		if !sched.Active() {
			t.Fatal("the simulation alongside ended early: the slow paths weren't exercised")
		}
	})
}

// simAlongside starts a simulation whose one goroutine blocks (outside
// the World, with the watchdog far away) until the returned function is
// called, and returns once the simulation is active.
func simAlongside(t *testing.T) (stop func()) {
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		r := simworld.Run(simworld.Options{Seed: 1, Watchdog: time.Hour}, func(w *dst.World) { <-release })
		done <- r.Err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !sched.Active() {
		if time.Now().After(deadline) {
			t.Fatal("the simulation alongside never started")
		}
		runtime.Gosched()
	}
	return func() {
		close(release)
		if err := <-done; err != nil {
			t.Errorf("the simulation alongside failed: %v", err)
		}
	}
}

// within runs fn and fails the test with what (and how far it got, from
// progress) if fn doesn't return within the deadline, or panics.
func within(t *testing.T, what string, progress *atomic.Int64, total int, fn func()) {
	t.Helper()
	d := 30 * time.Second * time.Duration(stress)
	done := make(chan any, 1)
	go func() {
		defer func() { done <- recover() }()
		fn()
	}()
	select {
	case r := <-done:
		if r != nil {
			t.Fatalf("%s: failed at round %d of %d: %v", what, progress.Load(), total, r)
		}
	case <-time.After(d):
		buf := make([]byte, 1<<16)
		buf = buf[:runtime.Stack(buf, true)]
		t.Fatalf("%s: stuck at round %d of %d after %v (a goroutine was never woken or never returned)\n%s", what, progress.Load(), total, d, buf)
	}
}

// panicMsg runs fn and returns what it panicked with ("" if nothing).
func panicMsg(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

// ---- WaitGroup

// A group reused straight after Wait, round after round, with every Done
// from its own goroutine at once: each round's Wait returns after all
// of the round's work (whose writes it then reads, so -race checks the
// happens-before), and the group never panics.
// Fails if: Wait returns before every Done has finished (the bug fixed
// in 5d862e9, which corrupted a reused group), or a Done is lost.
func TestStressWaitGroupReuse(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const workers = 8
		n := rounds(3000)
		var g dst.WaitGroup
		var round atomic.Int64
		slots := make([]int, workers)
		within(t, "WaitGroup reused after Wait", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				g.Add(workers)
				for i := range workers {
					go func() {
						slots[i] = r
						g.Done()
					}()
				}
				g.Wait()
				for i, s := range slots {
					if s != r {
						panic(fmt.Sprintf("worker %d's write of round %d not visible after Wait (saw %d)", i, r, s))
					}
				}
			}
		})
	})
}

// Once Wait returns the group is reset by assignment, as a struct that
// holds one is reset for reuse (pebble's batches: *b = Batch{}), or put
// back in a pool: nothing touches it any more, so the reset can't be
// corrupted (sync.WaitGroup promises this: every Done has returned).
// Verifies: [wait-follows-done], [no-touch-after-release] (under -race)
// Fails if: a Done touches the group after the sync.WaitGroup's Add that
// lets Wait return (it read its waiter count there: 10 processes of 10
// fail), or Wait returns before every Done has finished (the bug fixed in
// 5d862e9, 4 of 10; TestWaitFollowsDone catches that one every time):
// -race reports the reset racing the late Done.
func TestStressWaitGroupResetAfterWait(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const workers = 4
		n := rounds(5000)
		type batch struct {
			wg  dst.WaitGroup
			ops int
		}
		pool := sync.Pool{New: func() any { return new(batch) }}
		var round atomic.Int64
		within(t, "WaitGroup reset after Wait", &round, n, func() {
			b := new(batch)
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				b.wg.Add(workers)
				for range workers {
					go b.wg.Done()
				}
				b.wg.Wait()
				if r%2 == 0 {
					*b = batch{ops: r} // reset in place
				} else {
					pool.Put(b) // or recycled
					b = pool.Get().(*batch)
					*b = batch{ops: r}
				}
			}
		})
	})
}

// Several goroutines Wait on each generation; once every one of them has
// returned, the group is reused (the rule sync.WaitGroup states). Every
// waiter sees the generation's work done.
// Fails if: a waiter returns before the last Done finished, or the
// group panics on reuse.
func TestStressWaitGroupManyWaiters(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const workers, waiters = 4, 3
		n := rounds(1500)
		var g dst.WaitGroup
		var round atomic.Int64
		var finished atomic.Int64
		within(t, "WaitGroup with many waiters", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				finished.Store(0)
				g.Add(workers)
				var returned sync.WaitGroup
				bad := make(chan string, waiters)
				for range waiters {
					returned.Add(1)
					go func() {
						defer returned.Done()
						g.Wait()
						if f := finished.Load(); f != workers {
							bad <- fmt.Sprintf("a waiter returned with %d of %d workers finished", f, workers)
						}
					}()
				}
				for range workers {
					go func() {
						finished.Add(1)
						g.Done()
					}()
				}
				returned.Wait()
				select {
				case m := <-bad:
					panic(m)
				default:
				}
			}
		})
	})
}

// Work that adds more work: each task Adds for its children before its
// own Done (legal while the count is positive), through WaitGroup.Go, and
// the tree's root Waits; the group is reused for the next tree.
// Fails if: Wait returns before the whole tree has run, or the counter
// goes wrong (a panic) as Adds and Dones from many goroutines interleave.
func TestStressWaitGroupNestedGo(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(300)
		var g dst.WaitGroup
		var round atomic.Int64
		within(t, "WaitGroup with nested Go", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var ran atomic.Int64
				var spawn func(depth int)
				spawn = func(depth int) {
					ran.Add(1)
					if depth == 0 {
						return
					}
					for range 3 {
						g.Go(w, func() { spawn(depth - 1) })
					}
				}
				g.Go(w, func() { spawn(3) })
				g.Wait()
				if got := ran.Load(); got != 1+3+9+27 {
					panic(fmt.Sprintf("Wait returned with %d of 40 tasks run", got))
				}
			}
		})
	})
}

// Misuse panics as sync.WaitGroup's does: a Done with nothing to do.
// Fails if: the panic is missing or says something else.
func TestStressWaitGroupNegativePanics(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		var std sync.WaitGroup
		want := panicMsg(std.Done)
		var g dst.WaitGroup
		if got := panicMsg(g.Done); got != want || want == "" {
			t.Fatalf("Done on a zero group panicked with %q, sync.WaitGroup with %q", got, want)
		}
	})
}

// ---- Mutex

// Goroutines take one mutex by Lock and by TryLock, round after round:
// at most one is ever inside, and a plain counter the critical section
// increments (a data race for -race if exclusion fails) ends exact. Half
// the unlocks happen from another goroutine than the lock's, which Go
// allows.
// Fails if: two goroutines are inside at once, an increment is lost, or
// a waiter is never woken.
func TestStressMutexExcludes(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(2000)
		var mu dst.Mutex
		var inside atomic.Int32
		var maxInside atomic.Int32
		counter := 0
		var progress atomic.Int64
		within(t, "Mutex exclusion", &progress, n*goroutines, func() {
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range n {
						if i%2 == 1 {
							for !mu.TryLock() {
								runtime.Gosched()
							}
						} else {
							mu.Lock()
						}
						if k := inside.Add(1); k > maxInside.Load() {
							maxInside.Store(k)
						}
						counter++
						inside.Add(-1)
						if r%2 == 0 {
							mu.Unlock()
						} else {
							done := make(chan struct{})
							go func() { mu.Unlock(); close(done) }() // unlocked by another goroutine
							<-done
						}
						progress.Add(1)
					}
				}()
			}
			wg.Wait()
		})
		if m := maxInside.Load(); m != 1 {
			t.Fatalf("%d goroutines were inside the critical section at once", m)
		}
		if counter != goroutines*n {
			t.Fatalf("counter %d, want %d: increments were lost", counter, goroutines*n)
		}
	})
}

// A critical section that panics unlocks in its defer; others are
// waiting on the lock meanwhile, and the mutex stays usable: every
// goroutine gets it again, and TryLock agrees with sync.Mutex's on a
// held and a free lock.
// Fails if: a deferred Unlock during a panic is lost (everyone after
// waits forever), or TryLock differs from sync.Mutex's.
func TestStressMutexUnlockInPanic(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 6
		n := rounds(1500)
		var mu dst.Mutex
		var progress atomic.Int64
		var panicked atomic.Int64
		within(t, "Mutex unlocked in a panic's defer", &progress, n*goroutines, func() {
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range n {
						func() {
							defer func() {
								if recover() != nil {
									panicked.Add(1)
								}
							}()
							mu.Lock()
							defer mu.Unlock()
							if (r+i)%3 == 0 {
								panic("boom")
							}
						}()
						progress.Add(1)
					}
				}()
			}
			wg.Wait()
		})
		if panicked.Load() == 0 {
			t.Fatal("no critical section panicked: the test didn't test anything")
		}
		var std sync.Mutex
		std.Lock()
		mu.Lock()
		if a, b := mu.TryLock(), std.TryLock(); a != b {
			t.Fatalf("TryLock on a held mutex: %v, sync.Mutex's %v", a, b)
		}
		mu.Unlock()
		std.Unlock()
		if a, b := mu.TryLock(), std.TryLock(); a != b || !a {
			t.Fatalf("TryLock on a free mutex: %v, sync.Mutex's %v", a, b)
		}
		mu.Unlock()
	})
}

// An object holding a lock is recycled by its last user the moment that
// user unlocks (a pooled object, reset on Put), while the previous
// holder's Unlock may still be returning: user 0 holds the lock (as a
// reader or a writer), user 1 waits for it as a writer, then unlocks and
// resets the object. sync's locks allow this; -race stays quiet for them
// (checked here too), and must for the dst locks.
// Verifies: [no-touch-after-release] (under -race)
// Fails if: an Unlock or RUnlock reads its lock after releasing it (dst's
// read their waiter count there): -race reports the reset racing that
// read, in every process.
func TestStressLockResetAfterUnlock(t *testing.T) {
	type rwLocker interface {
		sync.Locker
		RLock()
		RUnlock()
	}
	// run: per round, a fresh lock from fresh; reset zeroes it.
	run := func(t *testing.T, name string, n int, readers bool, fresh func() (sync.Locker, func())) {
		var round atomic.Int64
		within(t, name+" reset after unlock", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				l, reset := fresh()
				read := readers && r%2 == 0
				held := make(chan struct{})
				done := make(chan struct{})
				go func() { // user 0
					if read {
						l.(rwLocker).RLock()
					} else {
						l.Lock()
					}
					close(held)
					runtime.Gosched() // let user 1 start waiting
					if read {
						l.(rwLocker).RUnlock()
					} else {
						l.Unlock()
					}
				}()
				go func() { // user 1, the last
					<-held
					l.Lock()
					l.Unlock()
					reset()
					close(done)
				}()
				<-done
			}
		})
	}
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(3000)
		run(t, "sync.Mutex", n, false, func() (sync.Locker, func()) {
			o := new(struct{ mu sync.Mutex })
			return &o.mu, func() { o.mu = sync.Mutex{} }
		})
		run(t, "dst.Mutex", n, false, func() (sync.Locker, func()) {
			o := new(struct{ mu dst.Mutex })
			return &o.mu, func() { o.mu = dst.Mutex{} }
		})
		run(t, "sync.RWMutex", n, true, func() (sync.Locker, func()) {
			o := new(struct{ mu sync.RWMutex })
			return &o.mu, func() { o.mu = sync.RWMutex{} }
		})
		run(t, "dst.RWMutex", n, true, func() (sync.Locker, func()) {
			o := new(struct{ mu dst.RWMutex })
			return &o.mu, func() { o.mu = dst.RWMutex{} }
		})
	})
}

// ---- Once

// Many goroutines call Do on a fresh Once at the same moment, round
// after round: f runs exactly once, and every Do returns only after f has
// finished (each caller reads what f wrote, so -race checks it).
// Fails if: f runs twice, or a Do returns before f has finished (a
// "done" flag published before f returns).
func TestStressOnce(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(2000)
		var round atomic.Int64
		within(t, "Once with concurrent Do", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var o dst.Once
				var runs atomic.Int32
				value := 0
				start := make(chan struct{})
				var wg sync.WaitGroup
				bad := make(chan string, goroutines)
				for range goroutines {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						o.Do(func() {
							runs.Add(1)
							runtime.Gosched() // let the others reach Do meanwhile
							value = r
						})
						if value != r {
							bad <- fmt.Sprintf("Do returned before f finished (value %d, want %d)", value, r)
						}
					}()
				}
				close(start)
				wg.Wait()
				select {
				case m := <-bad:
					panic(m)
				default:
				}
				if k := runs.Load(); k != 1 {
					panic(fmt.Sprintf("f ran %d times", k))
				}
			}
		})
	})
}

// f panics while others wait in Do: as with sync.Once, the panic reaches
// the first caller, Do counts f as done (the waiters return without
// running it, and so does every later Do), and both Onces agree.
// Fails if: f runs again after its panic, or a waiter stays blocked.
func TestStressOncePanics(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 6
		n := rounds(1000)
		var round atomic.Int64
		type once interface{ Do(func()) }
		run := func(o once) (runs int32, panics int32) {
			var r, p atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			for range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					if panicMsg(func() {
						o.Do(func() {
							r.Add(1)
							runtime.Gosched()
							panic("f failed")
						})
					}) != "" {
						p.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()
			o.Do(func() { r.Add(1) })
			return r.Load(), p.Load()
		}
		within(t, "Once whose f panics", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				runs, panics := run(&dst.Once{})
				stdRuns, stdPanics := run(&sync.Once{})
				if runs != 1 || panics != 1 || runs != stdRuns || panics != stdPanics {
					panic(fmt.Sprintf("f ran %d times and %d callers saw its panic; with sync.Once %d and %d", runs, panics, stdRuns, stdPanics))
				}
			}
		})
	})
}

// ---- RWMutex

// Readers and writers share one lock, by Lock and RLock and by TryLock
// and TryRLock, round after round: a writer is always alone, readers
// overlap only with readers, and a plain counter the writers increment
// (a data race for -race if exclusion fails) ends exact. Some sections
// panic and unlock in their defer.
// Fails if: a writer is inside with anyone else, an increment is lost,
// or a deferred unlock during a panic is lost (everyone after waits).
func TestStressRWMutexExcludes(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(1500)
		var mu dst.RWMutex
		var readers, writers atomic.Int32
		var bad atomic.Value
		counter, writes := 0, 0
		var progress atomic.Int64
		within(t, "RWMutex exclusion", &progress, n*goroutines, func() {
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range n {
						write := (r+i)%4 == 0
						panicMsg(func() {
							if write {
								if i%2 == 1 {
									for !mu.TryLock() {
										runtime.Gosched()
									}
								} else {
									mu.Lock()
								}
								defer mu.Unlock()
								if k := writers.Add(1); k != 1 || readers.Load() != 0 {
									bad.Store(fmt.Sprintf("a writer is inside with %d writers and %d readers", k, readers.Load()))
								}
								counter++
								writes++
								writers.Add(-1)
							} else {
								if i%2 == 1 {
									for !mu.TryRLock() {
										runtime.Gosched()
									}
								} else {
									mu.RLock()
								}
								defer mu.RUnlock()
								readers.Add(1)
								if writers.Load() != 0 {
									bad.Store("a reader is inside with a writer")
								}
								_ = counter // read under the read lock
								readers.Add(-1)
							}
							if r%7 == 0 {
								panic("boom") // unlocked by the defer
							}
						})
						progress.Add(1)
					}
				}()
			}
			wg.Wait()
		})
		if b := bad.Load(); b != nil {
			t.Fatal(b)
		}
		if counter != writes || writes == 0 {
			t.Fatalf("counter %d after %d writes", counter, writes)
		}
	})
}

// waitReason waits until some goroutine is parked with the runtime wait
// reason (as goroutine dumps show it, "sync.RWMutex.Lock"), or stop
// reports true. A state, not a sleep: nothing here depends on timing.
func waitReason(reason string, stop func() bool) bool {
	buf := make([]byte, 1<<20)
	for !stop() {
		if strings.Contains(string(buf[:runtime.Stack(buf, true)]), "["+reason) {
			return true
		}
		runtime.Gosched()
	}
	return false
}

// Two readers hold the lock at once; once a writer is blocked in Lock, a
// new reader waits too (sync.RWMutex's rule, which keeps writers from
// starving), and gets the lock only after the writer. Each step is
// observed (a goroutine parked in the runtime with that wait reason),
// never timed.
// Fails if: a second reader can't get the lock alongside the first, or
// a reader arriving while a writer waits gets it before the writer.
func TestStressRWMutexWriterFirst(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(100)
		var round atomic.Int64
		within(t, "RWMutex writer preference", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var mu dst.RWMutex
				mu.RLock() // reader 1
				second := make(chan struct{})
				go func() { mu.RLock(); close(second) }()
				<-second // reader 2 alongside (else: stuck, and within says so)
				mu.RUnlock()
				var order []string
				var omu sync.Mutex
				note := func(s string) { omu.Lock(); order = append(order, s); omu.Unlock() }
				wrote := make(chan struct{})
				go func() {
					mu.Lock()
					note("writer")
					mu.Unlock()
					close(wrote)
				}()
				waitReason("sync.RWMutex.Lock", func() bool { return false })
				read := make(chan struct{})
				go func() {
					mu.RLock()
					note("reader")
					mu.RUnlock()
					close(read)
				}()
				closed := func(c chan struct{}) bool {
					select {
					case <-c:
						return true
					default:
						return false
					}
				}
				if !waitReason("sync.RWMutex.RLock", func() bool { return closed(read) }) {
					panic("a reader got the lock while a writer waited for it")
				}
				mu.RUnlock() // reader 2 leaves: the writer, then the reader
				<-wrote
				<-read
				if len(order) != 2 || order[0] != "writer" {
					panic(fmt.Sprintf("order %v, want the writer first", order))
				}
			}
		})
	})
}

// ---- Cond

// A bounded queue: producers wait on notFull, consumers on notEmpty, and
// each side Signals the other, sometimes holding the lock and sometimes
// not (both are allowed). Every item is consumed exactly once.
// Fails if: a Signal is lost while a goroutine waits (a stuck queue), or
// an item is lost or delivered twice.
func TestStressCondSignal(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const producers, consumers, size = 4, 4, 2
		n := rounds(2000) // items per producer
		var mu dst.Mutex
		notEmpty, notFull := dst.NewCond(&mu), dst.NewCond(&mu)
		var queue []int
		seen := make([]atomic.Int32, producers*n)
		var progress atomic.Int64
		within(t, "Cond signalling", &progress, producers*n, func() {
			var wg sync.WaitGroup
			for p := range producers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range n {
						mu.Lock()
						for len(queue) == size {
							notFull.Wait()
						}
						queue = append(queue, p*n+i)
						if i%2 == 0 {
							notEmpty.Signal()
							mu.Unlock()
						} else {
							mu.Unlock()
							notEmpty.Signal()
						}
					}
				}()
			}
			var cg sync.WaitGroup
			for range consumers {
				cg.Add(1)
				go func() {
					defer cg.Done()
					for {
						mu.Lock()
						for len(queue) == 0 {
							if progress.Load() == int64(producers*n) {
								mu.Unlock()
								return
							}
							notEmpty.Wait()
						}
						v := queue[0]
						queue = queue[1:]
						last := progress.Add(1) == int64(producers*n)
						mu.Unlock()
						notFull.Signal()
						if last {
							notEmpty.Broadcast() // the consumers still waiting stop
						}
						seen[v].Add(1)
					}
				}()
			}
			wg.Wait()
			cg.Wait()
		})
		for v := range seen {
			if k := seen[v].Load(); k != 1 {
				t.Fatalf("item %d consumed %d times", v, k)
			}
		}
	})
}

// A fresh Cond each round, so its first Waits run every time: waiters
// holding a read lock through RLocker (several at once, so their first
// Waits race each other) and the Broadcast that must reach all of them; then Broadcast
// generations on one Cond, each reaching every waiter.
// Fails if: two first Waits wait on different conditions (a lost
// wake-up: stuck), or a Broadcast misses a waiter.
func TestStressCondBroadcast(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const waiters = 4
		n := rounds(1500)
		var round atomic.Int64
		within(t, "Cond broadcast", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var rw dst.RWMutex
				c := dst.NewCond(rw.RLocker()) // several goroutines hold it at once
				var ready atomic.Bool
				var waiting atomic.Int32
				var wg sync.WaitGroup
				for range waiters {
					wg.Add(1)
					go func() {
						defer wg.Done()
						rw.RLock()
						waiting.Add(1)
						for !ready.Load() {
							c.Wait()
						}
						rw.RUnlock()
					}()
				}
				for waiting.Load() < waiters {
					runtime.Gosched()
				}
				rw.Lock() // every waiter is in Wait (it released its read lock)
				ready.Store(true)
				rw.Unlock()
				c.Broadcast()
				wg.Wait()
			}
		})
		// Generations on one Cond: every waiter sees every generation.
		var mu dst.Mutex
		c := dst.NewCond(&mu)
		gen, acked := 0, 0
		gens := rounds(1500)
		var g atomic.Int64
		within(t, "Cond broadcast generations", &g, gens, func() {
			var wg sync.WaitGroup
			for range waiters {
				wg.Add(1)
				go func() {
					defer wg.Done()
					mu.Lock()
					defer mu.Unlock()
					for seen := 0; seen < gens; {
						for gen == seen {
							c.Wait()
						}
						seen = gen
						acked++
						c.Broadcast() // wakes the driver too
					}
				}()
			}
			mu.Lock()
			for i := 1; i <= gens; i++ {
				g.Store(int64(i))
				gen = i
				c.Broadcast()
				for acked < i*waiters {
					c.Wait()
				}
			}
			mu.Unlock()
			wg.Wait()
		})
	})
}

// ---- Chan

// Receivers race a close: they range over the channel while senders
// fill it and it is closed, and they get exactly the values sent, the
// buffer drained before the range ends. Senders then try again, after
// the close (ordered after it: the race detector counts a send racing a
// close as a data race, for Go's channels too), and each panics with
// Go's message.
// Fails if: a value is lost or received twice, a range ends before the
// buffer drains (or never), or a send on the closed channel doesn't
// panic as Go's does.
func TestStressChanCloseRaces(t *testing.T) {
	want := panicMsg(func() { c := make(chan int, 1); close(c); c <- 1 })
	bothModes(t, func(t *testing.T, w *dst.World) {
		const senders, receivers = 4, 3
		n := rounds(400)
		var round atomic.Int64
		within(t, "Chan close racing receives", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				c := dst.MakeChan[int](w, r%3) // unbuffered and buffered
				var got [senders * 16]atomic.Int32
				var sent, done, rg sync.WaitGroup
				closed := make(chan struct{})
				panics := make(chan string, senders)
				for s := range senders {
					sent.Add(1)
					done.Add(1)
					go func() {
						defer done.Done()
						for i := range 16 {
							c.Send(s*16 + i)
						}
						sent.Done() // the closer may go
						<-closed
						panics <- panicMsg(func() { c.Send(-1) })
					}()
				}
				for range receivers {
					rg.Add(1)
					go func() {
						defer rg.Done()
						for v := range c.All() {
							got[v].Add(1)
						}
					}()
				}
				sent.Wait() // every value sent (some still in the buffer)
				c.Close()   // races the receivers
				close(closed)
				rg.Wait()
				done.Wait()
				for range senders {
					if m := <-panics; m != want {
						panic(fmt.Sprintf("a send on the closed channel panicked with %q, Go's with %q", m, want))
					}
				}
				for v := range got {
					if g := got[v].Load(); g != 1 {
						panic(fmt.Sprintf("value %d received %d times", v, g))
					}
				}
			}
		})
	})
}

// Breaking out of `range c.All()` takes nothing more from the channel:
// the value after the one the loop stopped at is still there for the
// next receive, under a sender that never waits for it.
// Fails if: All receives ahead of the loop (a value lost at the break).
func TestStressChanAllBreak(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(2000)
		var round atomic.Int64
		within(t, "Chan All with break", &round, n, func() {
			c := dst.MakeChan[int](w, 4)
			go func() {
				for i := range 2 * n {
					c.Send(i)
				}
			}()
			for r := 0; r < n; r++ {
				round.Store(int64(r))
				for v := range c.All() {
					if v != 2*r {
						panic(fmt.Sprintf("the loop got %d, want %d", v, 2*r))
					}
					break
				}
				if v, ok := c.Recv(); v != 2*r+1 || !ok {
					panic(fmt.Sprintf("after the break Recv got %d %v, want %d true", v, ok, 2*r+1))
				}
			}
		})
	})
}

// TrySend and TryRecv race each other and a close: every value a
// TrySend reports sent is received exactly once (by a TryRecv or the
// final drain), TryRecv on a closed and drained channel reports
// received with ok false (as a select with default does), and on an
// open empty channel received false.
// Fails if: a Try operation's report disagrees with what happened, or
// with Go's select-with-default on a closed or empty channel.
func TestStressChanTry(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(300)
		var round atomic.Int64
		within(t, "Chan TrySend and TryRecv", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				c := dst.MakeChan[int](w, 2)
				if _, _, got := c.TryRecv(); got {
					panic("TryRecv received from an empty open channel")
				}
				var sent, got atomic.Int64
				var seen [256]atomic.Int32
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					for v := range 256 {
						if c.TrySend(v) {
							sent.Add(1)
						}
					}
				}()
				go func() {
					defer wg.Done()
					for range 256 {
						if v, ok, received := c.TryRecv(); received && ok {
							got.Add(1)
							seen[v].Add(1)
						}
					}
				}()
				wg.Wait()
				c.Close()
				for {
					v, ok, received := c.TryRecv()
					if !received {
						panic("TryRecv on a closed channel reported nothing received")
					}
					if !ok {
						break
					}
					got.Add(1)
					seen[v].Add(1)
				}
				if s, g := sent.Load(), got.Load(); s != g || s == 0 {
					panic(fmt.Sprintf("TrySend reported %d sent, %d received", s, g))
				}
				for v := range seen {
					if seen[v].Load() > 1 {
						panic(fmt.Sprintf("value %d received twice", v))
					}
				}
			}
		})
	})
}

// ---- Select, Select2, Select3

// selectShape runs rounds selects over cases of the given directions
// ('r' receive, 's' send), each on its own unbuffered channel: a feeder
// goroutine sends to each receive case values tagged with the case's
// index, a drainer receives from each send case. Every select runs one
// case's callback, and a receive's callback gets its own channel's value
// (tag and order); a send case's value reaches its own drainer. Returns
// how many times each case fired.
func selectShape(w *dst.World, dirs string, n int, sel func(ops []dst.Op[int])) []int {
	const tag = 1_000_000
	fired := make([]int, len(dirs))
	chans := make([]dst.Chan[int], len(dirs))
	var stop atomic.Bool
	var helpers sync.WaitGroup
	drained := make([]atomic.Int64, len(dirs))
	for k, d := range dirs {
		chans[k] = dst.MakeChan[int](w, 0)
		helpers.Add(1)
		if d == 'r' {
			go func() { // feeder
				defer helpers.Done()
				defer chans[k].Close() // so the final Recv below never waits
				for seq := 0; !stop.Load(); seq++ {
					chans[k].Send(k*tag + seq)
				}
			}()
		} else {
			go func() { // drainer
				defer helpers.Done()
				last := -1
				for v := range chans[k].All() {
					if v/tag != k || v%tag <= last {
						panic(fmt.Sprintf("send case %d's drainer got %d (after %d)", k, v, last))
					}
					last = v % tag
					drained[k].Add(1)
				}
			}()
		}
	}
	lastRecv := make([]int, len(dirs))
	for k := range lastRecv {
		lastRecv[k] = -1
	}
	for r := range n {
		ops := make([]dst.Op[int], len(dirs))
		for k, d := range dirs {
			if d == 'r' {
				ops[k] = dst.OnRecv(chans[k], func(v int, ok bool) {
					if !ok || v/tag != k || v%tag <= lastRecv[k] {
						panic(fmt.Sprintf("receive case %d got %d %v (after %d)", k, v, ok, lastRecv[k]))
					}
					lastRecv[k] = v % tag
					fired[k]++
				})
			} else {
				ops[k] = dst.OnSend(chans[k], k*tag+r, func() { fired[k]++ })
			}
		}
		sel(ops)
	}
	stop.Store(true)
	for k, d := range dirs {
		if d == 'r' {
			chans[k].Recv() // unblock the feeder, which then sees stop (or it closed)
		} else {
			chans[k].Close()
		}
	}
	helpers.Wait()
	total := 0
	for k, d := range dirs {
		total += fired[k]
		if d == 's' && drained[k].Load() != int64(fired[k]) {
			panic(fmt.Sprintf("send case %d fired %d times, its drainer got %d", k, fired[k], drained[k].Load()))
		}
	}
	if total != n {
		panic(fmt.Sprintf("%d selects ran %d callbacks", n, total))
	}
	return fired
}

// Every direction and position of Select2 and Select3 (each a native
// select of its own) and of Select (reflection), with goroutines on the
// other end of every channel: each select runs exactly one case, with
// that case's value.
// Fails if: a select runs the wrong case's callback, gives a case another
// channel's value, or reports a send that didn't happen.
func TestStressSelectCases(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(300)
		var shape atomic.Int64
		within(t, "Select cases", &shape, 2*4+8, func() {
			for _, dirs := range []string{"rr", "rs", "sr", "ss"} {
				selectShape(w, dirs, n, func(ops []dst.Op[int]) { dst.Select2(w, ops[0], ops[1]) })
				selectShape(w, dirs, n, func(ops []dst.Op[int]) { dst.Select(w, ops[0], ops[1]) })
				shape.Add(1)
			}
			for _, dirs := range []string{"rrr", "rrs", "rsr", "rss", "srr", "srs", "ssr", "sss"} {
				selectShape(w, dirs, n, func(ops []dst.Op[int]) { dst.Select3(w, ops[0], ops[1], ops[2]) })
				selectShape(w, dirs, n, func(ops []dst.Op[int]) { dst.Select(w, ops[0], ops[1], ops[2]) })
				shape.Add(1)
			}
		})
	})
}

// Cases on closed channels: each is ready, gets the zero value with ok
// false, and the choice among several is random as Go's is (each picked
// at least a tenth of the time, loose on purpose). A send case on a
// closed channel panics with Go's message; a nil channel is never ready.
// Fails if: a closed receive reports ok, a select always prefers one
// closed case, a send on a closed channel doesn't panic as Go's, or a
// nil case runs.
func TestStressSelectClosed(t *testing.T) {
	wantSend := panicMsg(func() {
		c := make(chan int)
		close(c)
		select {
		case c <- 1:
		case <-make(chan int):
		}
	})
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(2000)
		closed := func() dst.Chan[int] { c := dst.MakeChan[int](w, 0); c.Close(); return c }
		var nilChan dst.Chan[int]
		count := func(k int, fired []int) dst.Op[int] {
			return dst.OnRecv(closed(), func(v int, ok bool) {
				if v != 0 || ok {
					panic(fmt.Sprintf("a receive on a closed channel got %d %v", v, ok))
				}
				fired[k]++
			})
		}
		never := dst.OnRecv(nilChan, func(int, bool) { panic("a nil channel's case ran") })
		fair := func(name string, fired []int) {
			for k, f := range fired {
				if f < n/10 {
					t.Fatalf("%s: case %d of %d picked %d times in %d", name, k, len(fired), f, n)
				}
			}
		}
		var progress atomic.Int64
		within(t, "Select on closed channels", &progress, 3, func() {
			f2, f3, fs := make([]int, 2), make([]int, 3), make([]int, 3)
			a2, b2 := count(0, f2), count(1, f2)
			a3, b3, c3 := count(0, f3), count(1, f3), count(2, f3)
			as, bs, cs := count(0, fs), count(1, fs), count(2, fs)
			for range n {
				dst.Select2(w, a2, b2)
				dst.Select3(w, a3, never, b3)
				dst.Select3(w, never, c3, never)
				dst.Select(w, as, bs, never, cs)
			}
			f3[2] -= n // the lone-c3 selects
			fair("Select2", f2)
			fair("Select3", f3[:2])
			fair("Select", fs)
			progress.Add(1)
			ran := false
			dst.Select(w, never, dst.Default(func() { ran = true }))
			if !ran {
				panic("Select with only a nil case didn't run its default")
			}
			progress.Add(1)
			for name, sel := range map[string]func(){
				"Select2": func() { dst.Select2(w, dst.OnSend(closed(), 1, nil), never) },
				"Select3": func() { dst.Select3(w, never, never, dst.OnSend(closed(), 1, nil)) },
				"Select":  func() { dst.Select(w, never, dst.OnSend(closed(), 1, nil)) },
			} {
				if m := panicMsg(sel); m != wantSend {
					panic(fmt.Sprintf("%s's send on a closed channel panicked with %q, Go's with %q", name, m, wantSend))
				}
			}
		})
	})
}

// ---- Timer, Ticker, AfterFunc, Sleep, Now

// Stop races the timer firing at a receiver: Stop reports false exactly
// when the receiver got the value, and after Stop or Reset returns no
// stale value arrives (Go 1.23's timers); a value after a Reset is from
// the new deadline.
// Fails if: Stop's result disagrees with the delivery, a stale value is
// received after Stop or Reset, or a reset timer fires early.
func TestStressTimerStopReset(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(1000)
		var round atomic.Int64
		var stoppedTrue, stoppedFalse atomic.Int64
		within(t, "Timer Stop and Reset", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				d := time.Duration(r%5) * 20 * time.Microsecond
				tm := w.NewTimer(d)
				quit := dst.MakeChan[struct{}](w, 0)
				got := make(chan bool, 1)
				go func() {
					received := false
					dst.Select2(w, dst.OnRecv(tm.C, func(time.Time, bool) { received = true }), dst.OnRecv(quit, func(struct{}, bool) {}))
					got <- received
				}()
				for range r % 7 {
					runtime.Gosched()
				}
				stopped := tm.Stop()
				if _, _, stale := tm.C.TryRecv(); stale {
					panic("a value was received after Stop returned")
				}
				quit.Close()
				if received := <-got; received == stopped {
					panic(fmt.Sprintf("Stop reported %v, and the receiver got the value: %v", stopped, received))
				}
				if stopped {
					stoppedTrue.Add(1)
				} else {
					stoppedFalse.Add(1)
				}
				// Reset: no stale value, and the next is from the new deadline.
				d2 := 200 * time.Microsecond
				start := w.Now()
				tm.Reset(d2)
				// Any value now is from the new deadline (a stale one would
				// carry a time from before the Reset): even one received at
				// once, since a slow machine may get here after d2.
				v, _, now := tm.C.TryRecv()
				if !now {
					v, _ = tm.C.Recv()
				}
				if v.Sub(start) < d2 {
					panic(fmt.Sprintf("after Reset a value from %v after it, before the new deadline %v: stale", v.Sub(start), d2))
				}
			}
		})
		if stoppedTrue.Load() == 0 || stoppedFalse.Load() == 0 {
			t.Fatalf("Stop reported true %d times and false %d: the race wasn't exercised both ways", stoppedTrue.Load(), stoppedFalse.Load())
		}
	})
}

// AfterFunc's Stop races the function starting: false means it has run
// (or will, exactly once); true means it never runs.
// Fails if: fn runs after a Stop that reported true, runs twice, or
// doesn't run after a Stop that reported false.
func TestStressAfterFuncStop(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(1000)
		var round atomic.Int64
		var stoppedTrue, stoppedFalse atomic.Int64
		within(t, "AfterFunc Stop", &round, n, func() {
			pending := []func(){} // the true-Stop checks, made after a grace
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var runs atomic.Int32
				ran := make(chan struct{}, 2)
				d := time.Duration(r%5) * 20 * time.Microsecond
				tm := w.AfterFunc(d, func() { runs.Add(1); ran <- struct{}{} })
				for range r % 7 {
					runtime.Gosched()
				}
				if tm.Stop() {
					stoppedTrue.Add(1)
					pending = append(pending, func() {
						if k := runs.Load(); k != 0 {
							panic(fmt.Sprintf("fn ran %d times after a Stop that reported true", k))
						}
					})
				} else {
					stoppedFalse.Add(1)
					<-ran // it runs (else: stuck, and within says so)
					if tm.Stop() {
						panic("a second Stop reported true after fn ran")
					}
				}
			}
			// fn would be late, never early: waiting only makes a false
			// pass possible, never a false failure.
			w.Sleep(5 * time.Millisecond)
			for _, check := range pending {
				check()
			}
		})
		if stoppedTrue.Load() == 0 || stoppedFalse.Load() == 0 {
			t.Fatalf("Stop reported true %d times and false %d: the race wasn't exercised both ways", stoppedTrue.Load(), stoppedFalse.Load())
		}
	})
}

// A ticker ticks no faster than its period, a Reset period applies from
// the Reset, and once Stop returns no tick arrives (none stale, none
// later).
// Fails if: a tick comes early, after Stop, or a Reset doesn't take.
func TestStressTicker(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(30)
		var round atomic.Int64
		within(t, "Ticker", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				d := 100 * time.Microsecond
				start := w.Now()
				tk := w.NewTicker(d)
				for i := 1; i <= 3; i++ {
					if v, _ := tk.C.Recv(); v.Sub(start) < time.Duration(i)*d {
						panic(fmt.Sprintf("tick %d at %v, before %v", i, v.Sub(start), time.Duration(i)*d))
					}
				}
				d2 := 400 * time.Microsecond
				reset := w.Now()
				tk.Reset(d2)
				if v, _ := tk.C.Recv(); v.Sub(reset) < d2 {
					panic(fmt.Sprintf("the first tick after Reset came %v after it, before the new period %v", v.Sub(reset), d2))
				}
				tk.Stop()
				if _, _, got := tk.C.TryRecv(); got {
					panic("a tick was received after Stop returned")
				}
				if r == n { // a later tick would come within a period or two; waiting only risks a false pass
					w.Sleep(4 * d2)
					if _, _, got := tk.C.TryRecv(); got {
						panic("a stopped ticker ticked")
					}
				}
			}
		})
	})
}

// Concurrent sleepers each sleep at least as long as asked, and each
// goroutine's clock readings never go backwards.
// Fails if: a Sleep returns early, or Now goes back.
func TestStressSleepAndNow(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(50)
		var progress atomic.Int64
		within(t, "Sleep and Now", &progress, goroutines*n, func() {
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					prev := w.Now()
					for r := range n {
						d := time.Duration((i+r)%4) * 50 * time.Microsecond
						start := w.Now()
						if start.Before(prev) {
							panic("Now went backwards")
						}
						w.Sleep(d)
						if got := w.Since(start); got < d {
							panic(fmt.Sprintf("Sleep(%v) returned after %v", d, got))
						}
						prev = w.Now()
						progress.Add(1)
					}
				}()
			}
			wg.Wait()
		})
	})
}

// ---- Context

// Cancellation races everything that hangs off a context: children made
// while the parent is being cancelled (by WithCancel, WithTimeout and
// their Cause variants) are cancelled too, with the parent's cause, a timeout's
// context ends with DeadlineExceeded, and AfterDone's stop agrees with
// whether f ran: false means f runs exactly once, true that it never
// does.
// Fails if: a child of a cancelled parent stays live, a timeout reports
// another error, or AfterDone's stop disagrees with f.
func TestStressContext(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(1000)
		var round atomic.Int64
		var stopTrue, stopFalse atomic.Int64
		within(t, "Context cancellation", &round, n, func() {
			var pending []func()
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				parent, cancelCause := dst.WithCancelCause(w, context.Background())
				cause := fmt.Errorf("round %d", r)
				cancel := func() { cancelCause(cause) }
				children := make(chan context.Context, 4)
				var wg sync.WaitGroup
				for i := range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						switch i {
						case 0:
							c, _ := dst.WithCancel(w, parent)
							children <- c
						case 1:
							c, _ := dst.WithTimeout(w, parent, time.Hour)
							children <- c
						case 2:
							c, _ := dst.WithCancelCause(w, parent)
							children <- c
						default:
							c, _ := dst.WithTimeoutCause(w, parent, time.Hour, errors.New("not this one"))
							children <- c
						}
					}()
				}
				var runs atomic.Int32
				ran := make(chan struct{}, 2)
				stop := dst.AfterDone(parent, func() { runs.Add(1); ran <- struct{}{} })
				var stopped atomic.Bool
				go func() { stopped.Store(stop()) }()
				cancel()
				wg.Wait()
				close(children)
				for c := range children {
					if _, ok := dst.Done(c).Recv(); ok || !errors.Is(c.Err(), context.Canceled) || context.Cause(c) != cause {
						panic(fmt.Sprintf("a child of a cancelled parent: %v, cause %v (want %v)", c.Err(), context.Cause(c), cause))
					}
				}
				for !stopped.Load() && runs.Load() == 0 {
					runtime.Gosched() // stop and f race: one of them happens
				}
				if stopped.Load() {
					stopTrue.Add(1)
					pending = append(pending, func() {
						if k := runs.Load(); k != 0 {
							panic(fmt.Sprintf("AfterDone's f ran %d times after its stop reported true", k))
						}
					})
				} else {
					stopFalse.Add(1)
					<-ran
				}
				if r%50 == 0 {
					ctx, cancel := dst.WithTimeout(w, context.Background(), time.Duration(r%3)*10*time.Microsecond)
					dst.Done(ctx).Recv()
					if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
						panic(fmt.Sprintf("a timed-out context's error: %v", ctx.Err()))
					}
					cancel()
				}
			}
			w.Sleep(5 * time.Millisecond) // f would be late, never early: only a false pass is possible
			for _, check := range pending {
				check()
			}
		})
		if stopTrue.Load()+stopFalse.Load() != int64(n) {
			t.Fatalf("stop reported %d+%d of %d", stopTrue.Load(), stopFalse.Load(), n)
		}
	})
}

// ---- Fault points on the real World

// In the real World a fault point never fires: concurrent Calls and Dos
// each run their operation exactly once and return its result, even
// with a context already done, and Inject and Buggify say None and false.
// Fails if: an operation is skipped or run twice, a result is replaced,
// or a fault is injected.
func TestStressRealFaultPoints(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(500)
		var progress atomic.Int64
		done, cancel := context.WithCancel(context.Background())
		cancel()
		within(t, "Call, Do and Inject", &progress, goroutines*n, func() {
			var wg sync.WaitGroup
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for r := range n {
						var runs atomic.Int32
						ctx := context.Background()
						if r%2 == 0 {
							ctx = done
						}
						v, err := dst.Call(ctx, w, "svc.op", dst.Errors{}, func() (int, error) { runs.Add(1); return i*n + r, nil })
						errDo := dst.Do(w, "svc.do", func() error { runs.Add(1); return os.ErrExist })
						if v != i*n+r || err != nil || !errors.Is(errDo, os.ErrExist) || runs.Load() != 2 {
							panic(fmt.Sprintf("Call gave %d %v, Do %v, operations ran %d times (want 2)", v, err, errDo, runs.Load()))
						}
						if f := dst.Inject(w, "svc.inject"); f != dst.None || w.Buggify("svc.bug") {
							panic(fmt.Sprintf("the real World injected %v", f))
						}
						progress.Add(1)
					}
				}()
			}
			wg.Wait()
		})
	})
}

// ---- The real filesystem (realworld's own code)

// A fresh real World's filesystem is first used by many goroutines at
// once (its root opens lazily), they write and read their own ranges of
// one file concurrently, and they contend for one file lock: at most one
// holds it at a time, the others get ErrLocked, and it can be taken
// again after its unlock.
// Fails if: concurrent first use fails or races, a range reads back
// wrong, two goroutines hold the lock at once, or a losing LockFile
// returns anything but ErrLocked.
func TestStressRealFS(t *testing.T) {
	bothModes(t, func(t *testing.T, _ *dst.World) {
		const goroutines = 8
		n := rounds(200)
		var progress atomic.Int64
		w := realworld.New(realworld.Options{Dir: t.TempDir()}) // fresh: its root isn't open yet
		var holders, maxHolders, won, lost atomic.Int32
		within(t, "real filesystem", &progress, goroutines*n, func() {
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					f, err := w.FS().OpenFile("data", os.O_RDWR|os.O_CREATE, 0o644)
					if err != nil {
						panic(err)
					}
					defer f.Close()
					buf := bytes.Repeat([]byte{byte('a' + i)}, 512)
					got := make([]byte, 512)
					for r := range n {
						off := int64((r%4)*goroutines+i) * 512
						if _, err := f.WriteAt(buf, off); err != nil {
							panic(err)
						}
						if _, err := f.ReadAt(got, off); err != nil || !bytes.Equal(got, buf) {
							panic(fmt.Sprintf("range %d read back wrong (%v)", off, err))
						}
						unlock, err := dst.LockFile(w.FS(), "LOCK")
						switch {
						case err == nil:
							won.Add(1)
							if k := holders.Add(1); k > maxHolders.Load() {
								maxHolders.Store(k)
							}
							runtime.Gosched()
							holders.Add(-1)
							if err := unlock(); err != nil {
								panic(err)
							}
						case errors.Is(err, dst.ErrLocked):
							lost.Add(1)
						default:
							panic(fmt.Sprintf("LockFile: %v", err))
						}
						progress.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()
		})
		if m := maxHolders.Load(); m != 1 {
			t.Fatalf("%d goroutines held the file lock at once", m)
		}
		if won.Load() == 0 || lost.Load() == 0 {
			t.Fatalf("the lock was won %d times and lost %d: contention wasn't exercised", won.Load(), lost.Load())
		}
	})
}

// Goroutines create one file and one directory tree at the same moment,
// in a fresh directory each round, by OpenFile with O_CREATE, LockFile
// and MkdirAll: every call succeeds, as the os package's do. (On macOS
// the openat(2) that os.Root uses fails such creates spuriously with
// ENOENT, and Go 1.25.0's os.Root.MkdirAll fails on a directory created
// meanwhile; the real World retries. Elsewhere, or on a later Go, the
// matching half passes without its retry.)
// Verifies: [create-like-os]
// Fails if: a concurrent create fails where the os package's wouldn't
// (without the retries, on macOS with Go 1.25.0: hundreds of 2,400 opens
// and of 1,200 MkdirAlls, every run).
func TestStressRealFSConcurrentCreate(t *testing.T) {
	bothModes(t, func(t *testing.T, _ *dst.World) {
		const goroutines = 8
		n := rounds(300)
		var round atomic.Int64
		var failed atomic.Int32
		var first atomic.Value
		within(t, "concurrent creates", &round, n, func() {
			for r := range n {
				round.Store(int64(r))
				w := realworld.New(realworld.Options{Dir: t.TempDir()})
				w.FS().Stat(".") // open the root first: only the creates race
				var wg sync.WaitGroup
				start := make(chan struct{})
				for i := range goroutines {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						err := w.FS().MkdirAll("a/b/c", 0o755)
						if err != nil {
							err = fmt.Errorf("MkdirAll: %w", err)
						} else if i%2 == 0 {
							var f dst.File
							if f, err = w.FS().OpenFile("data", os.O_RDWR|os.O_CREATE, 0o644); err == nil {
								f.Close()
							}
						} else {
							var unlock func() error
							if unlock, err = dst.LockFile(w.FS(), "LOCK"); err == nil {
								unlock()
							} else if errors.Is(err, dst.ErrLocked) {
								err = nil
							}
						}
						if err != nil {
							failed.Add(1)
							first.CompareAndSwap(nil, err.Error())
						}
					}()
				}
				close(start)
				wg.Wait()
			}
		})
		if k := failed.Load(); k > 0 {
			t.Fatalf("%d of %d concurrent creates failed (first: %v)", k, n*goroutines, first.Load())
		}
	})
}

// ---- OnceFunc, OnceValue, OnceValues

// Fresh functions each round, called by many goroutines at once: f runs
// once and every caller gets its result after it finished; a panicking f
// panics every caller with its value, as sync's do.
// Fails if: f runs twice, a caller returns before f finished, or a
// panic differs from sync's.
func TestStressOnceFuncs(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(1000)
		var round atomic.Int64
		within(t, "OnceFunc and OnceValue", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var runs atomic.Int32
				value := dst.OnceValue(func() int { runs.Add(1); runtime.Gosched(); return r })
				bad := dst.OnceFunc(func() { runs.Add(1); panic(r) })
				std := sync.OnceFunc(func() { panic(r) })
				want := panicMsg(std)
				var wg sync.WaitGroup
				errs := make(chan string, 2*goroutines)
				start := make(chan struct{})
				for range goroutines {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						if v := value(); v != r {
							errs <- fmt.Sprintf("OnceValue gave %d, want %d", v, r)
						}
						if m := panicMsg(bad); m != want {
							errs <- fmt.Sprintf("a panicking OnceFunc panicked with %q, sync's with %q", m, want)
						}
					}()
				}
				close(start)
				wg.Wait()
				select {
				case m := <-errs:
					panic(m)
				default:
				}
				if k := runs.Load(); k != 2 {
					panic(fmt.Sprintf("the functions ran %d times, want 2 (once each)", k))
				}
			}
		})
	})
}

// ---- Pool

// pooled is what the Pool test hands around: held while a goroutine
// owns it.
type pooled struct {
	held atomic.Bool
	n    int
}

// Goroutines Get, use and Put objects from one Pool in parallel, and
// simulated goroutines use the same Pool meanwhile (a package-level pool
// shared with simulations): an object is never handed to two holders at
// once, the real World reuses objects (Put then Get can return one),
// and a simulated goroutine always gets a fresh one from New.
// Fails if: two goroutines hold one object at once, the real path never
// reuses, or a simulated Get returns an object a real goroutine Put.
func TestStressPool(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const goroutines = 8
		n := rounds(2000)
		var made atomic.Int64
		p := &dst.Pool{New: func() any { made.Add(1); return new(pooled) }}
		var progress, reused atomic.Int64
		within(t, "Pool", &progress, goroutines*n, func() {
			var wg sync.WaitGroup
			for range goroutines {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range n {
						o := p.Get().(*pooled)
						if !o.held.CompareAndSwap(false, true) {
							panic("Get returned an object another goroutine holds")
						}
						if o.n > 0 {
							reused.Add(1)
						}
						o.n++ // a data race for -race if two hold it
						o.held.Store(false)
						p.Put(o)
						progress.Add(1)
					}
				}()
			}
			simErr := make(chan error, 1)
			go func() {
				r := simworld.Run(simworld.Options{Seed: 7}, func(sw *dst.World) {
					for range 200 {
						if o := p.Get().(*pooled); o.n != 0 {
							panic("a simulated Get returned an object a real goroutine had used")
						}
						p.Put(new(pooled))
						sw.Gosched()
					}
				})
				simErr <- r.Err
			}()
			wg.Wait()
			if err := <-simErr; err != nil {
				panic(fmt.Sprint("the simulation sharing the Pool: ", err))
			}
		})
		if reused.Load() == 0 {
			t.Fatalf("no Get returned a Put object in %d rounds (%d made): the real path isn't pooling", goroutines*n, made.Load())
		}
	})
}

// ---- Gosched and Yield

// On the real World both are runtime.Gosched: a loop spinning on a flag
// another goroutine sets ends with either, under parallel spinners.
// Fails if: either blocks, or never lets the setter run (stuck).
func TestStressGoschedAndYield(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		n := rounds(2000)
		var round atomic.Int64
		within(t, "Gosched and Yield spins", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				var flag atomic.Bool
				var wg sync.WaitGroup
				for i := range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for !flag.Load() {
							if i%2 == 0 {
								w.Gosched()
							} else {
								w.Yield()
							}
						}
					}()
				}
				go flag.Store(true)
				wg.Wait()
			}
		})
	})
}

// ---- Pipe and NetPipe

// Several writers share a Pipe while one goroutine reads it: each write
// arrives whole (writes are serialized, as io's), and every byte written
// is read. On NetPipe, deadlines are set while reads are waiting: each
// read returns data or a timeout, never hangs.
// Fails if: writes interleave, bytes are lost, or a read outlives its
// deadline (stuck).
func TestStressPipes(t *testing.T) {
	bothModes(t, func(t *testing.T, w *dst.World) {
		const writers = 4
		n := rounds(300)
		var round atomic.Int64
		within(t, "Pipe and NetPipe", &round, n, func() {
			for r := 1; r <= n; r++ {
				round.Store(int64(r))
				pr, pw := dst.Pipe(w)
				var wg sync.WaitGroup
				for i := range writers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						pw.Write(bytes.Repeat([]byte{byte('a' + i)}, 64))
					}()
				}
				go func() { wg.Wait(); pw.Close() }()
				got, err := io.ReadAll(pr)
				if err != nil || len(got) != writers*64 {
					panic(fmt.Sprintf("read %d bytes (%v), want %d", len(got), err, writers*64))
				}
				for i := 0; i < len(got); i += 64 {
					if !bytes.Equal(got[i:i+64], bytes.Repeat(got[i:i+1], 64)) {
						panic("two writes interleaved")
					}
				}
				a, b := dst.NetPipe(w)
				go b.Write([]byte("x"))
				buf := make([]byte, 1)
				a.SetReadDeadline(time.Now().Add(time.Duration(r%3) * 50 * time.Microsecond))
				if n, err := a.Read(buf); n == 0 && !errors.Is(err, os.ErrDeadlineExceeded) {
					panic(fmt.Sprintf("a read with a deadline returned %d %v", n, err))
				}
				a.Close()
				b.Close()
			}
		})
	})
}
