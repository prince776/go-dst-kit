package dst

import (
	"sync"
	"sync/atomic"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Mutex is sync.Mutex for code that runs in a World. The zero value is an
// unlocked mutex. In the real World it is a sync.Mutex; in a simulated
// World a contended Lock waits through the scheduler instead of blocking
// the baton.
//
// In a simulated World, Lock (like RWMutex's Lock and RLock) is a
// scheduling point: another goroutine may run before it acquires, as in
// real Go, so races between separate critical sections are explored.
//
// A Mutex (like every dst primitive) belongs to one World: don't share
// one between simulations running in parallel, or between a simulation
// and real code (a package-level mutex in code under test would be).
//
// Verified:
//   - [lock-is-a-scheduling-point] Another goroutine may run before Lock
//     acquires. So a lost update across two locked sections is found.
//   - [no-touch-after-release] Outside a simulation, an Unlock or RUnlock
//     (of a Mutex or RWMutex) and a WaitGroup's Done don't touch their
//     primitive once they have released it, as sync's don't: whoever goes
//     next (the next holder, the returning Wait) may reset or recycle the
//     struct that holds it at once, and -race stays quiet.
type Mutex struct {
	mu      sync.Mutex
	waiters atomic.Int32 // simulated goroutines parked on this mutex
	s       atomic.Pointer[sched.Scheduler]
}

// Lock is (*sync.Mutex).Lock.
func (m *Mutex) Lock() {
	if sched.Active() { // inlined: the real World's whole cost
		if s := simSchedSlow(&m.s); s != nil {
			s.Yield() // another goroutine may get here first, as in real Go
			for !m.mu.TryLock() {
				m.waiters.Add(1)
				s.ParkOn(m, "lock")
				m.waiters.Add(-1)
			}
			return
		}
	}
	m.mu.Lock()
}

// simSched returns the scheduler of the simulated goroutine calling, or
// nil in the real World (one inlined atomic load outside any simulation).
// A primitive caches the scheduler it was used under: while that one is
// mid-step, its running task is the caller (the scheduler's identity
// check verifies this), so the costly goroutine lookup happens once per
// primitive, not once per call. A primitive belongs to one World.
//
// Callers write `if sched.Active()` around it so the real World's check
// stays inlined.
func simSchedSlow(cache *atomic.Pointer[sched.Scheduler]) *sched.Scheduler {
	if s := cache.Load(); s != nil && s.Stepping() {
		return s
	}
	if t := sched.CurrentTask(); t != nil {
		cache.Store(t.Sched())
		return t.Sched()
	}
	return nil
}

// TryLock is (*sync.Mutex).TryLock.
func (m *Mutex) TryLock() bool { return m.mu.TryLock() }

// Unlock is (*sync.Mutex).Unlock.
//
// Like every unlock and Done here, it reads the primitive only before
// releasing it: once released, another goroutine may take it and reset
// or recycle the struct that holds it (a pooled object), as
// sync.Mutex allows.
func (m *Mutex) Unlock() {
	var s *sched.Scheduler
	if sched.Active() {
		if s = simSchedSlow(&m.s); s != nil && s.Unwinding() {
			return // a killed goroutine's deferred unlock: its process is gone
		}
	}
	wake := m.waiters.Load() > 0 && s != nil
	m.mu.Unlock()
	if wake {
		s.WakeOne(m) // m as a key only
	}
}

// RWMutex is sync.RWMutex for code that runs in a World. As in Go, once a
// writer waits, new readers wait too.
type RWMutex struct {
	mu             sync.RWMutex
	waiters        atomic.Int32
	writersWaiting atomic.Int32
	s              atomic.Pointer[sched.Scheduler]
}

// Lock is (*sync.RWMutex).Lock.
func (m *RWMutex) Lock() {
	if sched.Active() { // inlined: the real World's whole cost
		if s := simSchedSlow(&m.s); s != nil {
			s.Yield()
			m.writersWaiting.Add(1)
			for !m.mu.TryLock() {
				m.waiters.Add(1)
				s.ParkOn(m, "lock (write)")
				m.waiters.Add(-1)
			}
			m.writersWaiting.Add(-1)
			return
		}
	}
	m.mu.Lock()
}

// Unlock is (*sync.RWMutex).Unlock.
func (m *RWMutex) Unlock() {
	s, wake, killed := m.beforeRelease()
	if killed {
		return // a killed goroutine's deferred unlock: its process is gone
	}
	m.mu.Unlock()
	if wake {
		s.WakeAll(m) // m as a key only
	}
}

// RLock is (*sync.RWMutex).RLock.
func (m *RWMutex) RLock() {
	if sched.Active() { // inlined: the real World's whole cost
		if s := simSchedSlow(&m.s); s != nil {
			s.Yield()
			for m.writersWaiting.Load() > 0 || !m.mu.TryRLock() {
				m.waiters.Add(1)
				s.ParkOn(m, "lock (read)")
				m.waiters.Add(-1)
			}
			return
		}
	}
	m.mu.RLock()
}

// RUnlock is (*sync.RWMutex).RUnlock.
func (m *RWMutex) RUnlock() {
	s, wake, killed := m.beforeRelease()
	if killed {
		return // a killed goroutine's deferred unlock: its process is gone
	}
	m.mu.RUnlock()
	if wake {
		s.WakeAll(m) // m as a key only
	}
}

// TryLock is (*sync.RWMutex).TryLock: it fails while anyone holds the
// lock or a writer waits for it. In a simulated World it is a scheduling
// point, as Lock is.
func (m *RWMutex) TryLock() bool {
	if sched.Active() {
		if s := simSchedSlow(&m.s); s != nil {
			s.Yield()
			if m.writersWaiting.Load() > 0 {
				return false // a parked writer: sync's would hold the lock's writer half
			}
		}
	}
	return m.mu.TryLock()
}

// TryRLock is (*sync.RWMutex).TryRLock: it fails while a writer holds the
// lock or waits for it. In a simulated World it is a scheduling point, as
// RLock is.
func (m *RWMutex) TryRLock() bool {
	if sched.Active() {
		if s := simSchedSlow(&m.s); s != nil {
			s.Yield()
			if m.writersWaiting.Load() > 0 {
				return false // a parked writer, which sync's would see
			}
		}
	}
	return m.mu.TryRLock()
}

// RLocker is (*sync.RWMutex).RLocker: a Locker whose Lock and Unlock are
// m's RLock and RUnlock.
func (m *RWMutex) RLocker() sync.Locker { return (*rlocker)(m) }

type rlocker RWMutex

func (r *rlocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*RWMutex)(r).RUnlock() }

// beforeRelease reads what an unlock needs before it releases m (see
// Mutex.Unlock): the caller's scheduler, whether simulated goroutines wait,
// and whether the caller is a killed goroutine unwinding.
func (m *RWMutex) beforeRelease() (s *sched.Scheduler, wake, killed bool) {
	if sched.Active() {
		if s = simSchedSlow(&m.s); s != nil && s.Unwinding() {
			return s, false, true
		}
	}
	return s, s != nil && m.waiters.Load() > 0, false
}

// WaitGroup is sync.WaitGroup for code that runs in a World.
//
// Verified:
//   - [wait-follows-done] Outside a simulation, Wait returns only after
//     every Done has returned, as sync.WaitGroup's does, so a group
//     reused straight after Wait (Pebble's batches) stays consistent.
type WaitGroup struct {
	wg      sync.WaitGroup
	n       atomic.Int64
	waiters atomic.Int32
	s       atomic.Pointer[sched.Scheduler] // the waiters' scheduler
}

// Add is (*sync.WaitGroup).Add.
func (g *WaitGroup) Add(delta int) {
	n := g.n.Add(int64(delta))
	if n < 0 {
		panic("sync: negative WaitGroup counter")
	}
	// Decided before the sync.WaitGroup's Add: once that lets a Wait
	// return, the group is the waiter's to reset or recycle, and a Done
	// must not touch it any more (sync.WaitGroup's doesn't).
	var wake *sched.Scheduler
	if n == 0 && g.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			wake = t.Sched()
		} else if s := g.s.Load(); s != nil {
			// Simulated goroutines wait, and this goroutine isn't one of
			// theirs: their wake-up would be lost. Current reports the
			// misuse and doesn't return here.
			s.Current()
			wake = s
		}
	}
	g.wg.Add(delta)
	if wake != nil {
		wake.WakeAll(g) // g as a key only
	}
}

// Done is (*sync.WaitGroup).Done.
func (g *WaitGroup) Done() { g.Add(-1) }

// Go is (*sync.WaitGroup).Go: it runs fn in a new goroutine of w and
// counts it.
func (g *WaitGroup) Go(w *World, fn func()) {
	g.Add(1)
	w.GoNamed(funcName(fn), func() {
		defer g.Done()
		fn()
	})
}

// Wait is (*sync.WaitGroup).Wait.
//
// Outside a simulation it is exactly the sync.WaitGroup's Wait: it
// returns only once every Done has returned, so the group can be reused
// at once (a Done updates the simulation's count before the
// sync.WaitGroup, so that count reading zero doesn't mean the last Done
// has finished).
func (g *WaitGroup) Wait() {
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
		g.s.Store(s)
		for g.n.Load() != 0 {
			g.waiters.Add(1)
			s.ParkOn(g, "WaitGroup.Wait")
			g.waiters.Add(-1)
		}
		return
	}
	g.wg.Wait()
}

// Once is sync.Once for code that runs in a World: a second caller waits
// (through the World) while the first runs f.
type Once struct {
	m    Mutex
	done atomic.Bool
}

// Do is (*sync.Once).Do.
func (o *Once) Do(f func()) {
	if o.done.Load() {
		return
	}
	o.m.Lock()
	defer o.m.Unlock()
	if !o.done.Load() {
		defer o.done.Store(true)
		f()
	}
}
