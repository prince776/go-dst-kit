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
		prev := m.s.Load()
		if s := simSchedSlow(&m.s, "dst.Mutex"); s != nil {
			s.Yield() // another goroutine may get here first, as in real Go
			for !m.mu.TryLock() {
				checkShared(s, "dst.Mutex", true, prev, m.s.Load())
				m.waiters.Add(1)
				s.ParkOn(m, "lock")
				m.waiters.Add(-1)
			}
			return
		}
	}
	m.mu.Lock()
}

// checkShared is called by a goroutine of s about to wait on a primitive
// that the schedulers in seen were (also) used under. If one of them is
// another simulation that is still running, the primitive is shared
// between simulations running at once: their goroutines can't wake each
// other, so the wait might never end, and the seeds depend on each other.
// For a lock (every simulated goroutine taking it records its run), any
// other run, even one that has ended, means the holder isn't of this
// run: one that ended holding it (a package-level lock left locked).
// Either way the run fails as misuse naming the primitive, not as a
// deadlock later.
func checkShared(s *sched.Scheduler, what string, lock bool, seen ...*sched.Scheduler) {
	for _, o := range seen {
		switch {
		case o == nil || o == s:
		case o.Live():
			s.Misuse(what + " shared between simulations running at once (a package-level variable?): " +
				"one simulation can't wake another's goroutines, and the seeds would depend on each other. " +
				"Make it per instance (a field of the system under test), or run the seeds one at a time (Parallel: 1)")
		case lock:
			s.Misuse(what + " held by a goroutine of another simulation, which has ended (a package-level variable " +
				"shared between simulations?): no goroutine of this one can release it. " +
				"Make it per instance (a field of the system under test)")
		}
	}
}

// simSched returns the scheduler of the simulated goroutine calling, or
// nil in the real World (one inlined atomic load outside any simulation).
// A primitive caches the scheduler it was used under: while the caller
// is that scheduler's running task, it is used without the costly
// goroutine lookup, which happens once per primitive (and again only
// when other goroutines, of another simulation or the real World, use
// it).
//
// Callers write `if sched.Active()` around it so the real World's check
// stays inlined.
func simSchedSlow(cache *atomic.Pointer[sched.Scheduler], what string) *sched.Scheduler {
	if s := cache.Load(); s != nil && s.HeldByCaller() {
		return s
	}
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
		if prev := cache.Swap(s); prev != nil && prev != s && prev.Live() && !s.Unwinding() {
			// Goroutines of another simulation, still running, use it too.
			checkShared(s, what, false, prev)
		}
		return s
	}
	return nil
}

// TryLock is (*sync.Mutex).TryLock. In a simulation it records the run
// it was taken under, as Lock does (checkShared).
func (m *Mutex) TryLock() bool {
	if sched.Active() {
		simSchedSlow(&m.s, "dst.Mutex")
	}
	return m.mu.TryLock()
}

// Unlock is (*sync.Mutex).Unlock.
//
// Like every unlock and Done here, it reads the primitive only before
// releasing it: once released, another goroutine may take it and reset
// or recycle the struct that holds it (a pooled object), as
// sync.Mutex allows.
func (m *Mutex) Unlock() {
	var s *sched.Scheduler
	if sched.Active() {
		if s = simSchedSlow(&m.s, "dst.Mutex"); s != nil && s.Unwinding() {
			return // a killed goroutine's deferred unlock: its process is gone
		}
	}
	wake := s != nil && m.waiters.Load() > 0 // the real World reads nothing here
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
		prev := m.s.Load()
		if s := simSchedSlow(&m.s, "dst.RWMutex"); s != nil {
			s.Yield()
			m.writersWaiting.Add(1)
			for !m.mu.TryLock() {
				checkShared(s, "dst.RWMutex", true, prev, m.s.Load())
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
		prev := m.s.Load()
		if s := simSchedSlow(&m.s, "dst.RWMutex"); s != nil {
			s.Yield()
			for m.writersWaiting.Load() > 0 || !m.mu.TryRLock() {
				checkShared(s, "dst.RWMutex", true, prev, m.s.Load())
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
		if s := simSchedSlow(&m.s, "dst.RWMutex"); s != nil {
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
		if s := simSchedSlow(&m.s, "dst.RWMutex"); s != nil {
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
		if s = simSchedSlow(&m.s, "dst.RWMutex"); s != nil && s.Unwinding() {
			return s, false, true
		}
	}
	return s, s != nil && m.waiters.Load() > 0, false
}

// WaitGroup is sync.WaitGroup for code that runs in a World. Outside a
// simulation every method is the sync.WaitGroup's, plus one branch;
// simulated goroutines count and wait on a count of their own. A group
// belongs to one World: one a simulation used and real goroutines use
// after it ended is fine. Real and simulated goroutines sharing a group
// at once is reported: an Add or Done from a goroutine the simulation
// didn't start, while simulated goroutines wait on the group, fails the
// run as misuse (a check made only while a simulation runs).
//
// Verified:
//   - [wait-follows-done] Outside a simulation, Wait returns only after
//     every Done has returned, as sync.WaitGroup's does, so a group
//     reused straight after Wait (Pebble's batches) stays consistent.
type WaitGroup struct {
	wg      sync.WaitGroup                  // the real World's
	n       atomic.Int64                    // a simulation's count
	waiters atomic.Int32                    // simulated goroutines parked in Wait
	s       atomic.Pointer[sched.Scheduler] // theirs (set by a simulated Wait)
}

// Add is (*sync.WaitGroup).Add.
func (g *WaitGroup) Add(delta int) {
	if sched.Active() {
		if t := sched.CurrentTask(); t != nil {
			n := g.n.Add(int64(delta))
			if n < 0 {
				panic("sync: negative WaitGroup counter")
			}
			if n == 0 && g.waiters.Load() > 0 {
				t.Sched().WakeAll(g) // g as a key only
			}
			return
		}
		if g.waiters.Load() > 0 {
			if s := g.s.Load(); s != nil {
				// Simulated goroutines wait on g, and this goroutine isn't
				// one of theirs: their wake-up would be lost. Current
				// reports the misuse and doesn't return here.
				s.Current()
			}
		}
	}
	g.wg.Add(delta)
}

// Done is (*sync.WaitGroup).Done.
func (g *WaitGroup) Done() { g.Add(-1) }

// Go is (*sync.WaitGroup).Go: it runs fn in a new goroutine of w and
// counts it.
func (g *WaitGroup) Go(w *World, fn func()) {
	if w.s == nil {
		g.wg.Go(fn)
		return
	}
	g.Add(1)
	w.GoNamed(funcName(fn), func() {
		defer g.Done()
		fn()
	})
}

// Wait is (*sync.WaitGroup).Wait.
func (g *WaitGroup) Wait() {
	if sched.Active() {
		if t := sched.CurrentTask(); t != nil {
			s := t.Sched()
			prev := g.s.Swap(s)
			for g.n.Load() != 0 {
				checkShared(s, "dst.WaitGroup", false, prev, g.s.Load())
				g.park(s)
			}
			return
		}
	}
	g.wg.Wait()
}

// park waits for a wake-up, counted as a waiter until it returns or the
// goroutine is killed (a crash unwinds it through its defers).
func (g *WaitGroup) park(s *sched.Scheduler) {
	g.waiters.Add(1)
	defer g.waiters.Add(-1)
	s.ParkOn(g, "WaitGroup.Wait")
}

// Once is sync.Once for code that runs in a World: a second caller waits
// (through the World) while the first runs f.
type Once struct {
	m    Mutex
	done atomic.Bool
}

// Do is (*sync.Once).Do: sync.Once's algorithm, its fast path inlined
// as sync.Once's is.
func (o *Once) Do(f func()) {
	if !o.done.Load() {
		o.doSlow(f)
	}
}

func (o *Once) doSlow(f func()) {
	o.m.Lock()
	defer o.m.Unlock()
	if !o.done.Load() {
		defer o.done.Store(true)
		f()
	}
}
