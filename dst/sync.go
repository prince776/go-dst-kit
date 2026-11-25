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
// An uncontended Lock is not a scheduling point.
type Mutex struct {
	mu      sync.Mutex
	waiters atomic.Int32 // simulated goroutines parked on this mutex
}

// Lock is (*sync.Mutex).Lock.
func (m *Mutex) Lock() {
	if m.mu.TryLock() {
		return
	}
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
		for !m.mu.TryLock() {
			m.waiters.Add(1)
			s.ParkOn(m, "lock")
			m.waiters.Add(-1)
		}
		return
	}
	m.mu.Lock()
}

// TryLock is (*sync.Mutex).TryLock.
func (m *Mutex) TryLock() bool { return m.mu.TryLock() }

// Unlock is (*sync.Mutex).Unlock.
func (m *Mutex) Unlock() {
	m.mu.Unlock()
	if m.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			t.Sched().WakeOne(m)
		}
	}
}

// RWMutex is sync.RWMutex for code that runs in a World. As in Go, once a
// writer waits, new readers wait too.
type RWMutex struct {
	mu             sync.RWMutex
	waiters        atomic.Int32
	writersWaiting atomic.Int32
}

// Lock is (*sync.RWMutex).Lock.
func (m *RWMutex) Lock() {
	if m.mu.TryLock() {
		return
	}
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
		m.writersWaiting.Add(1)
		for !m.mu.TryLock() {
			m.waiters.Add(1)
			s.ParkOn(m, "lock (write)")
			m.waiters.Add(-1)
		}
		m.writersWaiting.Add(-1)
		return
	}
	m.mu.Lock()
}

// Unlock is (*sync.RWMutex).Unlock.
func (m *RWMutex) Unlock() {
	m.mu.Unlock()
	m.wake()
}

// RLock is (*sync.RWMutex).RLock.
func (m *RWMutex) RLock() {
	if m.writersWaiting.Load() == 0 && m.mu.TryRLock() {
		return
	}
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
		for m.writersWaiting.Load() > 0 || !m.mu.TryRLock() {
			m.waiters.Add(1)
			s.ParkOn(m, "lock (read)")
			m.waiters.Add(-1)
		}
		return
	}
	m.mu.RLock()
}

// RUnlock is (*sync.RWMutex).RUnlock.
func (m *RWMutex) RUnlock() {
	m.mu.RUnlock()
	m.wake()
}

func (m *RWMutex) wake() {
	if m.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			t.Sched().WakeAll(m)
		}
	}
}

// WaitGroup is sync.WaitGroup for code that runs in a World.
type WaitGroup struct {
	wg      sync.WaitGroup
	n       atomic.Int64
	waiters atomic.Int32
}

// Add is (*sync.WaitGroup).Add.
func (g *WaitGroup) Add(delta int) {
	n := g.n.Add(int64(delta))
	if n < 0 {
		panic("sync: negative WaitGroup counter")
	}
	g.wg.Add(delta)
	if n == 0 && g.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			t.Sched().WakeAll(g)
		}
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
func (g *WaitGroup) Wait() {
	if g.n.Load() == 0 {
		return
	}
	if t := sched.CurrentTask(); t != nil {
		s := t.Sched()
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
