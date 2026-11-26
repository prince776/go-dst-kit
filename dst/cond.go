package dst

import (
	"sync"
	"sync/atomic"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Cond is sync.Cond for code that runs in a World: a rendezvous point for
// goroutines waiting for an event. Set L (usually a *Mutex or *RWMutex)
// before use, or make it with NewCond. As with sync.Cond, a Cond must not
// be copied after first use, Signal and Broadcast may be called with or
// without L held, and a Signal or Broadcast with no waiters is lost.
//
// In the real World it is a sync.Cond. In a simulated World waiters park
// on the scheduler.
type Cond struct {
	L sync.Locker

	real    atomic.Pointer[sync.Cond] // created by the first real Wait
	waiters atomic.Int32              // simulated goroutines parked in Wait
}

// NewCond returns a Cond with locker l.
func NewCond(l sync.Locker) *Cond { return &Cond{L: l} }

// Wait atomically unlocks c.L and suspends the caller until Signal or
// Broadcast wakes it, then locks c.L again before returning. As with
// sync.Cond, the caller should wait in a loop that rechecks its
// condition.
func (c *Cond) Wait() {
	if t := sched.CurrentTask(); t != nil {
		// Unlock and park can't be separated: no other simulated goroutine
		// runs until this one parks.
		c.waiters.Add(1)
		c.L.Unlock()
		t.Sched().ParkOn(c, "Cond.Wait")
		c.waiters.Add(-1)
		c.L.Lock()
		return
	}
	c.realCond().Wait()
}

// Signal wakes one goroutine waiting on c, if there is any.
func (c *Cond) Signal() {
	if c.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			t.Sched().WakeOne(c)
			return
		}
	}
	if r := c.real.Load(); r != nil {
		r.Signal()
	}
}

// Broadcast wakes all goroutines waiting on c.
func (c *Cond) Broadcast() {
	if c.waiters.Load() > 0 {
		if t := sched.CurrentTask(); t != nil {
			t.Sched().WakeAll(c)
			return
		}
	}
	if r := c.real.Load(); r != nil {
		r.Broadcast()
	}
}

func (c *Cond) realCond() *sync.Cond {
	if r := c.real.Load(); r != nil {
		return r
	}
	c.real.CompareAndSwap(nil, sync.NewCond(c.L))
	return c.real.Load()
}
