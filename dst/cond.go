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
// In the real World it is a sync.Cond, held in the Cond itself (no
// allocation of its own). In a simulated World waiters park on the
// scheduler.
type Cond struct {
	L sync.Locker

	real    sync.Cond                       // the real World's; its L is set once, from L
	ready   atomic.Uint32                   // real.L is set
	init    sync.Mutex                      // guards setting it (a Cond made as a literal)
	waiters atomic.Int32                    // simulated goroutines parked in Wait
	s       atomic.Pointer[sched.Scheduler] // the last waiters' (checkShared)
}

// NewCond returns a Cond with locker l. Its real-World half is set up
// here, so outside a simulation every method is sync.Cond's plus one
// branch; a Cond made as a literal (Cond{L: l}) sets it up on first use.
func NewCond(l sync.Locker) *Cond {
	c := &Cond{L: l}
	c.real.L = l
	c.ready.Store(1)
	return c
}

// Wait atomically unlocks c.L and suspends the caller until Signal or
// Broadcast wakes it, then locks c.L again before returning. As with
// sync.Cond, the caller should wait in a loop that rechecks its
// condition.
func (c *Cond) Wait() {
	if t := sched.CurrentTask(); t != nil { // nil at once outside a simulation
		// Unlock and park can't be separated: no other simulated goroutine
		// runs until this one parks.
		s := t.Sched()
		prev := c.s.Swap(s)
		c.L.Unlock()
		// After the Unlock: a goroutine failed for sharing doesn't keep
		// the lock from the other simulation's goroutines.
		checkShared(s, "dst.Cond", false, prev)
		c.waiters.Add(1)
		s.ParkOn(c, "Cond.Wait")
		c.waiters.Add(-1)
		c.L.Lock()
		return
	}
	c.realCond().Wait()
}

// Signal wakes one goroutine waiting on c, if there is any.
func (c *Cond) Signal() {
	if sched.Active() && c.simWake(false) { // inlined: the real World's cost is the flag
		return
	}
	c.realCond().Signal()
}

// Broadcast wakes all goroutines waiting on c.
func (c *Cond) Broadcast() {
	if sched.Active() && c.simWake(true) {
		return
	}
	c.realCond().Broadcast()
}

// simWake wakes c's simulated waiters (one, or all) if the caller is a
// simulated goroutine and some wait, reporting whether it did.
func (c *Cond) simWake(all bool) bool {
	if c.waiters.Load() == 0 {
		return false
	}
	t := sched.CurrentTask()
	if t == nil {
		return false
	}
	if all {
		t.Sched().WakeAll(c)
	} else {
		t.Sched().WakeOne(c)
	}
	return true
}

// realCond is the real World's sync.Cond, set up on first use for a Cond
// made as a literal.
func (c *Cond) realCond() *sync.Cond {
	if c.ready.Load() == 0 {
		c.initReal()
	}
	return &c.real
}

func (c *Cond) initReal() {
	c.init.Lock()
	defer c.init.Unlock()
	if c.ready.Load() == 0 {
		c.real.L = c.L
		c.ready.Store(1)
	}
}
