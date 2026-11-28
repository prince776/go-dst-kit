package dst

import (
	"sync"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Pool is sync.Pool for code that runs in a World. In the real World it
// is a sync.Pool (a goroutine outside every simulation pays one atomic
// load more). Inside a simulation, Get always returns New's result (nil
// for a nil New, as sync.Pool's does) and Put drops x: a pool is usually
// a package-level variable, shared by every simulation in the process,
// and an object passing through it would carry one seed's state into
// another, as would one a killed goroutine's deferred Put returns at the
// end-of-simulation unwind. A simulation loses pooling's allocation
// savings, never its behaviour: code must not rely on getting back what
// it put.
//
// Verified:
//   - [pools-dont-cross-seeds] An object Put in one simulation is never
//     returned by Get in another, nor one Put by a killed goroutine's
//     deferred functions; inside a simulation Get always calls New. In
//     the real World, Put then Get can return the same object.
type Pool struct {
	// New, if set, makes a value when Get would return nil, as
	// sync.Pool's New does.
	New func() any

	p sync.Pool
}

// simulated reports whether the calling goroutine is a simulated one.
func simulated() bool { return sched.Active() && sched.CurrentTask() != nil }

// Get is (*sync.Pool).Get.
func (p *Pool) Get() any {
	if simulated() {
		if p.New == nil {
			return nil
		}
		return p.New()
	}
	if x := p.p.Get(); x != nil {
		return x
	}
	if p.New != nil {
		return p.New()
	}
	return nil
}

// Put is (*sync.Pool).Put.
func (p *Pool) Put(x any) {
	if simulated() {
		return
	}
	p.p.Put(x)
}
