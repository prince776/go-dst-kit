package dst

import "errors"

// Fault is what a fault point decided for one call.
type Fault int

const (
	// None: proceed normally.
	None Fault = iota
	// Fail: the operation must not happen, and the caller sees an error
	// (an unavailable service, a rejected request).
	Fail
	// FailAfter: the operation happens, but the caller sees an error
	// anyway (a timeout or lost reply after the service acted).
	FailAfter
)

func (f Fault) String() string {
	switch f {
	case Fail:
		return "Fail"
	case FailAfter:
		return "FailAfter"
	}
	return "None"
}

// ErrInjected is the error Do returns for an injected fault. Fakes that
// use Inject directly can wrap it, or return their own service's errors.
var ErrInjected = errors.New("dst: injected fault")

// Inject is a fault point: put it in a fake of a service (an object store,
// a database client, a queue), so a simulation can make the service fail,
// fail after acting, or be slow. name identifies the point, as
// "service.operation": a service outage fails every point of the service
// at once, and test options select points by prefix.
//
// In the real World it always returns None and costs one comparison. In a
// simulation the runner decides, per seed and only during the fault phase
// (see dsttest.Faults.Points); a slow call's latency is slept here, before
// Inject returns. A quiet Inject doesn't change the schedule: adding one
// leaves every other decision of the seed as it was.
//
//	func (b *FakeBucket) Put(key string, data []byte) error {
//		f := dst.Inject(b.w, "objstore.put")
//		if f == dst.Fail {
//			return errUnavailable
//		}
//		b.store(key, data)
//		if f == dst.FailAfter {
//			return errTimeout
//		}
//		return nil
//	}
func Inject(w *World, name string) Fault {
	if w.s == nil || w.s.Points == nil {
		return None
	}
	k, lat := w.s.Points.Inject(name)
	if lat > 0 {
		w.Sleep(lat)
	}
	return Fault(k)
}

// Do runs op as the operation behind the fault point name: on Fail it
// returns ErrInjected without running op; on FailAfter it runs op and
// returns ErrInjected whatever op returned.
func Do(w *World, name string, op func() error) error {
	switch Inject(w, name) {
	case Fail:
		return ErrInjected
	case FailAfter:
		op()
		return ErrInjected
	}
	return op()
}

// Buggify reports whether the named BUGGIFY point fires (after
// FoundationDB): code may then take a legal but unusual path (a tiny
// buffer, an early timeout, a reordered retry) that is hard to reach
// otherwise. It is always false in the real World. In a simulation, each
// point is active in about a quarter of seeds and then fires on about a
// quarter of calls, only during the fault phase.
func (w *World) Buggify(name string) bool {
	if w.s == nil || w.s.Points == nil {
		return false
	}
	return w.s.Points.Buggify(name)
}
