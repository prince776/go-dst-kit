package dst

import (
	"context"
	"errors"
	"time"
)

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
// Inject returns, without watching any context. A quiet Inject doesn't
// change the schedule: adding one leaves every other decision of the seed
// as it was.
//
// Most fakes don't call Inject: they stay plain, and a wrapper adds faults
// with one Call per method. Inject is for a fake that handles faults
// itself:
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
	f, lat := decide(w, name)
	if lat > 0 {
		w.Sleep(lat)
	}
	return f
}

// decide asks the runner for one call's decision and latency.
func decide(w *World, name string) (Fault, time.Duration) {
	if w.s == nil || w.s.Points == nil {
		return None, 0
	}
	k, lat := w.s.Points.Inject(name)
	return Fault(k), lat
}

// Errors are what a fault point's caller sees, as the real client would
// report it: Before when the request was refused (Fail: nothing happened),
// After when the reply was lost (FailAfter: it happened). A nil function
// returns ErrInjected. Map the two to different errors only if the real
// client can tell them apart.
type Errors struct {
	Before func() error
	After  func() error
}

func (e Errors) before() error {
	if e.Before == nil {
		return ErrInjected
	}
	return e.Before()
}

func (e Errors) after() error {
	if e.After == nil {
		return ErrInjected
	}
	return e.After()
}

// Call runs op, a call to a service you don't run, behind the fault point
// name: the building block of a fault-injecting wrapper around a fake.
//
//   - None: op runs; Call returns its result.
//   - Fail: op doesn't run; Call returns errs.Before().
//   - FailAfter: op runs (its effects stay); Call returns errs.After(),
//     discarding op's result.
//
// Injected latency is waited first, and is cut short if ctx is done: Call
// then returns ctx.Err(), and op still runs unless the decision was Fail
// (a slow request the service received still takes effect, though the
// caller gave up on it). So op stands for the service acting: it
// shouldn't give up because ctx is done. A ctx already done when Call
// starts means the request is never sent: op doesn't run.
//
// In the real World Call just returns op().
func Call[T any](ctx context.Context, w *World, name string, errs Errors, op func() (T, error)) (T, error) {
	var zero T
	if w.s != nil && w.s.Points != nil {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
	}
	f, lat := decide(w, name)
	if lat > 0 && !waitOrDone(ctx, w, lat) {
		if f != Fail {
			op()
		}
		return zero, ctx.Err()
	}
	switch f {
	case Fail:
		return zero, errs.before()
	case FailAfter:
		op()
		return zero, errs.after()
	}
	return op()
}

// waitOrDone waits d, reporting false if ctx was done first.
func waitOrDone(ctx context.Context, w *World, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	t := w.NewTimer(d) // (a context that is never done is a nil arm: never ready)
	defer t.Stop()
	slept := false
	Select2(w, OnRecv(t.C, func(time.Time, bool) { slept = true }), OnDone(ctx, func() {}))
	return slept
}

// Do is Call for an operation that returns only an error, with no
// context and ErrInjected for both faults.
func Do(w *World, name string, op func() error) error {
	_, err := Call(context.Background(), w, name, Errors{}, func() (struct{}, error) { return struct{}{}, op() })
	return err
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
