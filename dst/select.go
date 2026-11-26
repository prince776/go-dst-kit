package dst

import (
	"context"
	"reflect"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Case is one arm of a Select: OnRecv, OnSend, OnDone or Default.
type Case interface {
	isNil() bool
	isDefault() bool
	// simulated World
	ready() bool
	commit()
	enqueue(sel *sched.SelState, idx int) (complete func())
	// real World
	realCase() reflect.SelectCase
	realDone(v reflect.Value, ok bool)
}

// Op is a Case on a channel of T: what OnRecv, OnSend and OnDone return,
// and what Select2 and Select3 take. It is a small value; Select2 and
// Select3 don't allocate for it.
type Op[T any] struct {
	c    Chan[T]
	send bool
	v    T             // the value to send
	recv func(T, bool) // a receive's function
	sent func()        // a send's function (may be nil)
}

// OnRecv is `case v, ok := <-c: fn(v, ok)`.
func OnRecv[T any](c Chan[T], fn func(v T, ok bool)) Op[T] { return Op[T]{c: c, recv: fn} }

// OnSend is `case c <- v: fn()`. fn may be nil.
func OnSend[T any](c Chan[T], v T, fn func()) Op[T] { return Op[T]{c: c, send: true, v: v, sent: fn} }

// OnDone is `case <-ctx.Done(): fn()`.
func OnDone(ctx context.Context, fn func()) Op[struct{}] {
	return OnRecv(Done(ctx), func(struct{}, bool) { fn() })
}

// Default is `default: fn()`. fn may be nil.
func Default(fn func()) Case { return defaultCase{fn} }

// Select is the select statement: it runs exactly one case's function. If
// some cases are ready it picks one at random (seeded in a simulated
// World); otherwise it runs Default if present, or waits until a case is
// ready. Cases on nil channels are never ready; with no cases it blocks
// forever.
//
// In the real World it uses reflection (about 4x the cost of a native
// select); Select2 and Select3 are native for two or three cases.
func Select(w *World, cases ...Case) {
	if w.s == nil {
		realSelect(cases)
		return
	}
	s := w.s
	s.Yield()
	var def Case
	for _, i := range s.Perm(len(cases)) {
		c := cases[i]
		switch {
		case c.isDefault():
			def = c
		case c.isNil():
		case c.ready():
			c.commit()
			return
		}
	}
	if def != nil {
		def.commit()
		return
	}
	sel := &sched.SelState{}
	completes := make([]func(), len(cases))
	n := 0
	for i, c := range cases {
		if !c.isNil() {
			completes[i] = c.enqueue(sel, i)
			n++
		}
	}
	if n == 0 {
		s.Park("select with no cases that can proceed (nil channels)")
		panic("unreachable")
	}
	s.Park("select")
	completes[sel.Index]()
}

func realSelect(cases []Case) {
	rc := make([]reflect.SelectCase, len(cases))
	for i, c := range cases {
		rc[i] = c.realCase()
	}
	i, v, ok := reflect.Select(rc)
	cases[i].realDone(v, ok)
}

// fire runs the case's function: a receive's with (v, ok), a send's.
func (o Op[T]) fire(v T, ok bool) {
	if o.send {
		if o.sent != nil {
			o.sent()
		}
		return
	}
	o.recv(v, ok)
}

func (o Op[T]) isNil() bool     { return o.c.sim == nil }
func (o Op[T]) isDefault() bool { return false }

func (o Op[T]) ready() bool {
	if o.send {
		return o.c.sim.SendReady()
	}
	return o.c.sim.RecvReady()
}

func (o Op[T]) commit() {
	if o.send {
		o.c.sim.TrySend(o.v)
		o.fire(o.v, true)
		return
	}
	v, ok, _ := o.c.sim.TryRecv()
	o.fire(v, ok)
}

func (o Op[T]) enqueue(sel *sched.SelState, idx int) func() {
	if o.send {
		w := o.c.sim.EnqueueSend(sel, idx, o.v)
		return func() {
			if w.ClosedPanic {
				panic("send on closed channel")
			}
			o.fire(o.v, true)
		}
	}
	w := o.c.sim.EnqueueRecv(sel, idx)
	return func() { o.fire(w.Val, w.OK) }
}

func (o Op[T]) realCase() reflect.SelectCase {
	r, s := realChans(o.c, o.send)
	if o.send {
		v := o.v // only this copy escapes (taking &o.v would move all of o)
		return reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(s), Send: reflect.ValueOf(&v).Elem()}
	}
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r)}
}

func (o Op[T]) realDone(v reflect.Value, ok bool) {
	if ok && !o.send {
		var x T
		reflect.ValueOf(&x).Elem().Set(v)
		o.fire(x, true)
		return
	}
	var zero T
	o.fire(zero, ok)
}

type defaultCase struct{ fn func() }

func (d defaultCase) isNil() bool                         { return false }
func (d defaultCase) isDefault() bool                     { return true }
func (d defaultCase) ready() bool                         { return true }
func (d defaultCase) commit()                             { d.run() }
func (d defaultCase) enqueue(*sched.SelState, int) func() { return nil }
func (d defaultCase) realCase() reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectDefault}
}
func (d defaultCase) realDone(reflect.Value, bool) { d.run() }
func (d defaultCase) run() {
	if d.fn != nil {
		d.fn()
	}
}
