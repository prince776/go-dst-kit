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
	enqueue(sel *sched.SelState, idx int)
	complete()
	// real World
	realCase() reflect.SelectCase
	realDone(v reflect.Value, ok bool)
}

// OnRecv is `case v, ok := <-c: fn(v, ok)`.
func OnRecv[T any](c Chan[T], fn func(v T, ok bool)) Case { return &recvCase[T]{c: c, fn: fn} }

// OnSend is `case c <- v: fn()`. fn may be nil.
func OnSend[T any](c Chan[T], v T, fn func()) Case { return &sendCase[T]{c: c, v: v, fn: fn} }

// OnDone is `case <-ctx.Done(): fn()`.
func OnDone(ctx context.Context, fn func()) Case {
	return OnRecv(Done(ctx), func(struct{}, bool) { fn() })
}

// Default is `default: fn()`. fn may be nil.
func Default(fn func()) Case { return defaultCase{fn} }

// Select is the select statement: it runs exactly one case's function. If
// some cases are ready it picks one at random (seeded in a simulated
// World); otherwise it runs Default if present, or waits until a case is
// ready. Cases on nil channels are never ready; with no cases it blocks
// forever.
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
	n := 0
	for i, c := range cases {
		if !c.isNil() {
			c.enqueue(sel, i)
			n++
		}
	}
	if n == 0 {
		s.Park("select with no cases that can proceed (nil channels)")
		panic("unreachable")
	}
	s.Park("select")
	cases[sel.Index].complete()
}

func realSelect(cases []Case) {
	rc := make([]reflect.SelectCase, len(cases))
	for i, c := range cases {
		rc[i] = c.realCase()
	}
	i, v, ok := reflect.Select(rc)
	cases[i].realDone(v, ok)
}

type recvCase[T any] struct {
	c  Chan[T]
	fn func(T, bool)
	w  *sched.Waiter[T]
}

func (r *recvCase[T]) isNil() bool     { return r.c.sim == nil }
func (r *recvCase[T]) isDefault() bool { return false }
func (r *recvCase[T]) ready() bool     { return r.c.sim.RecvReady() }
func (r *recvCase[T]) commit() {
	v, ok, _ := r.c.sim.TryRecv()
	r.fn(v, ok)
}
func (r *recvCase[T]) enqueue(sel *sched.SelState, idx int) { r.w = r.c.sim.EnqueueRecv(sel, idx) }
func (r *recvCase[T]) complete()                            { r.fn(r.w.Val, r.w.OK) }
func (r *recvCase[T]) realCase() reflect.SelectCase {
	if r.c.sim != nil {
		panic("dst: simulated channel used in the real World")
	}
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.c.ro)}
}
func (r *recvCase[T]) realDone(v reflect.Value, ok bool) {
	var x T
	if ok {
		reflect.ValueOf(&x).Elem().Set(v)
	}
	r.fn(x, ok)
}

type sendCase[T any] struct {
	c  Chan[T]
	v  T
	fn func()
	w  *sched.Waiter[T]
}

func (s *sendCase[T]) isNil() bool     { return s.c.sim == nil }
func (s *sendCase[T]) isDefault() bool { return false }
func (s *sendCase[T]) ready() bool     { return s.c.sim.SendReady() }
func (s *sendCase[T]) commit() {
	s.c.sim.TrySend(s.v)
	s.done()
}
func (s *sendCase[T]) enqueue(sel *sched.SelState, idx int) { s.w = s.c.sim.EnqueueSend(sel, idx, s.v) }
func (s *sendCase[T]) complete() {
	if s.w.ClosedPanic {
		panic("send on closed channel")
	}
	s.done()
}
func (s *sendCase[T]) done() {
	if s.fn != nil {
		s.fn()
	}
}
func (s *sendCase[T]) realCase() reflect.SelectCase {
	if s.c.sim != nil {
		panic("dst: simulated channel used in the real World")
	}
	if s.c.rw == nil && s.c.ro != nil {
		panic("dst: send on a receive-only channel")
	}
	return reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(s.c.rw), Send: reflect.ValueOf(&s.v).Elem()}
}
func (s *sendCase[T]) realDone(reflect.Value, bool) { s.done() }

type defaultCase struct{ fn func() }

func (d defaultCase) isNil() bool                  { return false }
func (d defaultCase) isDefault() bool              { return true }
func (d defaultCase) ready() bool                  { return true }
func (d defaultCase) commit()                      { d.run() }
func (d defaultCase) enqueue(*sched.SelState, int) {}
func (d defaultCase) complete()                    {}
func (d defaultCase) realCase() reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectDefault}
}
func (d defaultCase) realDone(reflect.Value, bool) { d.run() }
func (d defaultCase) run() {
	if d.fn != nil {
		d.fn()
	}
}
