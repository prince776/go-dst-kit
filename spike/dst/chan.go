package dst

import (
	"fmt"
	"reflect"
	"time"
)

// Chan is a channel usable from both Worlds. In the Real World it is a plain
// Go channel; in the Sim it is a model of one whose waits the scheduler sees.
type Chan[T any] struct {
	real chan T
	sim  *simChan[T]
}

// MakeChan is make(chan T, size) for w.
func MakeChan[T any](w World, size int) Chan[T] {
	if s, ok := w.(*Sim); ok {
		s.nextChan++
		return Chan[T]{sim: &simChan[T]{s: s, id: s.nextChan, size: size}}
	}
	return Chan[T]{real: make(chan T, size)}
}

// After is time.After for w.
func After(w World, d time.Duration) Chan[time.Time] {
	c := MakeChan[time.Time](w, 1)
	if s, ok := w.(*Sim); ok {
		s.at(s.now+d, func() { c.sim.trySend(s.Now()) })
	} else {
		time.AfterFunc(d, func() { c.real <- time.Now() })
	}
	return c
}

func (c Chan[T]) Send(v T) {
	if c.sim != nil {
		c.sim.send(v)
		return
	}
	c.real <- v
}

func (c Chan[T]) Recv() (T, bool) {
	if c.sim != nil {
		return c.sim.recv()
	}
	v, ok := <-c.real
	return v, ok
}

func (c Chan[T]) Close() {
	if c.sim != nil {
		c.sim.close()
		return
	}
	close(c.real)
}

// ---- the simulated channel ----

type simChan[T any] struct {
	s      *Sim
	id     int
	size   int
	buf    []T
	closed bool
	recvq  []*waiter[T]
	sendq  []*waiter[T]
}

type waiter[T any] struct {
	t           *task
	sel         *selState // nil for a plain send or receive
	idx         int
	val         T
	ok          bool
	closedPanic bool
}

type selState struct {
	fired bool
	idx   int
}

func (w *waiter[T]) alive() bool { return w.sel == nil || !w.sel.fired }

func (c *simChan[T]) hasAlive(q *[]*waiter[T]) bool {
	for len(*q) > 0 && !(*q)[0].alive() {
		*q = (*q)[1:]
	}
	return len(*q) > 0
}

func (c *simChan[T]) popAlive(q *[]*waiter[T]) *waiter[T] {
	if !c.hasAlive(q) {
		return nil
	}
	w := (*q)[0]
	*q = (*q)[1:]
	return w
}

func (c *simChan[T]) fire(w *waiter[T]) {
	if w.sel != nil {
		w.sel.fired, w.sel.idx = true, w.idx
	}
	c.s.runnable = append(c.s.runnable, w.t)
}

func (c *simChan[T]) sendReady() bool {
	return c.closed || c.hasAlive(&c.recvq) || len(c.buf) < c.size
}

func (c *simChan[T]) trySend(v T) bool {
	if c.closed {
		panic("send on closed channel")
	}
	if r := c.popAlive(&c.recvq); r != nil {
		r.val, r.ok = v, true
		c.fire(r)
		return true
	}
	if len(c.buf) < c.size {
		c.buf = append(c.buf, v)
		return true
	}
	return false
}

func (c *simChan[T]) recvReady() bool {
	return len(c.buf) > 0 || c.hasAlive(&c.sendq) || c.closed
}

func (c *simChan[T]) tryRecv() (v T, ok, done bool) {
	if len(c.buf) > 0 {
		v, c.buf = c.buf[0], c.buf[1:]
		if sw := c.popAlive(&c.sendq); sw != nil {
			c.buf = append(c.buf, sw.val)
			c.fire(sw)
		}
		return v, true, true
	}
	if sw := c.popAlive(&c.sendq); sw != nil {
		c.fire(sw)
		return sw.val, true, true
	}
	if c.closed {
		return v, false, true
	}
	return v, false, false
}

func (c *simChan[T]) send(v T) {
	t := c.s.current()
	c.s.yield(t)
	if c.trySend(v) {
		return
	}
	w := &waiter[T]{t: t, val: v}
	c.sendq = append(c.sendq, w)
	c.s.park(t, fmt.Sprintf("send on chan#%d (full, %d buffered)", c.id, len(c.buf)))
	if w.closedPanic {
		panic("send on closed channel")
	}
}

func (c *simChan[T]) recv() (T, bool) {
	t := c.s.current()
	c.s.yield(t)
	if v, ok, done := c.tryRecv(); done {
		return v, ok
	}
	w := &waiter[T]{t: t}
	c.recvq = append(c.recvq, w)
	c.s.park(t, fmt.Sprintf("receive from chan#%d (empty)", c.id))
	return w.val, w.ok
}

func (c *simChan[T]) close() {
	t := c.s.current()
	c.s.yield(t)
	if c.closed {
		panic("close of closed channel")
	}
	c.closed = true
	for w := c.popAlive(&c.recvq); w != nil; w = c.popAlive(&c.recvq) {
		var zero T
		w.val, w.ok = zero, false
		c.fire(w)
	}
	for w := c.popAlive(&c.sendq); w != nil; w = c.popAlive(&c.sendq) {
		w.closedPanic = true
		c.fire(w)
	}
}

// ---- select ----

// Case is one arm of a Select.
type Case interface {
	simReady() bool
	simCommit()
	simEnqueue(t *task, sel *selState, idx int)
	simComplete()
	realCase() reflect.SelectCase
	realDone(v reflect.Value, ok bool)
}

// OnRecv is `case v, ok := <-c: fn(v, ok)`.
func OnRecv[T any](c Chan[T], fn func(v T, ok bool)) Case { return &recvCase[T]{c: c, fn: fn} }

// OnSend is `case c <- v: fn()`; fn may be nil.
func OnSend[T any](c Chan[T], v T, fn func()) Case { return &sendCase[T]{c: c, v: v, fn: fn} }

// Default is `default: fn()`.
func Default(fn func()) Case { return defaultCase{fn} }

// Select is the select statement for w: it runs exactly one ready case's
// function, blocking until one is ready unless there's a Default. Among
// ready cases the choice is random, as in Go (seeded in the Sim).
func Select(w World, cases ...Case) {
	s, ok := w.(*Sim)
	if !ok {
		rc := make([]reflect.SelectCase, len(cases))
		for i, c := range cases {
			rc[i] = c.realCase()
		}
		i, v, ok := reflect.Select(rc)
		cases[i].realDone(v, ok)
		return
	}
	t := s.current()
	s.yield(t)
	var def Case
	for _, i := range s.rng.Perm(len(cases)) {
		c := cases[i]
		if _, isDef := c.(defaultCase); isDef {
			def = c
			continue
		}
		if c.simReady() {
			c.simCommit()
			return
		}
	}
	if def != nil {
		def.simCommit()
		return
	}
	sel := &selState{}
	for i, c := range cases {
		c.simEnqueue(t, sel, i)
	}
	s.park(t, "select")
	cases[sel.idx].simComplete()
}

type recvCase[T any] struct {
	c  Chan[T]
	fn func(T, bool)
	w  *waiter[T]
}

func (r *recvCase[T]) simReady() bool { return r.c.sim.recvReady() }
func (r *recvCase[T]) simCommit() {
	v, ok, _ := r.c.sim.tryRecv()
	r.fn(v, ok)
}
func (r *recvCase[T]) simEnqueue(t *task, sel *selState, idx int) {
	r.w = &waiter[T]{t: t, sel: sel, idx: idx}
	r.c.sim.recvq = append(r.c.sim.recvq, r.w)
}
func (r *recvCase[T]) simComplete() { r.fn(r.w.val, r.w.ok) }
func (r *recvCase[T]) realCase() reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.c.real)}
}
func (r *recvCase[T]) realDone(v reflect.Value, ok bool) {
	var x T
	if ok {
		x = v.Interface().(T)
	}
	r.fn(x, ok)
}

type sendCase[T any] struct {
	c  Chan[T]
	v  T
	fn func()
	w  *waiter[T]
}

func (s *sendCase[T]) simReady() bool { return s.c.sim.sendReady() }
func (s *sendCase[T]) simCommit() {
	s.c.sim.trySend(s.v)
	s.done()
}
func (s *sendCase[T]) simEnqueue(t *task, sel *selState, idx int) {
	s.w = &waiter[T]{t: t, sel: sel, idx: idx, val: s.v}
	s.c.sim.sendq = append(s.c.sim.sendq, s.w)
}
func (s *sendCase[T]) simComplete() {
	if s.w.closedPanic {
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
	return reflect.SelectCase{Dir: reflect.SelectSend, Chan: reflect.ValueOf(s.c.real), Send: reflect.ValueOf(&s.v).Elem()}
}
func (s *sendCase[T]) realDone(reflect.Value, bool) { s.done() }

type defaultCase struct{ fn func() }

func (d defaultCase) simReady() bool                   { return true }
func (d defaultCase) simCommit()                       { d.fn() }
func (d defaultCase) simEnqueue(*task, *selState, int) {}
func (d defaultCase) simComplete()                     {}
func (d defaultCase) realCase() reflect.SelectCase {
	return reflect.SelectCase{Dir: reflect.SelectDefault}
}
func (d defaultCase) realDone(reflect.Value, bool) { d.fn() }
