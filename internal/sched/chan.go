package sched

import "fmt"

// Chan is a model of a Go channel whose waits the scheduler sees. It
// follows the Go runtime's semantics: values are FIFO, blocked senders and
// receivers are served in FIFO order, a receiver waiting on an unbuffered
// channel takes a value straight from a sender, and close wakes everyone.
//
// Verified:
//   - [chan-matches-go] For small programs, the set of outcomes the
//     scheduler can produce (across seeds) equals the set an independent
//     model of Go's channel and select semantics allows.
type Chan[T any] struct {
	s      *Scheduler
	id     int
	size   int
	buf    []T
	closed bool
	recvq  []*Waiter[T]
	sendq  []*Waiter[T]
}

// SelState is shared by every waiter of one select: the first to fire wins.
type SelState struct {
	Fired bool
	Index int
}

// Waiter is a task blocked on a channel, alone or as one arm of a select.
type Waiter[T any] struct {
	t           *Task
	sel         *SelState
	idx         int
	Val         T
	OK          bool
	ClosedPanic bool
}

func (w *Waiter[T]) alive() bool {
	return (w.sel == nil || !w.sel.Fired) && !w.t.killed
}

// NewChan makes a channel with the given buffer size.
func NewChan[T any](s *Scheduler, size int) *Chan[T] {
	return &Chan[T]{s: s, id: s.NextID(), size: size}
}

func (c *Chan[T]) String() string { return fmt.Sprintf("chan#%d", c.id) }

// Len and Cap are len(c) and cap(c).
func (c *Chan[T]) Len() int { return len(c.buf) }
func (c *Chan[T]) Cap() int { return c.size }

func (c *Chan[T]) hasAlive(q *[]*Waiter[T]) bool {
	for len(*q) > 0 && !(*q)[0].alive() {
		*q = (*q)[1:]
	}
	return len(*q) > 0
}

func (c *Chan[T]) popAlive(q *[]*Waiter[T]) *Waiter[T] {
	if !c.hasAlive(q) {
		return nil
	}
	w := (*q)[0]
	*q = (*q)[1:]
	return w
}

func (c *Chan[T]) fire(w *Waiter[T]) {
	if w.sel != nil {
		w.sel.Fired, w.sel.Index = true, w.idx
	}
	c.s.Ready(w.t)
}

// SendReady reports whether a send would complete (or panic) now.
func (c *Chan[T]) SendReady() bool {
	return c.closed || c.hasAlive(&c.recvq) || len(c.buf) < c.size
}

// TrySend sends without blocking and reports whether it did. It panics if
// the channel is closed. Usable from timer callbacks.
func (c *Chan[T]) TrySend(v T) bool {
	if c.closed {
		panic("send on closed channel")
	}
	if r := c.popAlive(&c.recvq); r != nil {
		r.Val, r.OK = v, true
		c.fire(r)
		return true
	}
	if len(c.buf) < c.size {
		c.buf = append(c.buf, v)
		return true
	}
	return false
}

// RecvReady reports whether a receive would complete now.
func (c *Chan[T]) RecvReady() bool {
	return len(c.buf) > 0 || c.hasAlive(&c.sendq) || c.closed
}

// TryRecv receives without blocking. done reports whether it did; ok is
// false for a closed, drained channel.
func (c *Chan[T]) TryRecv() (v T, ok, done bool) {
	if len(c.buf) > 0 {
		v = c.buf[0]
		var zero T
		c.buf[0] = zero
		c.buf = c.buf[1:]
		if len(c.buf) == 0 {
			c.buf = c.buf[:0:0]
		}
		if sw := c.popAlive(&c.sendq); sw != nil {
			c.buf = append(c.buf, sw.Val)
			c.fire(sw)
		}
		return v, true, true
	}
	if sw := c.popAlive(&c.sendq); sw != nil {
		c.fire(sw)
		return sw.Val, true, true
	}
	if c.closed {
		return v, false, true
	}
	return v, false, false
}

// Send is c <- v from the current task.
func (c *Chan[T]) Send(v T) {
	s := c.s
	s.Yield()
	if c.TrySend(v) {
		return
	}
	w := &Waiter[T]{t: s.Current(), Val: v}
	c.sendq = append(c.sendq, w)
	s.Park(fmt.Sprintf("send on %s (buffer %d/%d)", c, len(c.buf), c.size))
	if w.ClosedPanic {
		panic("send on closed channel")
	}
}

// Recv is <-c from the current task.
func (c *Chan[T]) Recv() (T, bool) {
	s := c.s
	s.Yield()
	if v, ok, done := c.TryRecv(); done {
		return v, ok
	}
	w := &Waiter[T]{t: s.Current()}
	c.recvq = append(c.recvq, w)
	s.Park(fmt.Sprintf("receive from %s (empty)", c))
	return w.Val, w.OK
}

// Close is close(c) from the current task.
func (c *Chan[T]) Close() {
	c.s.Yield()
	c.CloseNow()
}

// CloseNow closes without yielding. Usable from timer callbacks.
func (c *Chan[T]) CloseNow() {
	if c.closed {
		panic("close of closed channel")
	}
	c.closed = true
	for w := c.popAlive(&c.recvq); w != nil; w = c.popAlive(&c.recvq) {
		var zero T
		w.Val, w.OK = zero, false
		c.fire(w)
	}
	for w := c.popAlive(&c.sendq); w != nil; w = c.popAlive(&c.sendq) {
		w.ClosedPanic = true
		c.fire(w)
	}
}

// EnqueueRecv registers the current task as one receiving arm of a select.
func (c *Chan[T]) EnqueueRecv(sel *SelState, idx int) *Waiter[T] {
	w := &Waiter[T]{t: c.s.Current(), sel: sel, idx: idx}
	c.recvq = append(c.recvq, w)
	return w
}

// EnqueueSend registers the current task as one sending arm of a select.
func (c *Chan[T]) EnqueueSend(sel *SelState, idx int, v T) *Waiter[T] {
	w := &Waiter[T]{t: c.s.Current(), sel: sel, idx: idx, Val: v}
	c.sendq = append(c.sendq, w)
	return w
}

// Sched returns the channel's scheduler.
func (c *Chan[T]) Sched() *Scheduler { return c.s }
