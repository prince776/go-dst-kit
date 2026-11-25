package dst

import (
	"iter"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Chan is a channel of T for a World: a plain Go channel in the real
// World, a model of one in the simulated World. The zero Chan is a nil
// channel: sends and receives block forever, and it's never ready in a
// Select.
type Chan[T any] struct {
	rw  chan T   // real, bidirectional
	ro  <-chan T // real, receive side
	sim *sched.Chan[T]
}

// MakeChan is make(chan T, size).
func MakeChan[T any](w *World, size int) Chan[T] {
	if w.s != nil {
		return Chan[T]{sim: sched.NewChan[T](w.s, size)}
	}
	c := make(chan T, size)
	return Chan[T]{rw: c, ro: c}
}

// RecvOnly adapts a real receive-only channel (from a library) for the
// real World. A simulated World can't wait on it.
func RecvOnly[T any](c <-chan T) Chan[T] { return Chan[T]{ro: c} }

func (c Chan[T]) isNil() bool { return c.sim == nil && c.ro == nil }

// Send is c <- v.
func (c Chan[T]) Send(v T) {
	switch {
	case c.sim != nil:
		c.sim.Send(v)
	case c.rw != nil:
		c.rw <- v
	case c.ro != nil:
		panic("dst: send on a receive-only channel")
	default:
		blockForever("send on nil channel")
	}
}

// Recv is v, ok := <-c.
func (c Chan[T]) Recv() (T, bool) {
	switch {
	case c.sim != nil:
		return c.sim.Recv()
	case c.ro != nil:
		v, ok := <-c.ro
		return v, ok
	default:
		blockForever("receive from nil channel")
		panic("unreachable")
	}
}

// Close is close(c).
func (c Chan[T]) Close() {
	switch {
	case c.sim != nil:
		c.sim.Close()
	case c.rw != nil:
		close(c.rw)
	case c.ro != nil:
		panic("dst: close of a receive-only channel")
	default:
		panic("close of nil channel")
	}
}

// TrySend sends if it can without waiting, like a select with a default
// case, and reports whether it did.
func (c Chan[T]) TrySend(v T) bool {
	switch {
	case c.sim != nil:
		c.sim.Sched().Yield()
		return c.sim.TrySend(v)
	case c.rw != nil:
		select {
		case c.rw <- v:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// TryRecv receives if it can without waiting. received reports whether it
// did; ok is false if the channel is closed and drained.
func (c Chan[T]) TryRecv() (v T, ok, received bool) {
	switch {
	case c.sim != nil:
		c.sim.Sched().Yield()
		return c.sim.TryRecv()
	case c.ro != nil:
		select {
		case v, ok = <-c.ro:
			return v, ok, true
		default:
			return v, false, false
		}
	default:
		return v, false, false
	}
}

// All receives until the channel is closed: `for v := range c.All()` is
// `for v := range c`.
func (c Chan[T]) All() iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			v, ok := c.Recv()
			if !ok || !yield(v) {
				return
			}
		}
	}
}

// Len is len(c).
func (c Chan[T]) Len() int {
	switch {
	case c.sim != nil:
		return c.sim.Len()
	case c.ro != nil:
		return len(c.ro)
	}
	return 0
}

// Cap is cap(c).
func (c Chan[T]) Cap() int {
	switch {
	case c.sim != nil:
		return c.sim.Cap()
	case c.ro != nil:
		return cap(c.ro)
	}
	return 0
}

// blockForever is a nil-channel operation: it parks a simulated goroutine
// (so a deadlock is reported) and blocks a real one.
func blockForever(why string) {
	if t := sched.CurrentTask(); t != nil {
		t.Sched().Park(why)
		panic("unreachable")
	}
	select {}
}
