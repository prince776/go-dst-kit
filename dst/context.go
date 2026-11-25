package dst

import (
	"context"
	"fmt"
	"time"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// WithCancel is context.WithCancel for a World.
//
// In a simulated World, derive contexts only with WithCancel, WithTimeout
// and WithDeadline (context.WithValue and context.WithCancel on top of
// them are fine; context.WithTimeout and context.AfterFunc are not: they
// use real timers or goroutines).
func WithCancel(w *World, parent context.Context) (context.Context, context.CancelFunc) {
	if w.s == nil {
		return context.WithCancel(parent)
	}
	c := newSimCtx(w, parent)
	return c, func() { c.cancel(context.Canceled, nil) }
}

// WithTimeout is context.WithTimeout for a World: the deadline follows the
// World's clock.
func WithTimeout(w *World, parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return WithDeadline(w, parent, w.Now().Add(d))
}

// WithDeadline is context.WithDeadline for a World.
func WithDeadline(w *World, parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if w.s == nil {
		return context.WithDeadline(parent, deadline)
	}
	c := newSimCtx(w, parent)
	cancel := func() { c.cancel(context.Canceled, nil) }
	if pd, ok := parent.Deadline(); ok && !pd.After(deadline) {
		return c, cancel // the parent's earlier deadline cancels us
	}
	c.deadline, c.hasDeadline = deadline, true
	if d := w.Until(deadline); d <= 0 {
		c.cancel(context.DeadlineExceeded, nil)
	} else if c.err == nil {
		c.timer = w.s.At(w.s.Now()+d, func() { c.cancel(context.DeadlineExceeded, nil) })
	}
	return c, cancel
}

// Done returns a channel that is closed when ctx is done, for waiting on a
// context in a World (Select with OnDone, or Done(ctx).Recv()).
func Done(ctx context.Context) Chan[struct{}] {
	if c, ok := ctx.(*simCtx); ok {
		return Chan[struct{}]{sim: c.sig}
	}
	d := ctx.Done()
	if d == nil {
		return Chan[struct{}]{} // never done
	}
	if c, ok := ctx.Value(simCtxKey{}).(*simCtx); ok && c.inner.Done() == d {
		return Chan[struct{}]{sim: c.sig}
	}
	if sched.CurrentTask() != nil {
		panic(fmt.Sprintf("dst: a simulated World can't wait on a %T; derive contexts with dst.WithCancel, WithTimeout or WithDeadline", ctx))
	}
	return RecvOnly(d)
}

type simCtxKey struct{}

// simCtx is a context whose cancellation goes through the simulator: no
// real timers, no goroutines.
//
// It is backed by a standard cancel context (inner): its Done channel is
// inner's, and Value exposes inner to the context package, so standard
// children (context.WithCancel, even below context.WithValue wrappers)
// register with inner and are cancelled synchronously when we cancel it,
// instead of being watched by a goroutine. One deviation: such a standard
// child of a context that hit its deadline reports Err() == Canceled
// (with Cause DeadlineExceeded), since only a real timer can make a
// standard context report DeadlineExceeded.
type simCtx struct {
	parent      context.Context
	w           *World
	deadline    time.Time
	hasDeadline bool
	inner       context.Context
	innerCancel context.CancelCauseFunc
	sig         *sched.Chan[struct{}] // closed on cancel, what the World waits on
	err, cause  error
	children    map[int]func()
	nextChild   int
	timer       sched.Timer
	unlink      func() bool
}

func newSimCtx(w *World, parent context.Context) *simCtx {
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	c := &simCtx{parent: parent, w: w, sig: sched.NewChan[struct{}](w.s, 0), children: map[int]func(){}}
	c.inner, c.innerCancel = context.WithCancelCause(context.Background())
	c.link()
	return c
}

// link arranges for the parent's cancellation to cancel c.
func (c *simCtx) link() {
	p := c.parent
	if p.Done() == nil {
		return
	}
	if err := p.Err(); err != nil {
		c.cancel(err, context.Cause(p))
		return
	}
	if sc, ok := p.Value(simCtxKey{}).(*simCtx); ok && sc.inner.Done() == p.Done() {
		c.unlink = sc.AfterFunc(func() { c.cancel(p.Err(), context.Cause(p)) })
		return
	}
	panic(fmt.Sprintf("dst: in a simulated World, a context can't be derived from a %T; derive contexts with dst.WithCancel, WithTimeout or WithDeadline", p))
}

func (c *simCtx) Deadline() (time.Time, bool) {
	if c.hasDeadline {
		return c.deadline, true
	}
	return c.parent.Deadline()
}

func (c *simCtx) Done() <-chan struct{} { return c.inner.Done() }
func (c *simCtx) Err() error            { return c.err }

func (c *simCtx) Value(key any) any {
	if key == (simCtxKey{}) {
		return c
	}
	if v := c.inner.Value(key); v != nil {
		return v // the context package's own lookup of its cancel context
	}
	return c.parent.Value(key)
}

func (c *simCtx) String() string { return fmt.Sprintf("dst.WithCancel(%v)", c.parent) }

// AfterFunc runs f (synchronously, in whoever cancels c) once c is done.
// The standard context package uses it to propagate cancellation to
// children without starting goroutines.
func (c *simCtx) AfterFunc(f func()) (stop func() bool) {
	if c.err != nil {
		f()
		return func() bool { return false }
	}
	c.nextChild++
	id := c.nextChild
	c.children[id] = f
	return func() bool {
		_, ok := c.children[id]
		delete(c.children, id)
		return ok
	}
}

func (c *simCtx) cancel(err, cause error) {
	if c.err != nil {
		return
	}
	if cause == nil {
		cause = err
	}
	c.err, c.cause = err, cause
	c.innerCancel(cause) // closes Done and cancels standard children, synchronously
	c.sig.CloseNow()
	c.timer.Stop()
	if c.unlink != nil {
		c.unlink()
	}
	for len(c.children) > 0 {
		// Cancel children in creation order, for determinism.
		min := -1
		for id := range c.children {
			if min < 0 || id < min {
				min = id
			}
		}
		f := c.children[min]
		delete(c.children, min)
		f()
	}
}
