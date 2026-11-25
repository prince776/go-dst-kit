// Package worldtest is the contract suite every World must pass: the same
// checks run against the real World and the simulated one. A check fails
// by panicking with a Failure (in the goroutine that runs the check, never
// in one it starts), so it works the same in both Worlds.
package worldtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Failure is a failed check.
type Failure string

func failf(format string, args ...any) { panic(Failure(fmt.Sprintf(format, args...))) }

// Check is one contract check.
type Check struct {
	Name string
	Fn   func(w *dst.World)
}

// Checks is the suite.
var Checks = []Check{
	{"chan/buffered-is-fifo", bufferedFIFO},
	{"chan/unbuffered-hands-over", unbufferedHandsOver},
	{"chan/close", closeSemantics},
	{"chan/try", tryOps},
	{"select/picks-a-ready-case", selectReady},
	{"select/default-only-when-none-ready", selectDefault},
	{"select/waits-for-first-ready", selectWaits},
	{"select/send-case", selectSend},
	{"select/nil-channel-never-ready", selectNil},
	{"time/sleep-advances-clock", sleepAdvances},
	{"time/timers-fire-in-deadline-order", timersInOrder},
	{"time/timer-stop-and-reset", timerStopReset},
	{"time/after-func-runs-once", afterFunc},
	{"time/ticker-ticks-until-stopped", ticker},
	{"sync/mutex-excludes", mutexExcludes},
	{"sync/rwmutex", rwMutex},
	{"sync/waitgroup-waits", waitGroup},
	{"sync/once-runs-once", once},
	{"context/cancel-propagates", contextCancel},
	{"context/timeout", contextTimeout},
	{"context/std-children-work", contextStdChildren},
	{"rand/in-range", randInRange},
}

const tick = 5 * time.Millisecond

func bufferedFIFO(w *dst.World) {
	c := dst.MakeChan[int](w, 3)
	for i := range 3 {
		c.Send(i)
	}
	if c.Len() != 3 || c.Cap() != 3 {
		failf("len %d cap %d, want 3 3", c.Len(), c.Cap())
	}
	for i := range 3 {
		if v, ok := c.Recv(); v != i || !ok {
			failf("recv %d %v, want %d true", v, ok, i)
		}
	}
}

func unbufferedHandsOver(w *dst.World) {
	c := dst.MakeChan[int](w, 0)
	w.Go(func() {
		for i := range 5 {
			c.Send(i)
		}
		c.Close()
	})
	var got []int
	for v := range c.All() {
		got = append(got, v)
	}
	if !slices.Equal(got, []int{0, 1, 2, 3, 4}) {
		failf("received %v", got)
	}
}

func panics(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

func closeSemantics(w *dst.World) {
	c := dst.MakeChan[int](w, 1)
	c.Send(7)
	c.Close()
	if v, ok := c.Recv(); v != 7 || !ok {
		failf("buffered value after close: %d %v", v, ok)
	}
	if v, ok := c.Recv(); v != 0 || ok {
		failf("recv from closed, drained: %d %v", v, ok)
	}
	if m := panics(func() { c.Send(1) }); m != "send on closed channel" {
		failf("send on closed: %q", m)
	}
	if m := panics(c.Close); m != "close of closed channel" {
		failf("close of closed: %q", m)
	}
	// A blocked receiver is woken by close.
	d := dst.MakeChan[int](w, 0)
	got := dst.MakeChan[bool](w, 1)
	w.Go(func() {
		_, ok := d.Recv()
		got.Send(ok)
	})
	w.Sleep(tick)
	d.Close()
	if ok, _ := got.Recv(); ok {
		failf("receiver woken by close got ok=true")
	}
}

func tryOps(w *dst.World) {
	c := dst.MakeChan[int](w, 1)
	if _, _, got := c.TryRecv(); got {
		failf("TryRecv on empty channel received")
	}
	if !c.TrySend(1) || c.TrySend(2) {
		failf("TrySend: want true then false on a 1-slot channel")
	}
	if v, ok, got := c.TryRecv(); !got || !ok || v != 1 {
		failf("TryRecv = %d %v %v", v, ok, got)
	}
}

func selectReady(w *dst.World) {
	a, b := dst.MakeChan[int](w, 1), dst.MakeChan[int](w, 1)
	seen := map[string]bool{}
	for range 200 {
		a.TrySend(1)
		b.TrySend(2)
		dst.Select(w,
			dst.OnRecv(a, func(int, bool) { seen["a"] = true }),
			dst.OnRecv(b, func(int, bool) { seen["b"] = true }),
			dst.Default(func() { failf("default ran with ready cases") }),
		)
	}
	if !seen["a"] || !seen["b"] {
		failf("with both ready, 200 selects chose only %v", seen)
	}
}

func selectDefault(w *dst.World) {
	a := dst.MakeChan[int](w, 0)
	ran := ""
	dst.Select(w,
		dst.OnRecv(a, func(int, bool) { ran = "recv" }),
		dst.Default(func() { ran = "default" }),
	)
	if ran != "default" {
		failf("ran %q, want default", ran)
	}
}

func selectWaits(w *dst.World) {
	a, b := dst.MakeChan[int](w, 0), dst.MakeChan[int](w, 0)
	w.Go(func() {
		w.Sleep(2 * tick)
		b.Send(42)
	})
	got := 0
	dst.Select(w,
		dst.OnRecv(a, func(int, bool) { failf("a was never sent") }),
		dst.OnRecv(b, func(v int, _ bool) { got = v }),
	)
	if got != 42 {
		failf("got %d", got)
	}
}

func selectSend(w *dst.World) {
	a := dst.MakeChan[int](w, 0)
	done := dst.MakeChan[int](w, 1)
	w.Go(func() {
		v, _ := a.Recv()
		done.Send(v)
	})
	sent := false
	dst.Select(w, dst.OnSend(a, 9, func() { sent = true }))
	if v, _ := done.Recv(); !sent || v != 9 {
		failf("sent %v, received %d", sent, v)
	}
}

func selectNil(w *dst.World) {
	var nilc dst.Chan[int]
	a := dst.MakeChan[int](w, 1)
	a.Send(1)
	for range 50 {
		ran := ""
		dst.Select(w,
			dst.OnRecv(nilc, func(int, bool) { ran = "nil" }),
			dst.OnSend(nilc, 1, func() { ran = "nil" }),
			dst.OnRecv(a, func(int, bool) { ran = "a" }),
		)
		if ran != "a" {
			failf("ran %q", ran)
		}
		a.Send(1)
	}
}

func sleepAdvances(w *dst.World) {
	t0 := w.Now()
	w.Sleep(2 * tick)
	if d := w.Since(t0); d < 2*tick {
		failf("slept %v, want >= %v", d, 2*tick)
	}
}

func timersInOrder(w *dst.World) {
	t3, t1, t2 := w.NewTimer(6*tick), w.NewTimer(2*tick), w.NewTimer(4*tick)
	var order []int
	for len(order) < 3 {
		dst.Select(w,
			dst.OnRecv(t1.C, func(time.Time, bool) { order = append(order, 1) }),
			dst.OnRecv(t2.C, func(time.Time, bool) { order = append(order, 2) }),
			dst.OnRecv(t3.C, func(time.Time, bool) { order = append(order, 3) }),
		)
	}
	if !slices.Equal(order, []int{1, 2, 3}) {
		failf("fired in order %v", order)
	}
}

func timerStopReset(w *dst.World) {
	t := w.NewTimer(2 * tick)
	if !t.Stop() {
		failf("Stop of a pending timer returned false")
	}
	fired := false
	dst.Select(w,
		dst.OnRecv(t.C, func(time.Time, bool) { fired = true }),
		dst.OnRecv(w.After(4*tick), func(time.Time, bool) {}),
	)
	if fired {
		failf("a stopped timer fired")
	}
	t.Reset(tick)
	if _, ok := t.C.Recv(); !ok {
		failf("reset timer's channel closed")
	}
}

func afterFunc(w *dst.World) {
	ran := dst.MakeChan[int](w, 2)
	w.AfterFunc(tick, func() { ran.Send(1) })
	stopped := w.AfterFunc(tick, func() { ran.Send(2) })
	if !stopped.Stop() {
		failf("Stop of a pending AfterFunc returned false")
	}
	if v, _ := ran.Recv(); v != 1 {
		failf("got %d", v)
	}
	w.Sleep(3 * tick)
	if ran.Len() != 0 {
		failf("a stopped AfterFunc ran")
	}
}

func ticker(w *dst.World) {
	tk := w.NewTicker(tick)
	for range 3 {
		tk.C.Recv()
	}
	tk.Stop()
	extra := false
	dst.Select(w,
		dst.OnRecv(tk.C, func(time.Time, bool) { extra = true }),
		dst.OnRecv(w.After(4*tick), func(time.Time, bool) {}),
	)
	if extra {
		failf("a stopped ticker ticked")
	}
}

func mutexExcludes(w *dst.World) {
	var mu dst.Mutex
	var wg dst.WaitGroup
	inside, maxInside, count := 0, 0, 0
	for range 4 {
		wg.Go(w, func() {
			for range 5 {
				mu.Lock()
				inside++
				maxInside = max(maxInside, inside)
				w.Sleep(0) // a scheduling point while holding the lock
				count++
				inside--
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if maxInside != 1 || count != 20 {
		failf("max %d inside at once, count %d", maxInside, count)
	}
}

func rwMutex(w *dst.World) {
	var mu dst.RWMutex
	var wg dst.WaitGroup
	var readers, writers, maxReaders atomic.Int32
	var bad atomic.Bool
	for i := range 6 {
		wg.Go(w, func() {
			for range 3 {
				if i%3 == 0 {
					mu.Lock()
					if writers.Add(1) > 1 || readers.Load() > 0 {
						bad.Store(true)
					}
					w.Sleep(0)
					writers.Add(-1)
					mu.Unlock()
				} else {
					mu.RLock()
					n := readers.Add(1)
					for m := maxReaders.Load(); n > m && !maxReaders.CompareAndSwap(m, n); m = maxReaders.Load() {
					}
					if writers.Load() > 0 {
						bad.Store(true)
					}
					w.Sleep(tick)
					readers.Add(-1)
					mu.RUnlock()
				}
			}
		})
	}
	wg.Wait()
	if bad.Load() {
		failf("a writer overlapped another writer or a reader")
	}
	if maxReaders.Load() < 2 {
		failf("readers never overlapped (max %d)", maxReaders.Load())
	}
}

func waitGroup(w *dst.World) {
	var wg dst.WaitGroup
	var done atomic.Int32
	for i := range 5 {
		wg.Add(1)
		w.Go(func() {
			defer wg.Done()
			w.Sleep(time.Duration(i) * tick / 2)
			done.Add(1)
		})
	}
	wg.Wait()
	if done.Load() != 5 {
		failf("Wait returned with %d of 5 done", done.Load())
	}
}

func once(w *dst.World) {
	var o dst.Once
	var wg dst.WaitGroup
	var runs atomic.Int32
	var early atomic.Bool
	for range 4 {
		wg.Go(w, func() {
			o.Do(func() {
				w.Sleep(tick) // others arrive while it runs
				runs.Add(1)
			})
			if runs.Load() != 1 {
				early.Store(true)
			}
		})
	}
	wg.Wait()
	if runs.Load() != 1 || early.Load() {
		failf("f ran %d times; a Do returned before f finished: %v", runs.Load(), early.Load())
	}
}

func contextCancel(w *dst.World) {
	parent, cancel := dst.WithCancel(w, context.Background())
	child, cancelChild := dst.WithCancel(w, parent)
	defer cancelChild()
	got := dst.MakeChan[error](w, 1)
	w.Go(func() {
		dst.Done(child).Recv()
		got.Send(child.Err())
	})
	w.Sleep(tick)
	if child.Err() != nil {
		failf("child done before cancel")
	}
	cancel()
	if err, _ := got.Recv(); !errors.Is(err, context.Canceled) {
		failf("child's Err = %v", err)
	}
	select {
	case <-parent.Done():
	default:
		failf("parent's Done channel not closed")
	}
}

func contextTimeout(w *dst.World) {
	ctx, cancel := dst.WithTimeout(w, context.Background(), 2*tick)
	defer cancel()
	if dl, ok := ctx.Deadline(); !ok || dl.Before(w.Now()) {
		failf("deadline %v %v", dl, ok)
	}
	start := w.Now()
	which := ""
	dst.Select(w,
		dst.OnDone(ctx, func() { which = "done" }),
		dst.OnRecv(w.After(20*tick), func(time.Time, bool) { which = "late" }),
	)
	if which != "done" || !errors.Is(ctx.Err(), context.DeadlineExceeded) || w.Since(start) < 2*tick {
		failf("%s after %v, err %v", which, w.Since(start), ctx.Err())
	}
}

type key struct{}

func contextStdChildren(w *dst.World) {
	parent, cancel := dst.WithCancel(w, context.Background())
	valued := context.WithValue(parent, key{}, "v")
	std, stdCancel := context.WithCancel(valued) // propagates via AfterFunc, no goroutine
	defer stdCancel()
	sub, subCancel := dst.WithTimeout(w, valued, time.Hour)
	defer subCancel()
	cancel()
	if std.Err() == nil || sub.Err() == nil || sub.Value(key{}) != "v" {
		failf("after cancel: std %v, sub %v, value %v", std.Err(), sub.Err(), sub.Value(key{}))
	}
	dst.Done(valued).Recv() // a value wrapper can be waited on
}

func randInRange(w *dst.World) {
	r := w.Rand()
	for range 100 {
		if v := r.IntN(10); v < 0 || v >= 10 {
			failf("IntN(10) = %d", v)
		}
	}
	b := make([]byte, 16)
	if n, err := r.Read(b); n != 16 || err != nil || slices.Equal(b, make([]byte, 16)) {
		failf("Read: %d %v %x", n, err, b)
	}
}
