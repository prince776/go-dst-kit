// Package worldtest is the contract suite every World must pass: the same
// checks run against the real World and the simulated one. A check fails
// by panicking with a Failure (in the goroutine that runs the check, never
// in one it starts), so it works the same in both Worlds.
package worldtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	mrand "math/rand"
	mrandv2 "math/rand/v2"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
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
	{"select2/every-direction-and-position", select2Exhaustive},
	{"select3/every-direction-and-position", select3Exhaustive},
	{"select2/picks-a-ready-case", select2Ready},
	{"select2/waits-for-first-ready", select2Waits},
	{"select2/closed-done-and-nil", select2ClosedDoneNil},
	{"time/sleep-advances-clock", sleepAdvances},
	{"time/timers-fire-in-deadline-order", timersInOrder},
	{"time/timer-stop-and-reset", timerStopReset},
	{"time/after-func-runs-once", afterFunc},
	{"time/ticker-ticks-until-stopped", ticker},
	{"sync/mutex-excludes", mutexExcludes},
	{"sync/rwmutex", rwMutex},
	{"sync/rwmutex-try-and-rlocker", rwMutexTry},
	{"sync/waitgroup-waits", waitGroup},
	{"sync/once-runs-once", once},
	{"sync/cond-signal-wakes-one-broadcast-all", condSignalBroadcast},
	{"sync/cond-wait-reacquires-lock", condReacquires},
	{"sync/cond-signal-without-waiter-is-lost", condLostSignal},
	{"context/cancel-propagates", contextCancel},
	{"context/timeout", contextTimeout},
	{"context/std-children-work", contextStdChildren},
	{"context/cause", contextCause},
	{"context/after-done-own-goroutine", afterDoneOwnGoroutine},
	{"rand/in-range", randInRange},
	{"rand/v2-and-n", randV2},
	{"rand/math-rand-v1", mathRandV1},
	{"rand/crypto-text", cryptoText},
	{"time/tick", tickChan},
	{"sync/once-func-value-values", onceFuncs},
	{"io/pipe", ioPipe},
	{"net/pipe", netPipe},
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

// rwMutexTry: TryLock and TryRLock succeed and fail as sync.RWMutex's do
// (held by a reader, by a writer, and with a writer waiting behind a
// reader), and RLocker locks the read side.
func rwMutexTry(w *dst.World) {
	var mu dst.RWMutex
	if !mu.TryRLock() {
		failf("TryRLock on a free lock failed")
	}
	if !mu.TryRLock() {
		failf("a second TryRLock beside a reader failed")
	}
	if mu.TryLock() {
		failf("TryLock succeeded while readers hold the lock")
	}
	mu.RUnlock()
	mu.RUnlock()
	if !mu.TryLock() {
		failf("TryLock on a free lock failed")
	}
	if mu.TryLock() || mu.TryRLock() {
		failf("TryLock or TryRLock succeeded while a writer holds the lock")
	}
	mu.Unlock()
	// A writer waits behind a reader: new readers and writers are refused.
	mu.RLock()
	got := dst.MakeChan[bool](w, 1)
	release := dst.MakeChan[bool](w, 0)
	w.Go(func() {
		mu.Lock()
		got.Send(true)
		release.Recv() // hold it until the checks below are done
		mu.Unlock()
	})
	// Until the writer waits, TryRLock succeeds; once it waits, it fails.
	for i := 0; mu.TryRLock(); i++ {
		mu.RUnlock()
		if i == 1000 {
			failf("TryRLock kept succeeding with a writer waiting for %v", 1000*tick)
		}
		w.Sleep(tick)
	}
	if mu.TryLock() {
		failf("TryLock succeeded while a writer waits")
	}
	mu.RUnlock()
	// The last reader left: the lock is the waiting writer's (it may hold
	// it already, until released), not ours.
	if mu.TryLock() {
		failf("TryLock succeeded after the last reader left, ahead of the waiting writer")
	}
	got.Recv()
	release.Send(true)
	// RLocker is the read side: two holders at once, and a writer refused.
	l := mu.RLocker()
	l.Lock()
	if !mu.TryRLock() {
		failf("TryRLock failed beside RLocker's lock: it isn't the read side")
	}
	if mu.TryLock() {
		failf("TryLock succeeded while RLocker holds the read side")
	}
	mu.RUnlock()
	l.Unlock()
	if !mu.TryLock() {
		failf("TryLock failed after RLocker's unlocks")
	}
	mu.Unlock()
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
	start := w.Now() // before the context: its deadline counts from creation
	ctx, cancel := dst.WithTimeout(w, context.Background(), 2*tick)
	defer cancel()
	if dl, ok := ctx.Deadline(); !ok || dl.Before(w.Now()) {
		failf("deadline %v %v", dl, ok)
	}
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

var errCause = errors.New("the cause")

// contextCause: the Cause variants record their cause for context.Cause
// (Canceled for a nil one, the deadline's error when a deadline cancels
// a WithCancelCause), children inherit it, and every one of them can be
// waited on.
func contextCause(w *dst.World) {
	ctx, cancel := dst.WithCancelCause(w, context.Background())
	child, childCancel := dst.WithCancel(w, ctx)
	defer childCancel()
	if context.Cause(ctx) != nil {
		failf("Cause before cancel: %v", context.Cause(ctx))
	}
	cancel(errCause)
	cancel(errors.New("a second cause")) // ignored, as in Go
	dst.Done(ctx).Recv()
	dst.Done(child).Recv()
	if ctx.Err() != context.Canceled || context.Cause(ctx) != errCause || context.Cause(child) != errCause {
		failf("WithCancelCause: Err %v, Cause %v, the child's Cause %v", ctx.Err(), context.Cause(ctx), context.Cause(child))
	}
	nilCtx, nilCancel := dst.WithCancelCause(w, context.Background())
	nilCancel(nil)
	if context.Cause(nilCtx) != context.Canceled {
		failf("a nil cause: Cause %v, want Canceled", context.Cause(nilCtx))
	}
	tctx, tcancel := dst.WithTimeoutCause(w, context.Background(), tick, errCause)
	defer tcancel()
	dst.Done(tctx).Recv()
	if tctx.Err() != context.DeadlineExceeded || context.Cause(tctx) != errCause {
		failf("WithTimeoutCause: Err %v, Cause %v", tctx.Err(), context.Cause(tctx))
	}
	dctx, dcancel := dst.WithDeadlineCause(w, context.Background(), w.Now().Add(-time.Second), errCause)
	defer dcancel()
	if dctx.Err() != context.DeadlineExceeded || context.Cause(dctx) != errCause {
		failf("WithDeadlineCause in the past: Err %v, Cause %v", dctx.Err(), context.Cause(dctx))
	}
	// Cancelled before the deadline: Canceled, and the cancel's cause.
	ectx, ecancel := dst.WithTimeoutCause(w, context.Background(), time.Hour, errCause)
	ecancel()
	if ectx.Err() != context.Canceled || context.Cause(ectx) != context.Canceled {
		failf("WithTimeoutCause cancelled early: Err %v, Cause %v", ectx.Err(), context.Cause(ectx))
	}
}

// afterDoneOwnGoroutine: AfterDone's f runs in a goroutine of its own,
// as context.AfterFunc's does, so it may wait for a lock the canceller
// holds (on a context cancelled later, and on one already done).
func afterDoneOwnGoroutine(w *dst.World) {
	var mu dst.Mutex
	ran := dst.MakeChan[string](w, 2)
	ctx, cancel := dst.WithCancel(w, context.Background())
	dst.AfterDone(ctx, func() { mu.Lock(); ran.Send("later"); mu.Unlock() })
	mu.Lock()
	cancel() // f can't run inside this: it would wait for mu, held here
	dst.AfterDone(ctx, func() { mu.Lock(); ran.Send("already done"); mu.Unlock() })
	mu.Unlock()
	got := map[string]bool{}
	for range 2 {
		v, _ := ran.Recv()
		got[v] = true
	}
	if !got["later"] || !got["already done"] {
		failf("AfterDone's functions: %v", got)
	}
	stopped, stop := dst.WithCancel(w, context.Background())
	if !dst.AfterDone(stopped, func() { failf("a stopped AfterDone ran") })() {
		failf("stop before the cancel reported false")
	}
	stop()
}

// randV2: every math/rand/v2 function stays in its range (unsigned ones
// beyond MaxInt64 too, through dst.N), and bad arguments panic as Go's.
func randV2(w *dst.World) {
	r := w.Rand()
	const big = uint64(1)<<63 + 5
	for range 200 {
		if v := r.Int32N(7); v < 0 || v >= 7 {
			failf("Int32N(7) = %d", v)
		}
		if v := r.Uint32N(7); v >= 7 {
			failf("Uint32N(7) = %d", v)
		}
		if v := r.Uint64N(big); v >= big {
			failf("Uint64N(%d) = %d", big, v)
		}
		if v := r.UintN(7); v >= 7 {
			failf("UintN(7) = %d", v)
		}
		if v := r.Float32(); v < 0 || v >= 1 {
			failf("Float32() = %v", v)
		}
		if v := r.ExpFloat64(); v < 0 {
			failf("ExpFloat64() = %v", v)
		}
		if v := r.NormFloat64(); v != v {
			failf("NormFloat64() = NaN")
		}
		if v := r.Int32(); v < 0 {
			failf("Int32() = %d", v)
		}
		_ = r.Uint()
		if v := dst.N(r, 3*time.Second); v < 0 || v >= 3*time.Second {
			failf("N(3s) = %v", v)
		}
		if v := dst.N(r, big); v >= big {
			failf("N(%d) = %d", big, v)
		}
	}
	want := panics(func() { mrandv2.N(0) })
	if m := panics(func() { dst.N(r, 0) }); m != want || m == "" {
		failf("N(0) panicked with %q, Go's with %q", m, want)
	}
}

// mathRandV1: math/rand's (v1) functions stay in range, and bad
// arguments panic with v1's messages.
func mathRandV1(w *dst.World) {
	r := w.MathRand()
	for range 200 {
		if v := r.Intn(7); v < 0 || v >= 7 {
			failf("Intn(7) = %d", v)
		}
		if v := r.Int31n(7); v < 0 || v >= 7 {
			failf("Int31n(7) = %d", v)
		}
		if v := r.Int63n(7); v < 0 || v >= 7 {
			failf("Int63n(7) = %d", v)
		}
		if r.Int() < 0 || r.Int31() < 0 || r.Int63() < 0 {
			failf("a negative Int, Int31 or Int63")
		}
		if v := r.Float64(); v < 0 || v >= 1 {
			failf("Float64() = %v", v)
		}
		if v := r.Float32(); v < 0 || v >= 1 {
			failf("Float32() = %v", v)
		}
		if v := r.ExpFloat64(); v < 0 {
			failf("ExpFloat64() = %v", v)
		}
	}
	if p := r.Perm(5); len(p) != 5 {
		failf("Perm(5) = %v", p)
	}
	b := make([]byte, 9)
	if n, err := r.Read(b); n != 9 || err != nil {
		failf("Read: %d %v", n, err)
	}
	for _, c := range []struct {
		name string
		v1   func()
		mine func()
	}{
		{"Intn", func() { mrand.Intn(0) }, func() { r.Intn(0) }},
		{"Int31n", func() { mrand.Int31n(0) }, func() { r.Int31n(0) }},
		{"Int63n", func() { mrand.Int63n(0) }, func() { r.Int63n(0) }},
	} {
		if want, got := panics(c.v1), panics(c.mine); want != got || want == "" {
			failf("%s(0) panicked with %q, math/rand's with %q", c.name, got, want)
		}
	}
}

// cryptoText: CryptoText is crypto/rand.Text's shape, and two differ.
func cryptoText(w *dst.World) {
	a, b := dst.CryptoText(w), dst.CryptoText(w)
	if len(a) != 26 || strings.Trim(a, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") != "" || a == b {
		failf("CryptoText: %q, %q", a, b)
	}
}

// tickChan: Tick's channel ticks; for d <= 0 it is nil (never ready).
func tickChan(w *dst.World) {
	start := w.Now()
	c := w.Tick(tick)
	for i := 1; i <= 2; i++ {
		if v, _ := c.Recv(); v.Sub(start) < time.Duration(i)*tick {
			failf("tick %d at %v", i, v.Sub(start))
		}
	}
	none := w.Tick(0)
	fired := false
	dst.Select(w, dst.OnRecv(none, func(time.Time, bool) { fired = true }), dst.Default(nil))
	if fired || none.Cap() != 0 {
		failf("Tick(0) is not a nil channel")
	}
}

// onceFuncs: OnceFunc, OnceValue and OnceValues run f once for callers
// that come together (they wait for it), and repeat its panic on every
// call, as sync's do.
func onceFuncs(w *dst.World) {
	var runs atomic.Int32
	f := dst.OnceFunc(func() { runs.Add(1); w.Sleep(tick) })
	v := dst.OnceValue(func() int { runs.Add(1); w.Sleep(tick); return 7 })
	vs := dst.OnceValues(func() (int, error) { runs.Add(1); w.Sleep(tick); return 8, io.EOF })
	var wg dst.WaitGroup
	for range 3 {
		wg.Go(w, func() {
			f()
			if v() != 7 {
				panic("OnceValue's value")
			}
			if a, err := vs(); a != 8 || err != io.EOF {
				panic("OnceValues' values")
			}
		})
	}
	wg.Wait()
	if runs.Load() != 3 {
		failf("the functions ran %d times, want 3 (once each)", runs.Load())
	}
	bad := dst.OnceFunc(func() { panic("f failed") })
	std := sync.OnceFunc(func() { panic("f failed") })
	for range 2 {
		if got, want := panics(bad), panics(std); got != want || want == "" {
			failf("a panicking OnceFunc: %q, sync's %q", got, want)
		}
	}
}

// ioPipe: a Pipe carries a write to reads in pieces (a write waits for
// all of it to be read), and closes end it with io's errors: EOF or the
// writer's error for a reader, ErrClosedPipe or the reader's error for a
// writer.
func ioPipe(w *dst.World) {
	r, wr := dst.Pipe(w)
	w.Go(func() {
		wr.Write([]byte("hello world"))
		wr.CloseWithError(io.ErrUnexpectedEOF)
	})
	var got []byte
	buf := make([]byte, 4)
	for {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if err != io.ErrUnexpectedEOF {
				failf("Read after the writer's close: %v, want its error", err)
			}
			break
		}
	}
	if string(got) != "hello world" {
		failf("read %q", got)
	}
	r2, wr2 := dst.Pipe(w)
	wr2.Close()
	if _, err := r2.Read(buf); err != io.EOF {
		failf("Read after Close: %v, want EOF", err)
	}
	r3, wr3 := dst.Pipe(w)
	r3.CloseWithError(errCause)
	if _, err := wr3.Write([]byte("x")); err != errCause {
		failf("Write after the reader's close: %v, want its error", err)
	}
	r4, wr4 := dst.Pipe(w)
	r4.Close()
	if _, err := wr4.Write([]byte("x")); err != io.ErrClosedPipe {
		failf("Write after the reader's Close: %v, want ErrClosedPipe", err)
	}
	// A write blocked on no reader ends when the reader closes.
	r5, wr5 := dst.Pipe(w)
	res := dst.MakeChan[error](w, 1)
	w.Go(func() {
		_, err := wr5.Write([]byte("never read"))
		res.Send(err)
	})
	w.Sleep(tick)
	r5.Close()
	if err, _ := res.Recv(); err != io.ErrClosedPipe {
		failf("a blocked Write after the reader's Close: %v", err)
	}
}

// netPipe: NetPipe's ends carry data both ways, EOF after the other
// end closes, ErrClosedPipe on a closed end, and deadlines that follow
// the World's clock, wrapped in a net.OpError timing out as os's
// ErrDeadlineExceeded, then lifted by a zero deadline.
func netPipe(w *dst.World) {
	a, b := dst.NetPipe(w)
	w.Go(func() {
		a.Write([]byte("ping"))
		buf := make([]byte, 4)
		io.ReadFull(a, buf)
		a.Close()
	})
	buf := make([]byte, 4)
	if _, err := io.ReadFull(b, buf); err != nil || string(buf) != "ping" {
		failf("read %q %v", buf, err)
	}
	b.Write([]byte("pong"))
	if _, err := b.Read(buf); err != io.EOF {
		failf("Read after the other end closed: %v, want EOF", err)
	}
	b.Close()
	if _, err := b.Write(buf); err != io.ErrClosedPipe {
		failf("Write on a closed end: %v, want ErrClosedPipe", err)
	}
	c, d := dst.NetPipe(w)
	defer d.Close()
	start := w.Now()
	c.SetReadDeadline(w.Now().Add(3 * tick))
	_, err := c.Read(buf)
	var ne net.Error
	if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &ne) || !ne.Timeout() || w.Since(start) < 3*tick {
		failf("a read deadline: %v after %v", err, w.Since(start))
	}
	c.SetReadDeadline(time.Time{}) // lifted: a read waits for data again
	w.Go(func() { d.Write([]byte("late")) })
	if n, err := c.Read(buf); err != nil || string(buf[:n]) != "late" {
		failf("a read after the deadline was lifted: %q %v", buf[:n], err)
	}
	c.SetWriteDeadline(w.Now().Add(-time.Second))
	if _, err := c.Write(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		failf("a write past its deadline: %v", err)
	}
	if c.LocalAddr().Network() != "pipe" || c.RemoteAddr().String() != "pipe" {
		failf("addresses %v %v", c.LocalAddr(), c.RemoteAddr())
	}
}

func randInRange(w *dst.World) {
	r := w.Rand()
	for range 100 {
		if v := r.IntN(10); v < 0 || v >= 10 {
			failf("IntN(10) = %d", v)
		}
	}
	b := make([]byte, 16)
	if n, err := r.CryptoRead(b); n != 16 || err != nil || slices.Equal(b, make([]byte, 16)) {
		failf("CryptoRead: %d %v %x", n, err, b)
	}
	c := make([]byte, 16)
	if _, err := io.ReadFull(dst.CryptoReader(w), c); err != nil || slices.Equal(b, c) {
		failf("CryptoReader: %v, %x after %x", err, c, b)
	}
}

// condWaiters starts n goroutines that each wait on c once (under mu) and
// count themselves woken. It returns once all n are waiting.
func condWaiters(w *dst.World, mu *dst.Mutex, c *dst.Cond, n int, woken *atomic.Int32, wg *dst.WaitGroup) {
	waiting := 0
	for range n {
		wg.Go(w, func() {
			mu.Lock()
			waiting++
			c.Wait()
			woken.Add(1)
			mu.Unlock()
		})
	}
	for {
		mu.Lock()
		all := waiting == n // each counted itself before Wait released mu
		mu.Unlock()
		if all {
			return
		}
		w.Sleep(tick)
	}
}

func condSignalBroadcast(w *dst.World) {
	var mu dst.Mutex
	c := dst.NewCond(&mu)
	var woken atomic.Int32
	var wg dst.WaitGroup
	condWaiters(w, &mu, c, 3, &woken, &wg)
	if n := woken.Load(); n != 0 {
		failf("%d woken before any Signal", n)
	}
	c.Signal()
	w.Sleep(4 * tick)
	if n := woken.Load(); n != 1 {
		failf("Signal woke %d of 3 waiters, want 1", n)
	}
	mu.Lock()
	c.Broadcast()
	mu.Unlock()
	wg.Wait()
	if n := woken.Load(); n != 3 {
		failf("Broadcast left %d of 3 waiters woken", n)
	}
}

func condReacquires(w *dst.World) {
	var mu dst.Mutex
	c := dst.Cond{L: &mu} // the zero value with L set works too
	ready := false
	var held atomic.Bool
	var wg dst.WaitGroup
	wg.Go(w, func() {
		mu.Lock()
		for !ready {
			c.Wait()
		}
		held.Store(!mu.TryLock()) // Wait must have taken mu back
		mu.Unlock()
	})
	w.Sleep(tick)
	mu.Lock()
	ready = true
	c.Signal()
	mu.Unlock()
	wg.Wait()
	if !held.Load() {
		failf("Wait returned without holding L")
	}
}

func condLostSignal(w *dst.World) {
	var mu dst.Mutex
	c := dst.NewCond(&mu)
	c.Signal() // nobody waits: lost
	c.Broadcast()
	var woken atomic.Int32
	var wg dst.WaitGroup
	condWaiters(w, &mu, c, 1, &woken, &wg)
	w.Sleep(4 * tick)
	if woken.Load() != 0 {
		failf("a Signal made before Wait woke a later waiter")
	}
	c.Signal()
	wg.Wait()
}

// typedCase builds a case at position pos for the exhaustive checks: a
// receive or a send, ready or not. fired records which position ran and
// with what value.
func typedCase(w *dst.World, pos int, send, ready bool, fired *[2]int) dst.Op[int] {
	if send {
		var c dst.Chan[int]
		if ready {
			c = dst.MakeChan[int](w, 1) // room to send
		} else {
			c = dst.MakeChan[int](w, 0) // nobody receives
		}
		return dst.OnSend(c, 100+pos, func() { *fired = [2]int{pos, 100 + pos} })
	}
	var c dst.Chan[int]
	if ready {
		c = dst.MakeChan[int](w, 1)
		c.Send(10 + pos)
	} else {
		c = dst.MakeChan[int](w, 0) // nobody sends
	}
	return dst.OnRecv(c, func(v int, ok bool) {
		if !ok {
			v = -2 // received from a closed channel: wrong
		}
		*fired = [2]int{pos, v}
	})
}

// exhaustive runs sel with n cases for every combination of directions,
// with exactly one case ready, and checks that exactly that case fired with
// its value.
func exhaustive(w *dst.World, n int, sel func(cases []dst.Op[int])) {
	for dirs := 0; dirs < 1<<n; dirs++ {
		for readyPos := 0; readyPos < n; readyPos++ {
			fired := [2]int{-1, -1}
			cases := make([]dst.Op[int], n)
			for pos := range n {
				cases[pos] = typedCase(w, pos, dirs&(1<<pos) != 0, pos == readyPos, &fired)
			}
			// Run it apart, with a limit: a case wired to the wrong channel
			// direction would otherwise hang the check.
			done := dst.MakeChan[int](w, 1)
			w.Go(func() {
				sel(cases)
				done.Send(1)
			})
			hung := false
			dst.Select2(w,
				dst.OnRecv(done, func(int, bool) {}),
				dst.OnRecv(w.After(time.Second), func(time.Time, bool) { hung = true }),
			)
			if hung {
				failf("directions %0*b, only case %d ready: select never returned", n, dirs, readyPos)
			}
			want := 10 + readyPos
			if dirs&(1<<readyPos) != 0 {
				want = 100 + readyPos
			}
			if fired != [2]int{readyPos, want} {
				failf("directions %0*b, only case %d ready: fired %v, want [%d %d]", n, dirs, readyPos, fired, readyPos, want)
			}
		}
	}
}

func select2Exhaustive(w *dst.World) {
	exhaustive(w, 2, func(c []dst.Op[int]) { dst.Select2(w, c[0], c[1]) })
}

func select3Exhaustive(w *dst.World) {
	exhaustive(w, 3, func(c []dst.Op[int]) { dst.Select3(w, c[0], c[1], c[2]) })
}

func select2Ready(w *dst.World) {
	a, b := dst.MakeChan[int](w, 1), dst.MakeChan[string](w, 1)
	seen := map[string]bool{}
	for range 200 {
		a.TrySend(1)
		b.TrySend("x")
		dst.Select2(w,
			dst.OnRecv(a, func(int, bool) { seen["a"] = true }),
			dst.OnRecv(b, func(string, bool) { seen["b"] = true }),
		)
	}
	if !seen["a"] || !seen["b"] {
		failf("with both ready, 200 Select2s chose only %v", seen)
	}
}

func select2Waits(w *dst.World) {
	a, b := dst.MakeChan[int](w, 0), dst.MakeChan[string](w, 0)
	w.Go(func() {
		w.Sleep(tick)
		b.Send("late")
	})
	got := ""
	dst.Select2(w,
		dst.OnRecv(a, func(int, bool) { failf("a was never sent to") }),
		dst.OnRecv(b, func(v string, _ bool) { got = v }),
	)
	if got != "late" {
		failf("got %q, want late", got)
	}
}

func select2ClosedDoneNil(w *dst.World) {
	var nilc dst.Chan[int]
	closed := dst.MakeChan[int](w, 0)
	closed.Close()
	ok := true
	dst.Select2(w,
		dst.OnRecv(nilc, func(int, bool) { failf("nil channel was ready") }),
		dst.OnRecv(closed, func(_ int, k bool) { ok = k }),
	)
	if ok {
		failf("receive from a closed channel reported ok")
	}
	ctx, cancel := dst.WithCancel(w, context.Background())
	cancel()
	done := false
	dst.Select3(w,
		dst.OnRecv(nilc, func(int, bool) { failf("nil channel was ready") }),
		dst.OnSend(nilc, 1, func() { failf("send on nil channel was ready") }),
		dst.OnDone(ctx, func() { done = true }),
	)
	if !done {
		failf("OnDone didn't fire for a cancelled context")
	}
}
