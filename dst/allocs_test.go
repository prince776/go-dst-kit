//go:build !race

package dst_test

import (
	"context"
	"io"
	"math/rand"
	randv2 "math/rand/v2"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/realworld"
)

// selectHandler holds a select's results, written by method values.
type selectHandler struct{ n int }

func (h *selectHandler) onInt(v int, _ bool)    { h.n += v }
func (h *selectHandler) onStr(s string, _ bool) { h.n += len(s) }

var (
	condSink  *sync.Cond
	dcondSink *dst.Cond
)

// Verifies: [real-is-std]
// In the real World each primitive allocates exactly what its std
// counterpart does, per operation (testing.AllocsPerRun), but for the
// documented exceptions, each held to its exact count: the dst.Timer
// (and Ticker) a timer is made with, the one value holding both ends of
// a Pipe, and Select2's (Select3's) callbacks when they capture
// variables. (dst.Select, which uses reflection, isn't measured.)
// Fails if a real path allocates more or less than that: a wrapper made
// per call, shadow state, a closure that escapes.
func TestRealPathAllocs(t *testing.T) {
	w := realworld.New(realworld.Options{})
	var (
		sm  sync.Mutex
		dm  dst.Mutex
		srw sync.RWMutex
		drw dst.RWMutex
		swg sync.WaitGroup
		dwg dst.WaitGroup
		so  sync.Once
		do  dst.Once
		sc  = sync.NewCond(&sm)
		dc  = dst.NewCond(&dm)
		lit = &dst.Cond{L: &dm}
		ch  = make(chan int, 1)
		dch = dst.MakeChan[int](w, 1)
		ch2 = make(chan string, 1)
		dc2 = dst.MakeChan[string](w, 1)
		sp  = sync.Pool{New: func() any { return new(int) }}
		dp  = dst.Pool{New: func() any { return new(int) }}
		sof = sync.OnceFunc(func() {})
		dof = dst.OnceFunc(func() {})
	)
	sink := 0
	var h selectHandler
	onInt, onStr := h.onInt, h.onStr // bound once, as the Select2 doc says
	cases := []struct {
		name     string
		std, dst func()
		extra    float64 // the documented exceptions' allocations
	}{
		{"Mutex Lock+Unlock", func() { sm.Lock(); sm.Unlock() }, func() { dm.Lock(); dm.Unlock() }, 0},
		{"Mutex TryLock", func() { sm.TryLock(); sm.Unlock() }, func() { dm.TryLock(); dm.Unlock() }, 0},
		{"RWMutex Lock+Unlock", func() { srw.Lock(); srw.Unlock() }, func() { drw.Lock(); drw.Unlock() }, 0},
		{"RWMutex RLock+RUnlock", func() { srw.RLock(); srw.RUnlock() }, func() { drw.RLock(); drw.RUnlock() }, 0},
		{"RWMutex TryRLock", func() { srw.TryRLock(); srw.RUnlock() }, func() { drw.TryRLock(); drw.RUnlock() }, 0},
		{"RWMutex RLocker", func() { l := srw.RLocker(); l.Lock(); l.Unlock() }, func() { l := drw.RLocker(); l.Lock(); l.Unlock() }, 0},
		{"WaitGroup Add+Done+Wait", func() { swg.Add(1); swg.Done(); swg.Wait() }, func() { dwg.Add(1); dwg.Done(); dwg.Wait() }, 0},
		{"Once Do (done)", func() { so.Do(func() {}) }, func() { do.Do(func() {}) }, 0},
		{"OnceFunc (done)", func() { sof() }, func() { dof() }, 0},
		{"Cond Signal+Broadcast", func() { sc.Signal(); sc.Broadcast() }, func() { dc.Signal(); dc.Broadcast() }, 0},
		{"Cond literal Signal", func() { sc.Signal() }, func() { lit.Signal() }, 0},
		{"NewCond", func() { condSink = sync.NewCond(&sm) }, func() { dcondSink = dst.NewCond(&dm) }, 0},
		{"chan send+recv", func() { ch <- 1; sink += <-ch }, func() { dch.Send(1); v, _ := dch.Recv(); sink += v }, 0},
		{"chan TrySend+TryRecv", func() {
			select {
			case ch <- 1:
			default:
			}
			select {
			case v := <-ch:
				sink += v
			default:
			}
		}, func() { dch.TrySend(1); v, _, _ := dch.TryRecv(); sink += v }, 0},
		{"chan len+cap", func() { sink += len(ch) + cap(ch) }, func() { sink += dch.Len() + dch.Cap() }, 0},
		{"Select2 (one ready)", func() {
			ch <- 1
			select {
			case v := <-ch:
				sink += v
			case s := <-ch2:
				sink += len(s)
			}
		}, func() {
			dch.Send(1)
			dst.Select2(w, dst.OnRecv(dch, func(v int, _ bool) { sink += v }), dst.OnRecv(dc2, func(s string, _ bool) { sink += len(s) }))
		}, 2}, // the two capturing callbacks: they reach the simulated path's Select, so escape
		{"Select2 (static callbacks)", func() {
			ch <- 1
			select {
			case <-ch:
			case <-ch2:
			}
		}, func() {
			dch.Send(1)
			dst.Select2(w, dst.OnRecv(dch, func(int, bool) {}), dst.OnRecv(dc2, func(string, bool) {}))
		}, 0},
		{"Select2 (method values bound once)", func() {
			ch <- 1
			select {
			case v := <-ch:
				sink += v
			case s := <-ch2:
				sink += len(s)
			}
		}, func() {
			dch.Send(1)
			dst.Select2(w, dst.OnRecv(dch, onInt), dst.OnRecv(dc2, onStr))
		}, 0},
		{"Pool Get+Put", func() { sp.Put(sp.Get()) }, func() { dp.Put(dp.Get()) }, 0},
		{"Now", func() { _ = time.Now() }, func() { _ = w.Now() }, 0},
		{"Since", func() { _ = time.Since(time.Time{}) }, func() { _ = w.Since(time.Time{}) }, 0},
		{"Sleep(0)", func() { time.Sleep(0) }, func() { w.Sleep(0) }, 0},
		{"NewTimer+Stop", func() { time.NewTimer(time.Hour).Stop() }, func() { w.NewTimer(time.Hour).Stop() }, 1},                          // the dst.Timer
		{"AfterFunc+Stop", func() { time.AfterFunc(time.Hour, func() {}).Stop() }, func() { w.AfterFunc(time.Hour, func() {}).Stop() }, 1}, // the dst.Timer
		{"NewTicker+Stop", func() { time.NewTicker(time.Hour).Stop() }, func() { w.NewTicker(time.Hour).Stop() }, 1},                       // the dst.Ticker
		{"WithCancel+cancel", func() { _, c := context.WithCancel(context.Background()); c() }, func() { _, c := dst.WithCancel(w, context.Background()); c() }, 0},
		{"WithTimeout+cancel", func() { _, c := context.WithTimeout(context.Background(), time.Hour); c() }, func() { _, c := dst.WithTimeout(w, context.Background(), time.Hour); c() }, 0},
		{"WithCancelCause+cancel", func() { _, c := context.WithCancelCause(context.Background()); c(nil) }, func() { _, c := dst.WithCancelCause(w, context.Background()); c(nil) }, 0},
		{"AfterFunc(ctx)+stop", func() { context.AfterFunc(context.Background(), func() {})() }, func() { dst.AfterDone(context.Background(), func() {})() }, 0},
		{"rand/v2 IntN", func() { sink += randv2.IntN(10) }, func() { sink += w.Rand().IntN(10) }, 0},
		{"rand/v2 N", func() { sink += randv2.N(10) }, func() { sink += dst.N(w.Rand(), 10) }, 0},
		{"math/rand Intn", func() { sink += rand.Intn(10) }, func() { sink += w.MathRand().Intn(10) }, 0},
		{"io.Pipe", func() { r, wr := io.Pipe(); r.Close(); wr.Close() }, func() { r, wr := dst.Pipe(w); r.Close(); wr.Close() }, 1}, // both ends, in one value
		{"net.Pipe", func() { a, b := net.Pipe(); a.Close(); b.Close() }, func() { a, b := dst.NetPipe(w); a.Close(); b.Close() }, 0},
	}
	measure := func(mode string) {
		for _, c := range cases {
			c.std()
			c.dst() // warm both (lazy state, pools)
			std := testing.AllocsPerRun(200, c.std)
			got := testing.AllocsPerRun(200, c.dst)
			if got != std+c.extra {
				t.Errorf("%s, %s: %v allocations per run in the real World, want std's %v + %v", c.name, mode, got, std, c.extra)
			}
		}
	}
	measure("alone")
	// And while a simulation runs in the process: the real path then
	// takes its one branch (is this goroutine simulated?) and must still
	// allocate nothing more.
	stop := simAlongside(t)
	measure("alongside a simulation")
	stop()
	_ = sink
}
