//go:build !race

// The race detector reports this misuse itself (it is a data race), so the
// test runs without it.

package simworld_test

import (
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Verifies: [misuse-reported]
// Fails if: a raw goroutine's World operation goes unreported, is reported
// without the caller's stack, or hangs the run until the watchdog.
func TestRawGoroutineMisuse(t *testing.T) {
	start := time.Now()
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		ch := dst.MakeChan[int](w, 1)
		var started atomic.Bool
		go func() { // a leftover raw go statement
			started.Store(true)
			ch.Send(1)
		}()
		for i := 0; i < 100000 && !(started.Load() && i > 1000); i++ {
			w.Sleep(time.Microsecond)
		}
		for range 1000 {
			w.Sleep(time.Microsecond)
		}
	})
	var m *sched.MisuseError
	if !errors.As(r.Err, &m) || !strings.Contains(r.Err.Error(), "didn't start") {
		t.Fatalf("got %v", r.Err)
	}
	if !strings.Contains(m.Stack, "misuse_test.go") {
		t.Fatalf("stack doesn't show the caller:\n%s", m.Stack)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %v: the run waited for the watchdog", d)
	}
}

// Verifies: [misuse-reported] (a WaitGroup's waiters)
// Fails if: a raw goroutine whose Done should wake a simulated
// WaitGroup.Wait goes unreported (the waiter stays parked: the run ends
// in a deadlock instead).
func TestRawGoroutineWaitGroupMisuse(t *testing.T) {
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		var wg dst.WaitGroup
		var parked atomic.Bool
		wg.Add(1)
		go func() { // a goroutine of the real World, as a nil World gives
			for !parked.Load() {
				runtime.Gosched()
			}
			time.Sleep(5 * time.Millisecond) // let the waiter park
			wg.Done()
		}()
		w.Go(func() { // keeps the run going meanwhile
			for range 200000 {
				w.Sleep(time.Microsecond)
			}
		})
		parked.Store(true)
		wg.Wait()
	})
	var m *sched.MisuseError
	if !errors.As(r.Err, &m) || !strings.Contains(m.Stack, "misuse_test.go") {
		t.Fatalf("got %v", r.Err)
	}
}

// A sweep catches a leftover raw goroutine on its strictly checked seeds,
// though most seeds only sample the check.
func TestSweepCatchesRawGoroutine(t *testing.T) {
	rep := dsttest.Sweep(dsttest.Options{Seeds: 20, FaultPhase: time.Second, RecoveryPhase: time.Second}, func(h *dsttest.Harness) {
		h.Go(func(w *dst.World) {
			ch := dst.MakeChan[int](w, 1)
			done := dst.MakeChan[int](w, 1)
			go func() { ch.Send(1) }() // the leftover raw go statement
			dst.Select(w, dst.OnRecv(ch, func(int, bool) {}), dst.OnRecv(w.After(100*time.Millisecond), func(time.Time, bool) {}))
			done.Send(1)
		})
	})
	found := false
	for _, f := range rep.Failed {
		var m *sched.MisuseError
		found = found || errors.As(f.Err, &m)
	}
	if !found {
		t.Fatalf("no seed reported the raw goroutine: %d failed, %v", len(rep.Failed), rep.Failed)
	}
}
