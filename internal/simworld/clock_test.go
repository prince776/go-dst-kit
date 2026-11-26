package simworld_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// near reports whether a and b differ by at most 1µs (float rounding).
func near(a, b time.Duration) bool { return a-b <= time.Microsecond && b-a <= time.Microsecond }

// Verifies: [clocks-are-local]
// Fails if: a skewed machine's Now, Sleep, timers, context timeouts or
// connection deadlines don't follow its own clock (offset + rate x true
// time), or its clock goes backwards.
func TestClocksAreLocal(t *testing.T) {
	clocks := map[string]sched.Clock{
		"fast": {Offset: 5 * time.Millisecond, Rate: 1.01},
		"slow": {Offset: -3 * time.Millisecond, Rate: 0.99},
	}
	sim := simworld.New(simworld.Options{Seed: 1})
	var errs []string
	fail := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }
	for name, c := range clocks {
		m := sim.AddMachine(name, simworld.MachineOptions{Clock: c})
		m.Start = func(w *dst.World) {
			local := func() time.Duration { return w.Now().Sub(simworld.Epoch) }
			want := func() time.Duration { return c.Offset + time.Duration(float64(sim.S.Now())*c.Rate) }
			if !near(local(), want()) {
				fail("%s: Now reads %v, want %v", name, local(), want())
			}
			last := local()
			measure := func(what string, d time.Duration, wait func()) {
				l0, t0 := local(), sim.S.Now()
				wait()
				l, tr := local()-l0, sim.S.Now()-t0
				if l < d || l > d+time.Microsecond {
					fail("%s: %s took %v of local time, want %v", name, what, l, d)
				}
				if !near(tr, time.Duration(float64(d)/c.Rate)) {
					fail("%s: %s took %v of true time, want %v", name, what, tr, time.Duration(float64(d)/c.Rate))
				}
				if local() < last {
					fail("%s: clock went backwards", name)
				}
				last = local()
			}
			measure("Sleep(100ms)", 100*time.Millisecond, func() { w.Sleep(100 * time.Millisecond) })
			measure("a 50ms timer", 50*time.Millisecond, func() { w.NewTimer(50 * time.Millisecond).C.Recv() })
			measure("a 30ms context timeout", 30*time.Millisecond, func() {
				ctx, cancel := dst.WithTimeout(w, context.Background(), 30*time.Millisecond)
				defer cancel()
				dst.Done(ctx).Recv()
			})
			// A read deadline: dial a listener on this machine that never answers.
			ln, err := w.Net().Listen("tcp", "localhost:9")
			if err != nil {
				fail("%s: listen: %v", name, err)
				return
			}
			conn, err := w.Net().Dial("tcp", "localhost:9")
			if err != nil {
				fail("%s: dial: %v", name, err)
				return
			}
			measure("a 40ms read deadline", 40*time.Millisecond, func() {
				conn.SetReadDeadline(w.Now().Add(40 * time.Millisecond))
				if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
					fail("%s: read: %v, want a deadline error", name, err)
				}
			})
			conn.Close()
			ln.Close()
		}
	}
	res := sim.Run(func(w *dst.World) {
		for _, m := range sim.Machines() {
			if m.Name != "main" {
				m.Boot()
			}
		}
		w.Sleep(time.Second)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	for _, e := range errs {
		t.Error(e)
	}
}

// Verifies: [clock-survives-restart]
// Fails if: a restarted machine's clock starts over instead of carrying on.
func TestClockSurvivesRestart(t *testing.T) {
	c := sched.Clock{Offset: 7 * time.Millisecond, Rate: 1.02}
	sim := simworld.New(simworld.Options{Seed: 1})
	m := sim.AddMachine("db", simworld.MachineOptions{Clock: c})
	var boots []time.Duration
	m.Start = func(w *dst.World) {
		boots = append(boots, w.Now().Sub(simworld.Epoch))
		for {
			w.Sleep(time.Millisecond)
		}
	}
	res := sim.Run(func(w *dst.World) {
		m.Boot()
		w.Sleep(100 * time.Millisecond)
		m.Crash()
		w.Sleep(100 * time.Millisecond)
		m.Restart()
		w.Sleep(time.Millisecond)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	want := c.Offset + time.Duration(float64(200*time.Millisecond)*c.Rate)
	if len(boots) != 2 || !near(boots[0], c.Offset) || !near(boots[1], want) {
		t.Fatalf("boot clock readings %v, want [%v %v]", boots, c.Offset, want)
	}
}
