package dst

import (
	"time"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Timer is time.Timer for a World.
type Timer struct {
	C    Chan[time.Time]
	real *time.Timer
	w    *World
	ev   sched.Timer
	fn   func() // AfterFunc
}

// NewTimer is time.NewTimer.
func (w *World) NewTimer(d time.Duration) *Timer {
	if w.s == nil {
		t := time.NewTimer(d)
		return &Timer{C: RecvOnly(t.C), real: t}
	}
	t := &Timer{C: Chan[time.Time]{sim: sched.NewChan[time.Time](w.s, 1)}, w: w}
	t.arm(d)
	return t
}

// AfterFunc is time.AfterFunc: fn runs in its own goroutine after d.
func (w *World) AfterFunc(d time.Duration, fn func()) *Timer {
	if w.s == nil {
		return &Timer{real: time.AfterFunc(d, fn)}
	}
	t := &Timer{w: w, fn: fn}
	t.arm(d)
	return t
}

// Tick is time.Tick: a ticker's channel, nil (the zero Chan) for d <= 0.
func (w *World) Tick(d time.Duration) Chan[time.Time] {
	if w.s == nil {
		if c := time.Tick(d); c != nil {
			return RecvOnly(c)
		}
		return Chan[time.Time]{}
	}
	if d <= 0 {
		return Chan[time.Time]{}
	}
	return w.NewTicker(d).C
}

// After is time.After.
func (w *World) After(d time.Duration) Chan[time.Time] { return w.NewTimer(d).C }

func (t *Timer) arm(d time.Duration) {
	s, m := t.w.s, t.w.m
	inc := m.Incarnation
	t.ev = s.At(m.TrueAfter(d), func() { // d by this machine's clock
		if !m.Alive(inc) {
			return // the machine that set this timer crashed
		}
		if t.fn != nil {
			s.Spawn(m, "AfterFunc "+funcName(t.fn), t.fn)
			return
		}
		t.C.sim.TrySend(t.w.Now())
	})
}

// Stop is (*time.Timer).Stop. In a simulated World, as in Go 1.23+, no
// stale value is received after Stop returns. In the real World it is
// the time package's Stop, with the timer semantics of the main module's
// go.mod: before go 1.23 (or with GODEBUG asynctimerchan=1) a value sent
// before Stop may still be received after it.
func (t *Timer) Stop() bool {
	if t.real != nil {
		return t.real.Stop()
	}
	stopped := t.ev.Stop()
	if t.C.sim != nil {
		if _, _, got := t.C.sim.TryRecv(); got {
			stopped = true // fired but not yet received: as if stopped in time
		}
	}
	return stopped
}

// Reset is (*time.Timer).Reset.
func (t *Timer) Reset(d time.Duration) bool {
	if t.real != nil {
		return t.real.Reset(d)
	}
	active := t.Stop()
	t.arm(d)
	return active
}

// Ticker is time.Ticker for a World.
type Ticker struct {
	C    Chan[time.Time]
	real *time.Ticker
	w    *World
	d    time.Duration
	ev   sched.Timer
}

// NewTicker is time.NewTicker. Like Go's, it drops ticks for a slow
// receiver.
func (w *World) NewTicker(d time.Duration) *Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	if w.s == nil {
		t := time.NewTicker(d)
		return &Ticker{C: RecvOnly(t.C), real: t}
	}
	t := &Ticker{C: Chan[time.Time]{sim: sched.NewChan[time.Time](w.s, 1)}, w: w, d: d}
	t.arm()
	return t
}

func (t *Ticker) arm() {
	s, m := t.w.s, t.w.m
	inc := m.Incarnation
	t.ev = s.At(m.TrueAfter(t.d), func() {
		if !m.Alive(inc) {
			return // the machine that started this ticker crashed
		}
		t.C.sim.TrySend(t.w.Now())
		t.arm()
	})
}

// Stop is (*time.Ticker).Stop.
func (t *Ticker) Stop() {
	if t.real != nil {
		t.real.Stop()
		return
	}
	t.ev.Stop()
	t.C.sim.TryRecv()
}

// Reset is (*time.Ticker).Reset.
func (t *Ticker) Reset(d time.Duration) {
	if t.real != nil {
		t.real.Reset(d)
		return
	}
	t.Stop()
	t.d = d
	t.arm()
}
