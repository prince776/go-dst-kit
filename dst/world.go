// Package dst is what code uses, instead of the language and the standard
// library, for everything that waits or is nondeterministic: goroutines,
// channels, select, sleeping, the clock, timers, locks, contexts,
// randomness, files and the network. A World provides them.
//
// In production the World is real (package realworld): every operation is
// the plain Go one. In tests the World is simulated (package dsttest): one
// seed decides every goroutine interleaving, delay and fault, so a failing
// seed replays exactly.
//
// Migrating code is a mechanical substitution:
//
//	go f()                  w.Go(f)
//	make(chan T, n)         dst.MakeChan[T](w, n)
//	c <- v / <-c / close    c.Send(v) / c.Recv() / c.Close()
//	for v := range c        for v := range c.All()
//	select { ... }          dst.Select(w, dst.OnRecv(c, ...), dst.OnSend(...), dst.Default(...))
//	time.Now / Sleep        w.Now() / w.Sleep(d)
//	time.NewTimer/After     w.NewTimer(d) / w.After(d)
//	sync.Mutex/WaitGroup    dst.Mutex / dst.WaitGroup (zero values work)
//	sync.Cond               dst.Cond (dst.NewCond)
//	context.WithTimeout     dst.WithTimeout(w, ctx, d)
//	math/rand, crypto/rand  w.Rand()
//	os, net                 w.FS(), w.Net()
//
// Inside a simulated World, code must not block any other way: a raw
// channel, sync.Mutex held across a wait, time.Sleep or real I/O escapes
// the scheduler. The simulator reports such escapes (it doesn't hang), and
// the dstlint analyzer finds them before running.
//
// Verified:
//   - [same-code-both-worlds] The contract suite passes unchanged against
//     the real World and the simulated one.
//   - [select-picks-ready] Select runs exactly one case, preferring ready
//     ones. A ready case is chosen at random among the ready ones; with
//     none ready it runs Default if there is one, else waits for the first
//     to become ready.
//   - [timers-follow-world-clock] Timers, tickers, Sleep and context
//     deadlines fire by the World's clock, in deadline order.
//
// Children: World, Chan, Select, Mutex, RWMutex, WaitGroup, Once, Cond, Timer,
// Ticker, WithCancel, WithTimeout, WithDeadline, Rand
// Depends on: internal/sched (the simulator's engine)
package dst

import (
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/worldhook"
)

// World provides goroutines, time, randomness, files and the network,
// either for real or simulated. Code takes a *World and uses nothing else
// for these. A World is safe for concurrent use by the goroutines it
// started.
type World struct {
	s     *sched.Scheduler // nil in the real World
	m     *sched.Machine
	epoch time.Time
	rnd   Rand
	fs    FS
	net   Net
}

func init() {
	worldhook.NewWorld = func(sp worldhook.Spec) any {
		w := &World{s: sp.Sched, m: sp.Machine, epoch: sp.Epoch}
		if w.s != nil {
			w.rnd = simRand{w.s.Rand("machine/" + w.m.Name + "/rand")}
		} else {
			w.rnd = realRand{}
		}
		if sp.FS != nil {
			w.fs = sp.FS.(FS)
		}
		if sp.Net != nil {
			w.net = sp.Net.(Net)
		}
		return w
	}
}

// Simulated reports whether w is a simulated World.
func (w *World) Simulated() bool { return w.s != nil }

// Go starts fn in a new goroutine, as `go fn()` does.
func (w *World) Go(fn func()) {
	if w.s == nil {
		go fn()
		return
	}
	w.GoNamed(funcName(fn), fn)
}

// GoNamed is Go with a name for the goroutine, shown in deadlock reports
// and event logs.
func (w *World) GoNamed(name string, fn func()) {
	if w.s == nil {
		go fn()
		return
	}
	w.s.Current()
	w.s.Spawn(w.m, name, fn)
	w.s.Yield()
}

// Sleep pauses the goroutine for d, as time.Sleep does.
func (w *World) Sleep(d time.Duration) {
	if w.s == nil {
		time.Sleep(d)
		return
	}
	w.s.Sleep(d)
}

// Now is time.Now.
func (w *World) Now() time.Time {
	if w.s == nil {
		return time.Now()
	}
	return w.epoch.Add(w.s.Now())
}

// Since is time.Since.
func (w *World) Since(t time.Time) time.Duration { return w.Now().Sub(t) }

// Until is time.Until.
func (w *World) Until(t time.Time) time.Duration { return t.Sub(w.Now()) }

// Rand is the World's randomness, for math/rand and crypto/rand uses that
// decide behaviour (IDs, jitter, election timeouts).
func (w *World) Rand() Rand { return w.rnd }

// FS is the World's filesystem.
func (w *World) FS() FS {
	if w.fs == nil {
		panic("dst: this World has no filesystem")
	}
	return w.fs
}

// Net is the World's network.
func (w *World) Net() Net {
	if w.net == nil {
		panic("dst: this World has no network")
	}
	return w.net
}

// Logf writes to the simulation's event log (shown when replaying a seed
// verbosely). It does nothing in the real World.
func (w *World) Logf(format string, args ...any) {
	if w.s != nil {
		w.s.Logf(format, args...)
	}
}

// funcName is a short readable name for fn ("pool.(*Pool).worker").
func funcName(fn func()) string {
	f := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
	if f == nil {
		return "goroutine"
	}
	name := f.Name()
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	return name
}
