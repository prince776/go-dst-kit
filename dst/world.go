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
//	time.NewTimer/After     w.NewTimer(d) / w.After(d) (time.Tick: w.Tick(d))
//	sync.Mutex/WaitGroup    dst.Mutex / dst.WaitGroup (zero values work)
//	sync.Cond               dst.Cond (dst.NewCond)
//	sync.OnceFunc/OnceValue dst.OnceFunc / dst.OnceValue / dst.OnceValues
//	context.WithTimeout     dst.WithTimeout(w, ctx, d) (and the Cause variants)
//	context.AfterFunc       dst.AfterDone(ctx, f)
//	runtime.Gosched         w.Yield()
//	math/rand/v2            w.Rand() (rand.N: dst.N(w.Rand(), n))
//	math/rand (v1)          w.MathRand()
//	crypto/rand.Read        w.Rand().CryptoRead (Reader: dst.CryptoReader(w), Text: dst.CryptoText(w))
//	os, net                 w.FS(), w.Net()
//	os.Link                 dst.Link(w.FS(), old, new)
//	os.Chmod, Truncate, ... dst.Chmod(w.FS(), name, mode), ... (os's own in production)
//	filepath.WalkDir, Glob  dst.WalkDir(w.FS(), root, fn), dst.Glob(w.FS(), pattern)
//	io.Pipe / net.Pipe      dst.Pipe(w) / dst.NetPipe(w)
//	net.DialTimeout         dst.DialTimeout(w, network, address, d)
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
//   - [raw-is-the-channel] In the real World, Chan.Raw is the native
//     channel values flow through; on a simulated channel it panics.
//   - [real-never-injects] In the real World, Inject always returns None,
//     Do and Call always run their operation, and Buggify is always false.
//   - [call-fail-never-runs] On Fail, Call doesn't run its operation and
//     returns the Before error.
//   - [call-fail-after-runs] On FailAfter, Call runs its operation once
//     and returns the After error, not its result.
//   - [call-latency-honours-ctx] Injected latency ends when the caller's
//     context does: Call returns its error then, having run the operation
//     unless the request was refused.
//   - [real-is-std] In the real World every primitive is its std
//     counterpart plus at most one branch on a process-wide flag (is any
//     simulation running?): no shadow state is read or written there, and
//     each operation allocates exactly what std's does, but for the
//     documented exceptions (a Timer or Ticker is one wrapper more; a
//     Pipe's two ends one value more; Select2 and Select3 callbacks that
//     capture variables one each; Select uses reflection).
//
// Mixing Worlds: a primitive a simulation used works in the real World
// after the simulation ends. Real and simulated goroutines sharing one
// primitive at once is a bug in the test: a WaitGroup reports it as
// misuse; a lock or Cond a real goroutine releases doesn't wake simulated
// waiters, which the run reports as a deadlock (and the determinism
// check, as nondeterminism). The same holds between simulations running
// at once (a sweep's parallel seeds): one can't wake another's waiters,
// so a package-level lock they block on ends a run in a deadlock naming
// it; seeds must not share state they wait on.
//
// Children: World, Current, Chan, Select, Mutex, RWMutex, WaitGroup, Once, OnceFunc,
// OnceValue, OnceValues, Cond, Pool, Timer, Ticker, WithCancel, WithCancelCause,
// WithTimeout, WithTimeoutCause, WithDeadline, WithDeadlineCause, AfterDone, Rand, N,
// MathRand, CryptoReader, CryptoText, Pipe, NetPipe, DialTimeout, Inject, Do, Call, Buggify
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
	math  *simMathRand // MathRand's, made on first use (simulated Worlds)
	fs    FS
	net   Net
}

func init() {
	worldhook.NewWorld = func(sp worldhook.Spec) any {
		w := &World{s: sp.Sched, m: sp.Machine, epoch: sp.Epoch}
		if w.s != nil {
			w.rnd = newSimRand("machine/"+w.m.Name+"/rand", w.s.Rand)
		} else {
			w.rnd = realRand{}
		}
		if sp.FS != nil {
			w.fs = sp.FS.(FS)
		}
		if sp.Net != nil {
			w.net = sp.Net.(Net)
		}
		if sp.Machine != nil {
			sp.Machine.World = w
		}
		return w
	}
}

// Current returns the World of the calling goroutine, for code that no
// World can reach. It is a last resort: pass a World (as a field, a
// parameter, an option) wherever one is reachable, and use Current only
// in leaf code with no owner to hand it one, such as a package-level
// helper called from everywhere. dstlint reports every use.
//
// For a goroutine a simulated World started, Current is that machine's
// World. For any other goroutine (one a raw go statement started, a
// finalizer, the test's own goroutine) it is the real World, even while
// a simulation runs: such a goroutine gets real randomness and the real
// clock, and nothing reports it. The determinism check is the backstop:
// a seed whose runs differ because of it is reported as
// nondeterministic. Outside a simulation the World Current returns has
// goroutines, time, locks and randomness but no filesystem or network
// (realworld.New gives those).
//
// Verified:
//   - [current-is-the-callers] A goroutine a simulated World started gets
//     its own machine's World (even with other machines and other
//     simulations running at once); a goroutine no World started gets the
//     real World.
func Current() *World {
	if t := sched.CurrentTask(); t != nil {
		if w, ok := t.Machine.World.(*World); ok {
			return w
		}
	}
	return currentReal
}

// currentReal is Current's real World.
var currentReal = &World{rnd: realRand{}}

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

// Yield lets other goroutines run, as runtime.Gosched does in a spin. Use
// it where code waits for another goroutine without any World operation
// in between, such as a loop spinning on an atomic:
//
//	for flag.Load() == 0 {
//		w.Yield()
//	}
//
// In a simulated World only one goroutine runs at a time, so such a loop
// would otherwise never let the other one run. There Yield returns only
// after another goroutine has taken a step or a timer has fired: the
// goroutine waited for runs, time advances while it sleeps, and a spin
// that nothing can ever end is reported as a deadlock.
//
// Verified:
// Yield or Gosched? A loop whose exit condition depends on another
// goroutine is a spin and takes Yield. A loop that makes its own progress
// and only offers others a turn (evicting a batch of entries per lock
// hold, say) takes Gosched. Yield in a self-progressing loop is wrong when
// nothing else can run: simulated time jumps to the next timer before the
// loop goes on, and if that timer is far off (the end of the run) it shows
// as a liveness hang.
//
// Verified:
//   - [yield-waits-for-progress] In a simulated World, Yield returns only
//     after another goroutine has taken a step or time has advanced; a
//     spin waiting for a sleeping goroutine ends at its wake-up after a
//     handful of iterations, and a spin no one can end is a deadlock.
func (w *World) Yield() {
	if w.s == nil {
		runtime.Gosched()
		return
	}
	w.s.YieldToOthers()
}

// Gosched is a plain scheduling point, as runtime.Gosched is outside a
// spin: the goroutine stays runnable, and another runnable one may run
// first. In the real World it is runtime.Gosched. Use it in a loop that
// makes its own progress and only offers others a turn; a loop waiting
// for another goroutine takes Yield (see Yield for telling them apart).
//
// Verified:
//   - [gosched-stays-runnable] Gosched never waits for time to advance:
//     a self-progressing loop calling it finishes at the same simulated
//     instant, and another runnable goroutine may run before it goes on.
func (w *World) Gosched() {
	if w.s == nil {
		runtime.Gosched()
		return
	}
	w.s.Yield()
}

// Sleep pauses the goroutine for d, as time.Sleep does.
func (w *World) Sleep(d time.Duration) {
	if w.s == nil {
		time.Sleep(d)
		return
	}
	if d <= 0 {
		w.s.Sleep(d) // a scheduling point
		return
	}
	w.s.Sleep(w.m.TrueAfter(d) - w.s.Now()) // d by this machine's clock
}

// Now is time.Now.
func (w *World) Now() time.Time {
	if w.s == nil {
		return time.Now()
	}
	return w.epoch.Add(w.m.LocalNow())
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
