// Package sched is the deterministic engine under the simulated World: a
// baton scheduler for goroutines, a virtual clock with timers, wait queues,
// and machines (groups of goroutines that crash together).
//
// Every simulated goroutine (a Task) is a real goroutine, but only one runs
// at a time. It holds the baton until it reaches a scheduling point (a World
// operation), where it hands the baton back and the seed picks which
// runnable task goes next. When none is runnable, the clock jumps to the
// next timer. Nothing else decides the order, so a seed replays exactly.
//
// Verified:
//   - [one-at-a-time] At most one task runs at any moment.
//   - [seed-replays] The same seed and program give the same sequence of
//     scheduling decisions and the same fingerprint.
//   - [deadlock-reported] A run where nothing can run or wake fails, listing
//     each live task and what it waits for. That is: no task runnable, no
//     timer pending, and the run's goal not met.
//   - [leak-reported] A task that keeps the baton longer than the watchdog
//     (blocked outside the World) fails the run, naming the task.
//   - [panic-reported] A panic in a task fails the run with the task's name
//     and the panic site.
//   - [crash-is-final] A crashed machine's tasks never run again.
//   - [pct-is-legal] PCT scheduling produces only outcomes Go's channel
//     and select semantics allow.
//   - [pct-finds-starvation] PCT finds a bug needing one goroutine starved
//     for 50 steps, which uniform choice never does.
//   - [pct-no-livelock] Under PCT a polling loop can't starve the goroutine
//     it waits for: it is demoted after 100 steps.
//   - [misuse-reported] A World operation from a goroutine the World
//     didn't start, or from an invariant, fails the run with its stack.
//   - [nothing-leaks] After Shutdown, no task goroutine is left running.
//
// This package is single-threaded by construction: every exported method is
// called either by the task holding the baton or by the scheduler itself
// (timer callbacks), never concurrently.
package sched

import (
	"container/heap"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prince776/go-dst-kit/internal/points"
)

// Options configure a Scheduler.
type Options struct {
	Seed uint64
	// Watchdog is how long a task may keep the baton (in wall time) before
	// the run fails as blocked outside the World. Default 10s.
	Watchdog time.Duration
	// CheckpointEvery records the fingerprint every this many steps, to
	// locate where two runs of a seed diverge. Default 1000.
	CheckpointEvery uint64
	// Logf, if set, receives a line for every scheduling event (verbose
	// replay). It must not touch the World.
	Logf func(format string, args ...any)
	// MaxTime and MaxSteps end a run that goes on too long (a livelock, or
	// a goal never reached) with an error. Zero means no limit.
	MaxTime  time.Duration
	MaxSteps uint64
	// AfterStep, if set, runs on the scheduler after every task step and
	// timer (for invariants). It must not use the World; returning an
	// error fails the run.
	AfterStep func() error
	// PCTDepth, if positive, schedules by PCT (probabilistic concurrency
	// testing) instead of uniformly: every task gets a random priority, the
	// highest-priority runnable task always runs, and at PCTDepth-1 random
	// steps the running task drops below all others. A bug that needs d
	// specific orderings is found with probability at least about
	// 1/(n*k^(d-1)) per run (n tasks, k steps), where uniform choice can
	// make one long-starved task exponentially unlikely.
	PCTDepth int
	// PCTHorizon is the step range the priority drops are drawn from
	// (default 10000).
	PCTHorizon uint64
	// Forks continue the run, from each fork's anchor on, with every
	// stream of choices reseeded from the fork's seed (see Fork). They
	// apply in order: a fork whose anchor never comes leaves the rest
	// unapplied.
	Forks []Fork
	// Pairs records interleaving pairs (see FirstPairs). It costs a stack
	// walk at each scheduling point, so it is off unless exploring.
	Pairs bool
}

// Fork is a point where a run continues with fresh randomness: just after
// the Nth Mark(Marker) when Marker is set, else at step Step.
type Fork struct {
	Marker string
	N      int
	Step   uint64
	Seed   uint64
}

// stream is a stream of choices: reseeded at a fork.
type stream struct {
	key uint64
	pcg *rand.PCG
}

// Scheduler runs tasks one at a time in an order decided by the seed.
type Scheduler struct {
	// Points, if set, decides dst.Inject and World.Buggify calls.
	Points *points.Registry
	// OnPanic, if set, is asked about each panic in a task on a machine:
	// true means the panic is the process dying (fail-stop, as a real
	// panic kills a Go process), so the machine is killed (Kill) rather
	// than the run failed. It runs with the baton held.
	OnPanic func(t *Task, v any) bool

	opts Options
	rng  *rand.Rand
	// shuttingDown: Shutdown is killing tasks one at a time.
	shuttingDown bool
	streams      []*stream
	curSeed      uint64            // the seed streams are drawn from now (the last fork's)
	forked       int               // forks applied
	forkAt       []uint64          // the step each was applied at
	forkNow      bool              // the next fork's marker anchor has come
	marks        map[string]int    // Mark counts
	firstAt      map[string]uint64 // the step of each marker's first Mark
	pairs        map[uint64]uint64 // interleaving pair -> the step it first ran
	lastSite     uint64            // the site the last task handed off at
	now          time.Duration
	events       eventHeap
	seq          uint64
	tasks        []*Task
	runnable     []*Task
	cur          *Task
	// spinners are parked in YieldToOthers until another task takes a
	// step or a timer fires; spinStep says the step that just ended was
	// such a yield (which wakes no one).
	spinners []*Task
	spinStep bool
	handoff      chan struct{}
	hash         uint64
	steps        uint64
	checks       []uint64
	failMu       sync.Mutex // a goroutine outside the World may report misuse
	failure      error
	machines     []*Machine
	calls        uint64 // scheduling calls, for sampling the identity check
	// PCT state.
	pctRng   *rand.Rand
	drops    []uint64 // steps at which the running task's priority drops
	dropped  int      // how many drops happened
	lastPick *Task
	streak   int // consecutive picks of lastPick while others could run
	pausedN  int // machines paused now
	eligible []int
	loopGoid uint64
	// abort is closed when a misuse breaks the baton protocol: the loop
	// stops at once instead of waiting for a handoff that may never come.
	abort     chan struct{}
	abortOnce sync.Once
	queues    map[any][]*Task
	ids       int
	stopped   bool
}

// Machine is a group of tasks that live and die together: one process.
type Machine struct {
	ID   int
	Name string
	// World is the machine's *dst.World (as any: sched can't import dst),
	// for dst.Current.
	World any
	// Incarnation counts restarts.
	Incarnation int
	dead        bool
	s           *Scheduler
	onCrash     []func()
	clock       Clock
	paused      bool
	killed      bool // the last crash was a kill: the process died, the host didn't
	down, pause []Span
}

// Span is a stretch of true time; Open while it lasts (To is then 0).
type Span struct {
	From, To time.Duration
	Open     bool
}

func open(ss []Span, now time.Duration) []Span { return append(ss, Span{From: now, Open: true}) }

func closeLast(ss []Span, now time.Duration) {
	if n := len(ss); n > 0 && ss[n-1].Open {
		ss[n-1].To, ss[n-1].Open = now, false
	}
}

// Downtime is when the machine was crashed or killed, each crash to its
// restart (Open if it hasn't restarted).
func (m *Machine) Downtime() []Span { return slices.Clone(m.down) }

// Pauses is when the machine was paused, each pause to its resume or the
// crash that ended it.
func (m *Machine) Pauses() []Span { return slices.Clone(m.pause) }

// Paused reports whether the machine is paused.
func (m *Machine) Paused() bool { return m.paused }

// Clock is a machine's clock: its local time is Offset + Rate x the
// simulation's true time. It belongs to the machine, not the process, so
// it carries on across restarts. The zero Clock is true time.
type Clock struct {
	Offset time.Duration
	Rate   float64 // 1 + drift; 0 means 1
}

// SetClock sets the machine's clock (before it runs).
func (m *Machine) SetClock(c Clock) {
	if c.Rate == 0 {
		c.Rate = 1
	}
	m.clock = c
}

// Clock returns the machine's clock.
func (m *Machine) Clock() Clock {
	if m.clock.Rate == 0 {
		return Clock{Offset: m.clock.Offset, Rate: 1}
	}
	return m.clock
}

// Local converts true time to the machine's local time.
func (m *Machine) Local(t time.Duration) time.Duration {
	c := m.Clock()
	if c.Rate == 1 {
		return c.Offset + t
	}
	return c.Offset + time.Duration(float64(t)*c.Rate)
}

// LocalNow is the machine's local time now.
func (m *Machine) LocalNow() time.Duration { return m.Local(m.s.now) }

// TrueAt is the earliest true time, not before now, at which the machine's
// clock reads at least local: when a timer set for local should fire.
func (m *Machine) TrueAt(local time.Duration) time.Duration {
	c := m.Clock()
	var t time.Duration
	if c.Rate == 1 {
		t = local - c.Offset
	} else {
		t = time.Duration(math.Ceil(float64(local-c.Offset) / c.Rate))
		for m.Local(t) < local { // float rounding
			t++
		}
	}
	return max(t, m.s.now)
}

// TrueAfter is the true time at which d of the machine's local time will
// have passed.
func (m *Machine) TrueAfter(d time.Duration) time.Duration { return m.TrueAt(m.LocalNow() + d) }

// Dead reports whether the machine is crashed (and not yet restarted).
func (m *Machine) Dead() bool { return m.dead }

// Alive reports whether incarnation inc of the machine is still running:
// timers and callbacks created by an earlier incarnation must not fire.
func (m *Machine) Alive(inc int) bool { return !m.dead && m.Incarnation == inc }

// OnCrash registers fn to run when the machine crashes (e.g. its disk loses
// unsynced data). Hooks run in registration order; Killed tells them
// whether this crash is a kill.
func (m *Machine) OnCrash(fn func()) { m.onCrash = append(m.onCrash, fn) }

// Killed reports whether the machine's last crash was a kill (Kill): its
// process died but its host stayed up, so the host's page cache, and
// everything written to it, survived.
func (m *Machine) Killed() bool { return m.killed }

type state int

const (
	parked state = iota
	runnable
	running
	finished
)

// Task is one simulated goroutine.
type Task struct {
	ID      int
	Name    string
	Machine *Machine
	// Parent is the task that spawned this one (nil if spawned from
	// outside any task).
	Parent  *Task
	s       *Scheduler
	wake    chan struct{}
	state   state
	killed  bool
	blocked string
	goid    uint64
	prio    float64 // PCT priority (higher runs first)
	site    uint64  // where it last handed off, or started (Options.Pairs)
}

// Sched returns the task's scheduler.
func (t *Task) Sched() *Scheduler { return t.s }

type killSignal struct{}

// New returns a scheduler for one run.
func New(opts Options) *Scheduler {
	if opts.Watchdog == 0 {
		opts.Watchdog = 10 * time.Second
	}
	if opts.CheckpointEvery == 0 {
		opts.CheckpointEvery = 1000
	}
	s := &Scheduler{
		opts:    opts,
		handoff: make(chan struct{}),
		abort:   make(chan struct{}),
		hash:    14695981039346656037,
		queues:  map[any][]*Task{},
		curSeed: opts.Seed,
		marks:   map[string]int{},
		firstAt: map[string]uint64{},
		pairs:   map[uint64]uint64{},
	}
	s.rng = s.Rand("sched")
	if opts.PCTDepth > 0 {
		s.pctRng = s.Rand("pct")
		h := opts.PCTHorizon
		if h == 0 {
			h = 10000
		}
		for range opts.PCTDepth - 1 {
			s.drops = append(s.drops, 1+s.pctRng.Uint64N(h))
		}
		slices.Sort(s.drops)
	}
	return s
}

// starveLimit is how many steps in a row PCT lets one task run while
// others are runnable before demoting it, so a polling loop with the
// highest priority can't starve the goroutine it waits for.
const starveLimit = 100

// pick removes and returns the next task to run, or nil if every
// runnable task is on a paused machine.
func (s *Scheduler) pick() *Task {
	// The candidates: runnable tasks whose machine isn't paused.
	s.eligible = s.eligible[:0]
	for i, t := range s.runnable {
		if s.pausedN == 0 || !t.Machine.paused {
			s.eligible = append(s.eligible, i)
		}
	}
	n := len(s.eligible)
	if n == 0 {
		return nil
	}
	if s.pctRng == nil {
		i := s.eligible[s.rng.IntN(n)]
		t := s.runnable[i]
		s.runnable = slices.Delete(s.runnable, i, i+1)
		return t
	}
	best := func() int {
		b := s.eligible[0]
		for _, i := range s.eligible {
			if s.runnable[i].prio > s.runnable[b].prio {
				b = i
			}
		}
		return b
	}
	i := best()
	t := s.runnable[i]
	if s.dropped < len(s.drops) && s.steps+1 >= s.drops[s.dropped] {
		// A priority drop: the task about to run falls below every
		// starting priority (they are in (1, 2)).
		s.dropped++
		t.prio = 1 - float64(s.dropped)/float64(len(s.drops)+1)
		i = best()
		t = s.runnable[i]
	}
	if t == s.lastPick && n > 1 {
		s.streak++
		if s.streak >= starveLimit {
			low := t.prio
			for _, j := range s.eligible {
				low = min(low, s.runnable[j].prio)
			}
			t.prio = low - 1
			s.streak = 0
			i = best()
			t = s.runnable[i]
		}
	} else {
		s.streak = 0
	}
	s.lastPick = t
	s.runnable = slices.Delete(s.runnable, i, i+1)
	return t
}

// Rand returns a generator for one named consumer, derived from the seed
// and the label only. Adding a consumer never changes another's stream.
func (s *Scheduler) Rand(label string) *rand.Rand {
	k := labelKey(label)
	st := &stream{key: k, pcg: rand.NewPCG(s.curSeed^k, s.curSeed*0x9e3779b97f4a7c15+k)}
	s.streams = append(s.streams, st)
	return rand.New(st.pcg)
}

// ConfigRand is a stream for the run's configuration (a machine's clock,
// say): drawn from the root seed, and never reseeded at a fork.
func (s *Scheduler) ConfigRand(label string) *rand.Rand {
	k := labelKey(label)
	return rand.New(rand.NewPCG(s.opts.Seed^k, s.opts.Seed*0x9e3779b97f4a7c15+k))
}

func labelKey(label string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(label))
	return h.Sum64()
}

// fork reseeds every stream of choices from seed, each by its label.
func (s *Scheduler) fork(seed uint64) {
	s.curSeed = seed
	for _, st := range s.streams {
		st.pcg.Seed(seed^st.key, seed*0x9e3779b97f4a7c15+st.key)
	}
	s.forked++
	s.forkAt = append(s.forkAt, s.steps)
	s.forkNow = false
	s.Logf("fork %d: seed %d (step %d)", s.forked, seed, s.steps)
}

// Mark records that the marker name was reached (a Sometimes): it anchors
// forks, and its first step is reported by FirstMarks.
func (s *Scheduler) Mark(name string) {
	s.marks[name]++
	if s.marks[name] == 1 {
		s.firstAt[name] = s.steps
	}
	if s.forked < len(s.opts.Forks) {
		f := s.opts.Forks[s.forked]
		if f.Marker == name && s.marks[name] == f.N {
			s.forkNow = true // after the current step
		}
	}
}

// FirstMarks is the step at which each marker was first reached.
func (s *Scheduler) FirstMarks() map[string]uint64 { return s.firstAt }

// FirstPairs is the step at which each interleaving pair first ran, when
// Options.Pairs is set. A pair is two code sites: where one task handed
// the baton back (parked or yielded), and where the task that ran next
// resumes (its own last handoff, or its start). Sites are the first frame
// outside go-dst-kit, as function and line, so a pair names the same
// handoff in every process and run. A pair the run hasn't seen before is
// an ordering of the program's goroutines it hasn't tried: what the
// explorer steers by.
//
// Verified:
//   - [pairs-name-program-lines] Every pair is two of the program's own
//     scheduling points (or a start, a finished task, or the run's first
//     step), never a go-dst-kit frame; recording pairs doesn't change the
//     run, and nothing is recorded when Pairs is off.
//   - [pairs-same-in-every-process] A seed records the same pairs, at the
//     same steps, in a new process.
func (s *Scheduler) FirstPairs() map[uint64]uint64 { return s.pairs }

// ForkSteps is the step each applied fork was applied at.
func (s *Scheduler) ForkSteps() []uint64 { return s.forkAt }

// ForksApplied is how many of Options.Forks the run reached.
func (s *Scheduler) ForksApplied() int { return s.forked }

// Seed returns the run's seed.
func (s *Scheduler) Seed() uint64 { return s.opts.Seed }

// NewMachine adds a machine.
func (s *Scheduler) NewMachine(name string) *Machine {
	m := &Machine{ID: len(s.machines), Name: name, s: s}
	s.machines = append(s.machines, m)
	return m
}

// NextID returns a fresh identifier for diagnostics (channel numbers etc.).
func (s *Scheduler) NextID() int { s.ids++; return s.ids }

// Now is the virtual time since the start of the run.
func (s *Scheduler) Now() time.Duration { return s.now }

// Steps is the number of baton handoffs so far.
func (s *Scheduler) Steps() uint64 { return s.steps }

// Fingerprint is a hash of every scheduling decision, timer and traced
// value so far.
func (s *Scheduler) Fingerprint() uint64 { return s.hash }

// Checkpoints are the fingerprints recorded every CheckpointEvery steps.
func (s *Scheduler) Checkpoints() []uint64 { return s.checks }

// Trace folds values into the fingerprint.
func (s *Scheduler) Trace(vals ...uint64) {
	for _, v := range vals {
		s.hash = (s.hash ^ v) * 1099511628211
	}
}

// TraceString folds a string into the fingerprint.
func (s *Scheduler) TraceString(str string) {
	s.Trace(uint64(len(str)))
	for i := 0; i < len(str); i++ {
		s.Trace(uint64(str[i]))
	}
}

// Logf writes a verbose line, if enabled.
func (s *Scheduler) Logf(format string, args ...any) {
	if s.opts.Logf != nil {
		s.opts.Logf("%12v %s", s.now, fmt.Sprintf(format, args...))
	}
}

// Verbose reports whether Logf output goes anywhere.
func (s *Scheduler) Verbose() bool { return s.opts.Logf != nil }

// Fail ends the run with err (the first failure wins).
func (s *Scheduler) Fail(err error) {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	if s.failure == nil {
		s.failure = err
	}
}

// Err returns the run's failure, if any.
func (s *Scheduler) Err() error {
	s.failMu.Lock()
	defer s.failMu.Unlock()
	return s.failure
}

func (s *Scheduler) failed() bool { return s.Err() != nil }

// Spawn starts fn as a new task on machine m. It runs when the seed picks
// it; Spawn itself doesn't yield.
func (s *Scheduler) Spawn(m *Machine, name string, fn func()) *Task {
	return s.spawn(m, name, s.cur, fn)
}

// SpawnRoot is Spawn with no parent: a machine's boot, which is no task's
// work wherever it is started from.
func (s *Scheduler) SpawnRoot(m *Machine, name string, fn func()) *Task {
	return s.spawn(m, name, nil, fn)
}

func (s *Scheduler) spawn(m *Machine, name string, parent *Task, fn func()) *Task {
	t := &Task{ID: len(s.tasks), Name: name, Machine: m, Parent: parent, s: s, wake: make(chan struct{}), state: runnable}
	if s.pctRng != nil {
		t.prio = 1 + s.pctRng.Float64()
	}
	s.tasks = append(s.tasks, t)
	s.runnable = append(s.runnable, t)
	s.Logf("spawn %s", t)
	go t.main(fn)
	return t
}

func (t *Task) String() string {
	if t.Machine != nil && len(t.s.machines) > 1 {
		return fmt.Sprintf("%s/%s#%d", t.Machine.Name, t.Name, t.ID)
	}
	return fmt.Sprintf("%s#%d", t.Name, t.ID)
}

func (t *Task) main(fn func()) {
	<-t.wake
	t.goid = goid()
	register(t)
	defer func() {
		unregister(t)
		if r := recover(); r != nil {
			if _, ok := r.(killSignal); !ok {
				if t.Machine != nil && t.s.OnPanic != nil && t.s.OnPanic(t, r) {
					t.state = finished // this goroutine is ending already
					t.s.Logf("goroutine %s panicked, killing its process: %v", t, r)
					t.s.Kill(t.Machine)
				} else {
					t.s.Fail(&PanicError{Task: t.String(), Value: r, Stack: userStack()})
				}
			}
		}
		t.state = finished
		if t.s.opts.Pairs {
			t.s.lastSite = siteFinished
		}
		t.s.handOff()
	}()
	if t.killed {
		return
	}
	fn()
}

// PanicError is a panic in a simulated goroutine.
type PanicError struct {
	Task  string
	Value any
	Stack string
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("goroutine %s panicked: %v\n%s", e.Task, e.Value, e.Stack)
}

// Current returns the task holding the baton, for an operation it is
// about to make. It checks that the caller is that task's goroutine: a
// World operation from anywhere else (a raw go statement, an Always
// invariant, the test's own goroutine) fails the run with a MisuseError.
//
// The task must still be in the running state (an operation made on its
// behalf by another goroutine changes that), and the caller must be the
// task's goroutine: compared on every call where goid is cheap (amd64 and
// arm64: a few nanoseconds), on every 256th elsewhere (about 2µs and a
// process-wide lock there).
func (s *Scheduler) Current() *Task {
	t := s.cur
	if t == nil {
		s.misuse(nil)
	}
	if t.killed {
		panic(killSignal{})
	}
	s.calls++
	if t.state != running || goidCheap || s.calls%256 == 0 {
		if goid() != t.goid {
			s.misuse(t)
		}
		if t.state != running {
			// Our caller is the task, but another goroutine made an
			// operation on its behalf earlier in this step.
			s.misuse(t)
		}
	}
	return t
}

// handOff gives the baton back to the loop (unless the run was aborted).
func (s *Scheduler) handOff() {
	if s.shuttingDown {
		// Shutdown is waiting for this killed task: hand back even if the
		// run was aborted (a misuse between steps closes abort, and the
		// select below would then pick either case).
		s.handoff <- struct{}{}
		return
	}
	select {
	case s.handoff <- struct{}{}:
	case <-s.abort:
	}
}

// MisuseError reports a World operation made from a goroutine the World
// didn't start, or from an invariant run between steps.
type MisuseError struct {
	msg   string
	Stack string
}

func (e *MisuseError) Error() string { return e.msg + "\n" + e.Stack }

// misuse reports a World operation from the wrong goroutine. holder is
// the task holding the baton (nil between steps). It doesn't return.
func (s *Scheduler) misuse(holder *Task) {
	stack := callerStack()
	if holder == nil && goid() == s.loopGoid {
		// An invariant (or another callback run between steps) used the
		// World; afterStep turns this into the run's failure.
		panic(&MisuseError{msg: "an invariant (Always) or a callback run between steps used the World: it may only read state", Stack: stack})
	}
	msg := "a World operation was made from a goroutine this World didn't start (a raw go statement, or the test's own goroutine?)"
	if holder != nil {
		msg += fmt.Sprintf(" while %s was running", holder)
	}
	s.Fail(&MisuseError{msg: msg, Stack: stack})
	s.abortOnce.Do(func() { close(s.abort) })
	if holder != nil && goid() == holder.goid {
		// The real task noticed; the foreign goroutine already ran. End
		// this step cleanly: the run stops at the next handoff.
		panic(killSignal{})
	}
	runtime.Goexit() // a foreign goroutine: stop it before it touches the run
}

// Stepping reports whether a task holds the baton now (mid-step).
func (s *Scheduler) Stepping() bool { return s.cur != nil }

// InTask reports whether a task holds the baton (false inside timer
// callbacks).
func (s *Scheduler) InTask() bool { return s.cur != nil }

// Park hands the baton back until Ready(t) is called. why describes the
// wait, for deadlock reports.
func (s *Scheduler) Park(why string) {
	t := s.Current()
	t.state = parked
	t.blocked = why
	s.noteSite(t)
	s.handOff()
	<-t.wake
	t.blocked = ""
	if t.killed {
		panic(killSignal{})
	}
}

// Yield is a scheduling point: the current task stays runnable, but the
// seed may run another first.
func (s *Scheduler) Yield() {
	t := s.Current()
	t.state = runnable
	s.runnable = append(s.runnable, t)
	s.noteSite(t)
	s.handOff()
	<-t.wake
	if t.killed {
		panic(killSignal{})
	}
}

// YieldToOthers is World.Yield: the current task waits until another task
// has taken a step (one that didn't end in YieldToOthers) or a timer has
// fired, then becomes runnable again. A loop spinning on it therefore
// lets the task it waits for run, lets time advance while that task
// sleeps, and, when nothing else can ever happen, is a deadlock rather
// than a loop that never ends.
func (s *Scheduler) YieldToOthers() {
	t := s.Current()
	t.state = parked
	t.blocked = "Yield (spinning: waiting for another goroutine to make progress)"
	s.spinners = append(s.spinners, t)
	s.spinStep = true
	s.noteSite(t)
	s.handOff()
	<-t.wake
	t.blocked = ""
	if t.killed {
		panic(killSignal{})
	}
}

// wakeSpinners makes every task waiting in YieldToOthers runnable.
func (s *Scheduler) wakeSpinners() {
	if len(s.spinners) == 0 {
		return
	}
	for _, t := range s.spinners {
		s.Ready(t)
	}
	s.spinners = s.spinners[:0]
}

// Ready makes a parked task runnable. It does nothing for a task that isn't
// parked, is killed, or whose machine is dead.
func (s *Scheduler) Ready(t *Task) {
	if t.state != parked || t.killed || t.Machine.dead {
		return
	}
	t.state = runnable
	s.runnable = append(s.runnable, t)
}

// ParkOn parks the current task on a wait queue keyed by key (usually a
// pointer to a mutex or wait group).
func (s *Scheduler) ParkOn(key any, why string) {
	s.queues[key] = append(s.queues[key], s.Current())
	s.Park(why)
}

// WakeOne readies the longest-waiting live task parked on key.
func (s *Scheduler) WakeOne(key any) bool {
	q := s.queues[key]
	for len(q) > 0 {
		t := q[0]
		q = q[1:]
		if t.state == parked && !t.killed && !t.Machine.dead {
			s.setQueue(key, q)
			s.Ready(t)
			return true
		}
	}
	s.setQueue(key, q)
	return false
}

// WakeAll readies every task parked on key.
func (s *Scheduler) WakeAll(key any) {
	for s.WakeOne(key) {
	}
}

func (s *Scheduler) setQueue(key any, q []*Task) {
	if len(q) == 0 {
		delete(s.queues, key)
	} else {
		s.queues[key] = q
	}
}

// Timer is a pending callback.
type Timer struct{ e *event }

// Stop cancels the callback. It reports whether it was still pending.
func (t Timer) Stop() bool {
	if t.e == nil || t.e.fn == nil {
		return false
	}
	t.e.fn = nil
	return true
}

// At runs fn on the scheduler (not in a task) at virtual time when. fn may
// ready tasks and use the non-blocking channel operations, nothing else.
func (s *Scheduler) At(when time.Duration, fn func()) Timer {
	if when < s.now {
		when = s.now
	}
	s.seq++
	e := &event{at: when, seq: s.seq, fn: fn}
	heap.Push(&s.events, e)
	return Timer{e}
}

// Sleep parks the current task for d of virtual time.
func (s *Scheduler) Sleep(d time.Duration) {
	t := s.Current()
	if d <= 0 {
		s.Yield()
		return
	}
	s.At(s.now+d, func() { s.Ready(t) })
	s.Park(fmt.Sprintf("sleep until %v", s.now+d))
}

// Crash kills every task of m: they never run again, their deferred
// functions don't run (a process that loses power runs no defers), and
// the machine's OnCrash hooks run. If the current task is on m, Crash
// doesn't return.
func (s *Scheduler) Crash(m *Machine) { s.crash(m, false) }

// Kill is Crash for the process alone (kill -9, a panic, the OOM killer):
// its tasks die the same way, and the crash hooks run with m.Killed() true
// (the disk keeps what was written, unsynced or not).
func (s *Scheduler) Kill(m *Machine) { s.crash(m, true) }

func (s *Scheduler) crash(m *Machine, kill bool) {
	if m.dead {
		return
	}
	m.dead, m.killed = true, kill
	m.down = open(m.down, s.now)
	if m.paused { // a crash ends a pause (and Restart starts unpaused)
		m.paused = false
		s.pausedN--
		closeLast(m.pause, s.now)
	}
	if kill {
		s.Logf("kill %s (its host stays up)", m.Name)
		s.Trace(0x4b11, uint64(m.ID))
	} else {
		s.Logf("crash %s", m.Name)
		s.Trace(0xc7a5, uint64(m.ID))
	}
	s.runnable = slices.DeleteFunc(s.runnable, func(t *Task) bool { return t.Machine == m })
	for _, t := range s.tasks {
		if t.Machine == m && t.state != finished {
			t.killed = true
		}
	}
	for _, fn := range m.onCrash {
		fn()
	}
	if s.cur != nil && s.cur.Machine == m && s.cur.state != finished {
		// Stop here without unwinding; Shutdown releases the goroutine.
		// (A finished task is a panic killing its own process: it
		// returns to its deferred cleanup.)
		t := s.cur
		t.state = parked
		t.blocked = "crashed"
		s.handOff()
		<-t.wake
		panic(killSignal{})
	}
}

// Restart brings a crashed machine back with no tasks; the caller starts
// its new ones.
func (s *Scheduler) Restart(m *Machine) {
	if !m.dead {
		return
	}
	m.dead = false
	closeLast(m.down, s.now)
	m.Incarnation++
	s.Logf("restart %s (incarnation %d)", m.Name, m.Incarnation)
	s.Trace(0x2e57, uint64(m.ID))
}

// Pause stops a machine without crashing it (SIGSTOP, a long GC pause, a
// VM stall): its goroutines don't run until Resume, so its timers fire
// late, while its clock, its disk and the network carry on (data sent to
// it waits in its socket buffers).
func (s *Scheduler) Pause(m *Machine) {
	if m.dead || m.paused {
		return
	}
	m.paused = true
	s.pausedN++
	m.pause = open(m.pause, s.now)
	s.Logf("pause %s", m.Name)
	s.Trace(0x9a05e, uint64(m.ID))
}

// Resume lets a paused machine run again.
func (s *Scheduler) Resume(m *Machine) {
	if !m.paused {
		return
	}
	m.paused = false
	s.pausedN--
	closeLast(m.pause, s.now)
	s.Logf("resume %s", m.Name)
	s.Trace(0x7e50e, uint64(m.ID))
}

// Run runs tasks until done reports true, a task fails, or the run
// deadlocks. done is checked between steps.
func (s *Scheduler) Run(done func() bool) error {
	wd := time.NewTimer(time.Hour)
	defer wd.Stop()
	s.loopGoid = goid()
	for !s.failed() && !done() {
		if s.opts.MaxSteps > 0 && s.steps >= s.opts.MaxSteps {
			s.Fail(&LimitError{fmt.Sprintf("the run didn't finish within %d steps (t=%v)", s.opts.MaxSteps, s.now)})
			break
		}
		if s.forked < len(s.opts.Forks) {
			if f := s.opts.Forks[s.forked]; s.forkNow || (f.Marker == "" && s.steps >= f.Step) {
				s.fork(f.Seed)
			}
		}
		if t := s.pick(); t != nil {
			s.Trace(uint64(t.ID))
			s.steps++
			if s.opts.Pairs {
				if t.site == 0 {
					t.site = startSite(t.Name)
				}
				if p := pairKey(s.lastSite, t.site); s.pairs[p] == 0 {
					s.pairs[p] = s.steps
				}
			}
			if s.steps%s.opts.CheckpointEvery == 0 {
				s.checks = append(s.checks, s.hash)
			}
			t.state = running
			s.cur = t
			select {
			case t.wake <- struct{}{}:
			case <-s.abort:
				s.stopped, s.cur = true, nil
				return s.Err()
			}
			wd.Reset(s.opts.Watchdog)
			select {
			case <-s.abort:
				s.stopped, s.cur = true, nil
				return s.Err()
			case <-s.handoff:
			case <-wd.C:
				s.Fail(fmt.Errorf("goroutine %s kept running for %v of real time without reaching a World operation: it is probably blocked outside the World (a raw channel, sync primitive, time.Sleep, or I/O) or looping forever", t, s.opts.Watchdog))
				s.stopped = true // it may still hold the baton; Shutdown must not wait for it
				s.cur = nil
				return s.Err()
			}
			wd.Stop()
			s.cur = nil
			if !s.spinStep {
				s.wakeSpinners()
			}
			s.spinStep = false
			s.afterStep()
			continue
		}
		if s.events.Len() > 0 {
			e := heap.Pop(&s.events).(*event)
			if e.fn == nil {
				continue
			}
			if s.opts.MaxTime > 0 && e.at > s.opts.MaxTime {
				s.Fail(&LimitError{fmt.Sprintf("the run didn't finish within %v of simulated time", s.opts.MaxTime)})
				break
			}
			s.now = e.at
			s.Trace(uint64(e.at))
			fn := e.fn
			e.fn = nil
			fn()
			s.wakeSpinners()
			s.afterStep()
			continue
		}
		s.Fail(s.deadlock())
	}
	return s.Err()
}

func (s *Scheduler) afterStep() {
	if s.opts.AfterStep == nil || s.failed() {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if m, ok := r.(*MisuseError); ok {
				s.Fail(m) // an invariant used the World
				return
			}
			panic(r)
		}
	}()
	if err := s.opts.AfterStep(); err != nil {
		s.Fail(err)
	}
}

func (s *Scheduler) deadlock() error {
	var b strings.Builder
	fmt.Fprintf(&b, "deadlock at t=%v: no goroutine can run and no timer is pending", s.now)
	for _, t := range s.tasks {
		if t.state != finished && !t.killed {
			why := t.blocked
			if t.Machine.paused {
				why = "paused machine (never resumed)"
			}
			fmt.Fprintf(&b, "\n  %s: %s", t, why)
		}
	}
	return &DeadlockError{msg: b.String()}
}

// LimitError reports a run that went on past its time or step limit.
type LimitError struct{ msg string }

func (e *LimitError) Error() string { return e.msg }

// DeadlockError reports a deadlock.
type DeadlockError struct{ msg string }

func (e *DeadlockError) Error() string { return e.msg }

// Shutdown ends every task that hasn't finished, so no goroutine outlives
// the run. Killed tasks unwind with a panic that World operations can't
// catch; their deferred functions run, and any World operation they make
// does nothing.
func (s *Scheduler) Shutdown() {
	if s.stopped {
		return // a task escaped the World; we can't safely wake anything
	}
	s.stopped = true
	s.shuttingDown = true // read by the woken task after the wake send
	for _, t := range s.tasks {
		if t.state == finished {
			continue
		}
		t.killed = true
		if t.state == running {
			continue
		}
		s.cur = t
		t.wake <- struct{}{}
		<-s.handoff
		s.cur = nil
	}
}

type event struct {
	at  time.Duration
	seq uint64
	fn  func()
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

// Done reports whether the task has returned.
func (t *Task) Done() bool { return t.state == finished }

// Choose returns a seeded choice in [0, n), for nondeterminism the World
// resolves (which ready select arm runs).
func (s *Scheduler) Choose(n int) int { return s.rng.IntN(n) }

// Perm returns a seeded permutation of [0, n).
func (s *Scheduler) Perm(n int) []int { return s.rng.Perm(n) }

// Blocked describes what a parked task waits for ("" if not parked).
func (t *Task) Blocked() string {
	if t.state != parked {
		return ""
	}
	return t.blocked
}

// Tasks returns every task spawned so far, in spawn order.
func (s *Scheduler) Tasks() []*Task { return s.tasks }

// DescendsFrom reports whether t is a or was spawned, transitively, by a.
func (t *Task) DescendsFrom(a *Task) bool {
	for ; t != nil; t = t.Parent {
		if t == a {
			return true
		}
	}
	return false
}
