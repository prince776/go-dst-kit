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
	"math/rand/v2"
	"slices"
	"strings"
	"time"
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
}

// Scheduler runs tasks one at a time in an order decided by the seed.
type Scheduler struct {
	opts     Options
	rng      *rand.Rand
	now      time.Duration
	events   eventHeap
	seq      uint64
	tasks    []*Task
	runnable []*Task
	cur      *Task
	handoff  chan struct{}
	hash     uint64
	steps    uint64
	checks   []uint64
	failure  error
	machines []*Machine
	queues   map[any][]*Task
	ids      int
	stopped  bool
}

// Machine is a group of tasks that live and die together: one process.
type Machine struct {
	ID   int
	Name string
	// Incarnation counts restarts.
	Incarnation int
	dead        bool
	s           *Scheduler
	onCrash     []func()
}

// Dead reports whether the machine is crashed (and not yet restarted).
func (m *Machine) Dead() bool { return m.dead }

// Alive reports whether incarnation inc of the machine is still running:
// timers and callbacks created by an earlier incarnation must not fire.
func (m *Machine) Alive(inc int) bool { return !m.dead && m.Incarnation == inc }

// OnCrash registers fn to run when the machine crashes (e.g. its disk loses
// unsynced data). Hooks run in registration order.
func (m *Machine) OnCrash(fn func()) { m.onCrash = append(m.onCrash, fn) }

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
	goid    int64
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
		hash:    14695981039346656037,
		queues:  map[any][]*Task{},
	}
	s.rng = s.Rand("sched")
	return s
}

// Rand returns a generator for one named consumer, derived from the seed
// and the label only. Adding a consumer never changes another's stream.
func (s *Scheduler) Rand(label string) *rand.Rand {
	h := fnv.New64a()
	h.Write([]byte(label))
	k := h.Sum64()
	return rand.New(rand.NewPCG(s.opts.Seed^k, s.opts.Seed*0x9e3779b97f4a7c15+k))
}

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
	if s.failure == nil {
		s.failure = err
	}
}

// Err returns the run's failure, if any.
func (s *Scheduler) Err() error { return s.failure }

// Spawn starts fn as a new task on machine m. It runs when the seed picks
// it; Spawn itself doesn't yield.
func (s *Scheduler) Spawn(m *Machine, name string, fn func()) *Task {
	t := &Task{ID: len(s.tasks), Name: name, Machine: m, Parent: s.cur, s: s, wake: make(chan struct{}), state: runnable}
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
				t.s.Fail(&PanicError{Task: t.String(), Value: r, Stack: userStack()})
			}
		}
		t.state = finished
		t.s.handoff <- struct{}{}
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

// Current returns the task holding the baton. It panics if called from
// outside a task.
func (s *Scheduler) Current() *Task {
	t := s.cur
	if t == nil {
		panic("dst: World used outside a goroutine started by the World")
	}
	if t.killed {
		panic(killSignal{})
	}
	return t
}

// InTask reports whether a task holds the baton (false inside timer
// callbacks).
func (s *Scheduler) InTask() bool { return s.cur != nil }

// Park hands the baton back until Ready(t) is called. why describes the
// wait, for deadlock reports.
func (s *Scheduler) Park(why string) {
	t := s.Current()
	t.state = parked
	t.blocked = why
	s.handoff <- struct{}{}
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
	s.handoff <- struct{}{}
	<-t.wake
	if t.killed {
		panic(killSignal{})
	}
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
func (s *Scheduler) Crash(m *Machine) {
	if m.dead {
		return
	}
	m.dead = true
	s.Logf("crash %s", m.Name)
	s.Trace(0xc7a5, uint64(m.ID))
	s.runnable = slices.DeleteFunc(s.runnable, func(t *Task) bool { return t.Machine == m })
	for _, t := range s.tasks {
		if t.Machine == m && t.state != finished {
			t.killed = true
		}
	}
	for _, fn := range m.onCrash {
		fn()
	}
	if s.cur != nil && s.cur.Machine == m {
		// Stop here without unwinding; Shutdown releases the goroutine.
		t := s.cur
		t.state = parked
		t.blocked = "crashed"
		s.handoff <- struct{}{}
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
	m.Incarnation++
	s.Logf("restart %s (incarnation %d)", m.Name, m.Incarnation)
	s.Trace(0x2e57, uint64(m.ID))
}

// Run runs tasks until done reports true, a task fails, or the run
// deadlocks. done is checked between steps.
func (s *Scheduler) Run(done func() bool) error {
	wd := time.NewTimer(time.Hour)
	defer wd.Stop()
	for s.failure == nil && !done() {
		if s.opts.MaxSteps > 0 && s.steps >= s.opts.MaxSteps {
			s.Fail(&LimitError{fmt.Sprintf("the run didn't finish within %d steps (t=%v)", s.opts.MaxSteps, s.now)})
			break
		}
		if n := len(s.runnable); n > 0 {
			i := s.rng.IntN(n)
			t := s.runnable[i]
			s.runnable = slices.Delete(s.runnable, i, i+1)
			s.Trace(uint64(t.ID))
			s.steps++
			if s.steps%s.opts.CheckpointEvery == 0 {
				s.checks = append(s.checks, s.hash)
			}
			t.state = running
			s.cur = t
			t.wake <- struct{}{}
			wd.Reset(s.opts.Watchdog)
			select {
			case <-s.handoff:
			case <-wd.C:
				s.Fail(fmt.Errorf("goroutine %s kept running for %v of real time without reaching a World operation: it is probably blocked outside the World (a raw channel, sync primitive, time.Sleep, or I/O) or looping forever", t, s.opts.Watchdog))
				s.stopped = true // it may still hold the baton; Shutdown must not wait for it
				s.cur = nil
				return s.failure
			}
			wd.Stop()
			s.cur = nil
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
			s.afterStep()
			continue
		}
		s.Fail(s.deadlock())
	}
	return s.failure
}

func (s *Scheduler) afterStep() {
	if s.opts.AfterStep != nil && s.failure == nil {
		if err := s.opts.AfterStep(); err != nil {
			s.Fail(err)
		}
	}
}

func (s *Scheduler) deadlock() error {
	var b strings.Builder
	fmt.Fprintf(&b, "deadlock at t=%v: no goroutine can run and no timer is pending", s.now)
	for _, t := range s.tasks {
		if t.state != finished && !t.killed {
			fmt.Fprintf(&b, "\n  %s: %s", t, t.blocked)
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
