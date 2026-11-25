package dst

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"time"
)

// Sim is the simulated World. Every World goroutine is a real goroutine,
// but only one runs at a time: it holds the baton until it reaches a World
// operation, where it hands the baton back and the seed picks who runs
// next. When nobody can run, virtual time jumps to the next timer.
type Sim struct {
	rng      *rand.Rand // scheduling decisions
	userRand *rand.Rand // what code gets from w.Rand()
	epoch    time.Time
	now      time.Duration
	events   eventHeap
	seq      uint64
	tasks    []*task
	runnable []*task
	cur      *task
	sched    chan struct{} // a task hands the baton back
	hash     uint64
	steps    uint64
	failure  error
	nextChan int
	watchdog time.Duration
}

type task struct {
	id      int
	name    string
	wake    chan struct{}
	done    bool
	killed  bool
	parked  bool
	blocked string
}

type killSignal struct{}

// Result is one simulated run.
type Result struct {
	Seed        uint64
	Err         error
	Fingerprint uint64        // hash of every scheduling decision and timer
	Steps       uint64        // baton handoffs
	SimTime     time.Duration // virtual time elapsed
}

// Run runs main as the first World goroutine under the seed, until main
// returns (like a Go program), a goroutine panics, or everything deadlocks.
func Run(seed uint64, main func(w World)) Result {
	s := &Sim{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		epoch:    time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		sched:    make(chan struct{}),
		hash:     14695981039346656037,
		watchdog: 2 * time.Second,
	}
	s.userRand = rand.New(rand.NewPCG(s.rng.Uint64(), s.rng.Uint64()))
	m := s.spawn("main", func() { main(s) })
	s.loop(m)
	s.killAll()
	return Result{Seed: seed, Err: s.failure, Fingerprint: s.hash, Steps: s.steps, SimTime: s.now}
}

func (s *Sim) mix(v uint64) { s.hash = (s.hash ^ v) * 1099511628211 }

func (s *Sim) spawn(name string, fn func()) *task {
	t := &task{id: len(s.tasks), name: name, wake: make(chan struct{}), parked: true}
	s.tasks = append(s.tasks, t)
	s.runnable = append(s.runnable, t)
	go func() {
		<-t.wake
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(killSignal); !ok && s.failure == nil {
					s.failure = fmt.Errorf("goroutine %q panicked at t=%v: %v\n%s", t.name, s.now, r, debug.Stack())
				}
			}
			t.done = true
			s.sched <- struct{}{}
		}()
		t.parked = false
		if t.killed {
			return
		}
		fn()
	}()
	return t
}

func (s *Sim) loop(main *task) {
	wd := time.NewTimer(time.Hour)
	defer wd.Stop()
	for s.failure == nil && !main.done {
		if n := len(s.runnable); n > 0 {
			i := s.rng.IntN(n)
			t := s.runnable[i]
			s.runnable = append(s.runnable[:i], s.runnable[i+1:]...)
			s.mix(uint64(t.id))
			s.steps++
			s.cur = t
			t.wake <- struct{}{}
			wd.Reset(s.watchdog)
			select {
			case <-s.sched:
			case <-wd.C:
				s.failure = fmt.Errorf("goroutine %q kept the baton for %v: it is probably blocked outside the World (raw channel, mutex, time.Sleep or I/O)", t.name, s.watchdog)
				return
			}
			wd.Stop()
			s.cur = nil
			continue
		}
		if s.events.Len() > 0 {
			e := heap.Pop(&s.events).(*event)
			s.now = e.at
			s.mix(uint64(e.at))
			e.fn()
			continue
		}
		var b []string
		for _, t := range s.tasks {
			if !t.done {
				b = append(b, fmt.Sprintf("%s: %s", t.name, t.blocked))
			}
		}
		s.failure = fmt.Errorf("deadlock at t=%v; every goroutine is blocked:\n  %s", s.now, strings.Join(b, "\n  "))
	}
}

// killAll ends every goroutine still parked, so nothing leaks past Run.
func (s *Sim) killAll() {
	for _, t := range s.tasks {
		if !t.done && t.parked {
			t.killed = true
			t.wake <- struct{}{}
			<-s.sched
		}
	}
}

func (s *Sim) current() *task {
	if s.cur == nil {
		panic("dst: World used outside a goroutine started by the World")
	}
	return s.cur
}

// park hands the baton back until someone makes t runnable again.
func (s *Sim) park(t *task, why string) {
	if t.killed {
		panic(killSignal{})
	}
	t.blocked, t.parked = why, true
	s.sched <- struct{}{}
	<-t.wake
	t.blocked, t.parked = "", false
	if t.killed {
		panic(killSignal{})
	}
}

// yield is a scheduling point: t stays runnable, but the seed may run
// someone else first.
func (s *Sim) yield(t *task) {
	s.runnable = append(s.runnable, t)
	s.park(t, "runnable")
}

func (s *Sim) at(when time.Duration, fn func()) {
	s.seq++
	heap.Push(&s.events, &event{at: when, seq: s.seq, fn: fn})
}

func (s *Sim) Go(name string, fn func()) {
	t := s.current()
	s.spawn(name, fn)
	s.yield(t)
}

func (s *Sim) Sleep(d time.Duration) {
	t := s.current()
	if d <= 0 {
		s.yield(t)
		return
	}
	s.at(s.now+d, func() { s.runnable = append(s.runnable, t) })
	s.park(t, fmt.Sprintf("sleep until t=%v", s.now+d))
}

func (s *Sim) Now() time.Time { return s.epoch.Add(s.now) }
func (s *Sim) Rand() Rand     { return s.userRand }

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
