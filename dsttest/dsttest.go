// Package dsttest runs deterministic simulation tests: it sweeps seeds,
// and for each one builds a simulated World of machines, injects faults,
// checks invariants, and reports failures with a command that replays
// them exactly.
//
//	func TestStore(t *testing.T) {
//		dsttest.Run(t, dsttest.Options{Seeds: 500, Faults: dsttest.AllFaults}, func(h *dsttest.Harness) {
//			for _, name := range []string{"db1", "db2", "db3"} {
//				h.Machine(name, func(w *dst.World) { store.Serve(w, name) })
//			}
//			h.Go(func(w *dst.World) { workload(w, h) })     // clients, on a machine that never fails
//			h.Always("one leader", checkLeaders)            // after every step
//			h.Final("converged", checkConverged)            // after recovery
//		})
//	}
//
// The setup function runs once per seed, possibly in parallel with other
// seeds: keep every piece of state the run uses inside it.
//
// Each seed runs in phases. During the fault phase the runner crashes and
// restarts machines, partitions the network and lets disks lose unsynced
// data, at rates chosen per seed (swarm testing: some seeds turn whole
// fault classes off). During the recovery phase every machine is back and
// the network is whole: a correct system must recover. Then the Final
// checks run. A failing seed prints:
//
//	go test -run '^TestStore$' -dst.seed=2891 -dst.v
//
// which reruns exactly that seed with an event log.
//
// Flags: -dst.seed=N (one seed), -dst.seeds=N, -dst.v (event log),
// -dst.parallel=N.
//
// Verified:
//   - [failures-replay] A failing seed fails again, identically, when
//     replayed.
//   - [nondeterminism-reported] Code whose behaviour isn't decided by the
//     seed is reported as nondeterministic, with the step where runs
//     diverge.
//   - [always-checked] An Always invariant that breaks at any step fails
//     the seed at that step.
//   - [recovery-phase] After the fault phase, every machine is running and
//     the network is whole until the run ends.
//   - [expected-coverage] A sweep fails if an Expect-ed behaviour never
//     happened in any seed.
//   - [finds-planted-bugs] Known-bad systems (a WAL without fsync, a pool
//     that deadlocks) fail within the default number of seeds.
//   - [workload-finishes] A workload goroutine (h.Go) still running at the
//     end of the recovery phase fails the seed, naming what it waits for;
//     Final checks run only after the workload is done.
//
// Depends on: dst, internal/simworld
package dsttest

import (
	"errors"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simnet"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

var (
	flagSeed     = flag.Uint64("dst.seed", 0, "run only this seed")
	flagSeeds    = flag.Int("dst.seeds", 0, "number of seeds to sweep (overrides Options.Seeds)")
	flagVerbose  = flag.Bool("dst.v", false, "print the event log (with -dst.seed)")
	flagParallel = flag.Int("dst.parallel", 0, "seeds run at once (default GOMAXPROCS)")
)

// Faults selects the fault classes the runner may inject. Rates are drawn
// per seed.
type Faults struct {
	Crash     bool // crash and restart machines (power loss: unsynced data may be lost)
	Partition bool // partition the network, symmetrically or one way
	Disk      bool // torn writes and power loss inside I/O calls
	Network   bool // connection resets and latency spikes
}

// AllFaults enables every fault class.
var AllFaults = Faults{Crash: true, Partition: true, Disk: true, Network: true}

// Options configure a sweep.
type Options struct {
	// Seeds is the number of seeds to run (default 100; 10 with -short).
	Seeds int
	// FirstSeed is the first seed (default 1).
	FirstSeed uint64
	// Faults are the fault classes to inject (default none).
	Faults Faults
	// FaultPhase is how long faults are injected, in simulated time
	// (default 10s). RecoveryPhase follows, with no faults (default 10s).
	FaultPhase, RecoveryPhase time.Duration
	// MaxSteps bounds each run (default 10 million).
	MaxSteps uint64
	// DeterminismChecks is how many seeds (the first ones) are run twice to
	// check that the system is deterministic; failing seeds always are.
	// Default 5; -1 checks none.
	DeterminismChecks int
	// Parallel is how many seeds run at once (default GOMAXPROCS).
	Parallel int
	// KeepGoing reports every failing seed instead of stopping after the
	// first few.
	KeepGoing bool
}

func (o *Options) defaults() {
	if o.Seeds == 0 {
		o.Seeds = 100
		if testing.Testing() && testing.Short() {
			o.Seeds = 10
		}
	}
	if *flagSeeds > 0 {
		o.Seeds = *flagSeeds
	}
	if o.FirstSeed == 0 {
		o.FirstSeed = 1
	}
	if o.FaultPhase == 0 {
		o.FaultPhase = 10 * time.Second
	}
	if o.RecoveryPhase == 0 {
		o.RecoveryPhase = 10 * time.Second
	}
	if o.MaxSteps == 0 {
		o.MaxSteps = 10_000_000
	}
	if o.DeterminismChecks == 0 {
		o.DeterminismChecks = 5
	}
	if o.Parallel == 0 {
		o.Parallel = runtime.GOMAXPROCS(0)
	}
	if *flagParallel > 0 {
		o.Parallel = *flagParallel
	}
}

// Phase is where a run is.
type Phase int

const (
	Setup    Phase = iota // the setup function is running
	Faulty                // faults are being injected
	Recovery              // faults stopped; the system must recover
	Checking              // Final checks are running
)

func (p Phase) String() string { return [...]string{"setup", "faults", "recovery", "final checks"}[p] }

// Harness is what a test's setup function uses to describe the system and
// its checks. Its methods may be called only from the setup function and
// goroutines of the simulation.
type Harness struct {
	sim      *simworld.Sim
	test     *simworld.Machine
	opts     Options
	swarm    Swarm
	phase    Phase
	machines []*Machine
	always   []named[func() error]
	final    []named[func(w *dst.World) error]
	cover    map[string]int
	expect   []string
	workload []*sched.Task
}

type named[T any] struct {
	name string
	fn   T
}

// Machine is a simulated machine (a process) the runner may crash,
// restart and partition.
type Machine struct {
	m *simworld.Machine
	h *Harness
}

// Machine adds a machine running start (again after each restart), and
// boots it. Its network address is its name ("db1:8080").
func (h *Harness) Machine(name string, start func(w *dst.World)) *Machine {
	sm := h.sim.AddMachine(name, simworld.MachineOptions{Disk: h.swarm.Disk})
	sm.Disk.OnCrashLoss = func(_ int, torn bool) {
		h.Sometimes("disk lost unsynced writes")
		if torn {
			h.Sometimes("disk tore a write")
		}
	}
	sm.Start = start
	m := &Machine{m: sm, h: h}
	h.machines = append(h.machines, m)
	sm.Boot()
	return m
}

// Name is the machine's name (its network host).
func (m *Machine) Name() string { return m.m.Name }

// World is the machine's World.
func (m *Machine) World() *dst.World { return m.m.World }

// Dead reports whether the machine is crashed.
func (m *Machine) Dead() bool { return m.m.Dead() }

// Crash crashes the machine now.
func (m *Machine) Crash() {
	if !m.m.Dead() {
		m.h.Sometimes("machine crashed")
		m.m.Crash()
	}
}

// Restart restarts a crashed machine.
func (m *Machine) Restart() {
	if m.m.Dead() {
		m.m.Restart()
	}
}

// Machines lists the machines.
func (h *Harness) Machines() []*Machine { return h.machines }

// World is the World of the test machine, which never crashes and is
// where workload goroutines run.
func (h *Harness) World() *dst.World { return h.test.World }

// Go starts a workload goroutine on the test machine. Workload goroutines
// must return by the end of the recovery phase: one still running then
// fails the seed, with what it waits for, and the Final checks run only
// once the workload is done. That catches deadlocks and operations that
// never complete. Start goroutines meant to run forever with h.World().Go
// instead. A workload that would outlast the run should stop starting
// operations once h.Phase() leaves Faulty:
//
//	for range 100 {
//		if h.Phase() != dsttest.Faulty {
//			return
//		}
//		op(w)
//	}
func (h *Harness) Go(fn func(w *dst.World)) {
	w := h.test.World
	s := h.sim.S
	h.workload = append(h.workload, s.Spawn(h.test.M, fmt.Sprintf("workload%d", len(h.workload)+1), func() { fn(w) }))
	if s.InTask() {
		s.Yield()
	}
}

// Phase reports the current phase.
func (h *Harness) Phase() Phase { return h.phase }

// Seed is the run's seed.
func (h *Harness) Seed() uint64 { return h.sim.S.Seed() }

// Swarm is the fault mix chosen for this seed.
func (h *Harness) Swarm() Swarm { return h.swarm }

// Always registers an invariant checked after every scheduling step. It
// runs on the scheduler, between steps: it may read the system's state but
// must not use the World. Keep it cheap.
func (h *Harness) Always(name string, fn func() error) {
	h.always = append(h.always, named[func() error]{name, fn})
}

// Final registers a check that runs after the recovery phase, in a
// goroutine of the test machine (it may use the World: talk to the
// system, wait with deadlines).
func (h *Harness) Final(name string, fn func(w *dst.World) error) {
	h.final = append(h.final, named[func(w *dst.World) error]{name, fn})
}

// Sometimes records that a behaviour happened in this run (coverage).
func (h *Harness) Sometimes(name string) { h.cover[name]++ }

// Expect declares behaviours that must happen in at least one seed of the
// sweep; otherwise the sweep fails (the test never exercised them).
func (h *Harness) Expect(names ...string) { h.expect = append(h.expect, names...) }

// Fail fails the run with err (from workload goroutines that detect a
// violation).
func (h *Harness) Fail(err error) { h.sim.S.Fail(err) }

// Failf is Fail with a formatted error.
func (h *Harness) Failf(format string, args ...any) { h.Fail(fmt.Errorf(format, args...)) }

// Logf writes to the event log.
func (h *Harness) Logf(format string, args ...any) { h.sim.S.Logf(format, args...) }

// Partition cuts the network between two groups of machines (by name; the
// test machine is "test").
func (h *Harness) Partition(a, b []string) {
	h.Sometimes("network partitioned")
	h.sim.Net.Partition(a, b)
}

// Heal ends every partition.
func (h *Harness) Heal() { h.sim.Net.Heal() }

// ---- swarm ----

// Swarm is one seed's fault mix.
type Swarm struct {
	CrashEvery     time.Duration // mean time between crashes (0: none)
	Downtime       time.Duration // mean time a crashed machine stays down
	PartitionEvery time.Duration // mean time between partitions (0: none)
	PartitionFor   time.Duration // mean partition length
	OneWay         bool          // partitions may be one way
	Disk           simfs.Options
	Net            simnet.Options
}

func (s Swarm) String() string {
	return fmt.Sprintf("crash every ~%v (down ~%v), partition every ~%v (for ~%v, one-way %v), disk{ordered %v, torn %.2f, power-loss-in-io %.4f}, net{latency %v+%v, reset %.4f}",
		s.CrashEvery, s.Downtime, s.PartitionEvery, s.PartitionFor, s.OneWay,
		s.Disk.Ordered, s.Disk.TornWriteProb, s.Disk.IOCrashProb, s.Net.MinLatency, s.Net.MeanLatency, s.Net.ResetProb)
}

// sometimes returns v with probability 2/3 (swarm testing: whole fault
// classes are off for some seeds).
func sometimes[T any](r *rand.Rand, v T) T {
	var zero T
	if r.IntN(3) == 0 {
		return zero
	}
	return v
}

func newSwarm(seed uint64, f Faults) Swarm {
	r := rand.New(rand.NewPCG(seed, 0x5a4f5a4f))
	s := Swarm{
		Net:  simnet.Options{MinLatency: 50 * time.Microsecond, MeanLatency: time.Duration(100+r.IntN(2000)) * time.Microsecond},
		Disk: simfs.Options{SyncLatency: time.Duration(r.IntN(2000)) * time.Microsecond},
	}
	// Every draw happens whatever Faults says, so a seed means the same
	// mix when fault classes are switched on or off.
	crashEvery := sometimes(r, time.Duration(500+r.IntN(4000))*time.Millisecond)
	downtime := time.Duration(10+r.IntN(2000)) * time.Millisecond
	partEvery := sometimes(r, time.Duration(500+r.IntN(4000))*time.Millisecond)
	partFor := time.Duration(10+r.IntN(3000)) * time.Millisecond
	oneWay := r.IntN(2) == 0
	ordered := r.IntN(2) == 0
	torn := sometimes(r, r.Float64())
	ioCrash := sometimes(r, float64(r.IntN(20))/10000)
	reset := sometimes(r, float64(r.IntN(20))/10000)
	spike := r.IntN(4) == 0
	s.Disk.Ordered = ordered
	if f.Crash {
		s.CrashEvery, s.Downtime = crashEvery, downtime
	}
	if f.Partition {
		s.PartitionEvery, s.PartitionFor, s.OneWay = partEvery, partFor, oneWay
	}
	if f.Disk {
		s.Disk.TornWriteProb, s.Disk.IOCrashProb = torn, ioCrash
	}
	if f.Network {
		s.Net.ResetProb = reset
		if spike {
			s.Net.MeanLatency *= 20
		}
	}
	return s
}

// ---- running ----

// Result is one seed's run.
type Result struct {
	Seed        uint64
	Err         error
	Swarm       Swarm
	Fingerprint uint64
	Checkpoints []uint64
	Steps       uint64
	SimTime     time.Duration
	Coverage    map[string]int
	Expect      []string
}

// RunSeed runs one seed and returns its result. logf, if set, receives the
// event log.
func RunSeed(seed uint64, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	opts.defaults()
	swarm := newSwarm(seed, opts.Faults)
	h := &Harness{opts: opts, swarm: swarm, cover: map[string]int{}}
	h.sim = simworld.New(simworld.Options{
		Seed:     seed,
		Logf:     logf,
		MaxTime:  opts.FaultPhase + opts.RecoveryPhase + time.Hour,
		MaxSteps: opts.MaxSteps,
		Net:      swarm.Net,
		AfterStep: func() error {
			for _, a := range h.always {
				if err := a.fn(); err != nil {
					return &InvariantError{Name: a.name, Err: err}
				}
			}
			return nil
		},
	})
	h.test = h.sim.Machine("test")
	r := h.sim.RunOn(h.test, func(w *dst.World) {
		setup(h)
		h.phase = Faulty
		h.nemesis(w)
		h.phase = Recovery
		h.Heal()
		h.sim.Net.StopFaults()
		for _, m := range h.machines {
			m.m.Disk.StopFaults() // no more power loss inside I/O
			m.Restart()
		}
		w.Sleep(opts.RecoveryPhase)
		// The workload had the recovery phase to finish; Final checks then
		// look at a quiet end state.
		var stuck []string
		for _, t := range h.workload {
			if !t.Done() {
				stuck = append(stuck, fmt.Sprintf("%s: %s", t.Name, t.Blocked()))
			}
		}
		if len(stuck) > 0 {
			h.Fail(&StuckError{Goroutines: stuck})
			return
		}
		h.phase = Checking
		for _, f := range h.final {
			if err := f.fn(w); err != nil {
				h.Fail(&FinalError{Name: f.name, Err: err})
				return
			}
		}
	})
	return Result{Seed: seed, Err: r.Err, Swarm: swarm, Fingerprint: r.Fingerprint, Checkpoints: r.Checkpoints,
		Steps: r.Steps, SimTime: r.SimTime, Coverage: h.cover, Expect: h.expect}
}

// nemesis injects faults until the fault phase ends.
func (h *Harness) nemesis(w *dst.World) {
	end := w.Now().Add(h.opts.FaultPhase)
	r := h.sim.S.Rand("nemesis")
	sw := h.swarm
	exp := func(mean time.Duration) time.Duration {
		return time.Duration(r.ExpFloat64() * float64(mean))
	}
	type event struct {
		at time.Time
		fn func()
	}
	var events []event
	schedule := func(after time.Duration, fn func()) {
		events = append(events, event{w.Now().Add(after), fn})
	}
	var crash, partition func()
	crash = func() {
		alive := slices.DeleteFunc(slices.Clone(h.machines), func(m *Machine) bool { return m.Dead() })
		if len(alive) > 0 {
			m := alive[r.IntN(len(alive))]
			h.Logf("nemesis: crash %s", m.Name())
			m.Crash()
			schedule(exp(sw.Downtime), func() {
				h.Logf("nemesis: restart %s", m.Name())
				m.Restart()
			})
		}
		schedule(exp(sw.CrashEvery), crash)
	}
	partition = func() {
		names := []string{"test"}
		for _, m := range h.machines {
			names = append(names, m.Name())
		}
		r.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
		k := 1 + r.IntN(len(names)-1)
		a, b := names[:k], names[k:]
		if sw.OneWay && r.IntN(2) == 0 {
			h.Sometimes("network partitioned one way")
			for _, x := range a {
				for _, y := range b {
					h.sim.Net.Block(x, y)
				}
			}
		} else {
			h.Partition(a, b)
		}
		schedule(exp(sw.PartitionFor), func() { h.Heal() })
		schedule(exp(sw.PartitionEvery), partition)
	}
	if sw.CrashEvery > 0 && len(h.machines) > 0 {
		schedule(exp(sw.CrashEvery), crash)
	}
	if sw.PartitionEvery > 0 && len(h.machines) > 0 {
		schedule(exp(sw.PartitionEvery), partition)
	}
	for {
		next := end
		i := -1
		for j, e := range events {
			if e.at.Before(next) {
				next, i = e.at, j
			}
		}
		if d := next.Sub(w.Now()); d > 0 {
			w.Sleep(d)
		}
		if i < 0 {
			return
		}
		fn := events[i].fn
		events = slices.Delete(events, i, i+1)
		fn()
	}
}

// InvariantError is a broken Always invariant.
type InvariantError struct {
	Name string
	Err  error
}

func (e *InvariantError) Error() string { return fmt.Sprintf("invariant %q broken: %v", e.Name, e.Err) }
func (e *InvariantError) Unwrap() error { return e.Err }

// StuckError reports workload goroutines that never finished: a deadlock,
// or an operation that never completes even after the system recovered.
type StuckError struct{ Goroutines []string }

func (e *StuckError) Error() string {
	return "workload goroutines never finished (deadlock or lost liveness):\n  " + strings.Join(e.Goroutines, "\n  ")
}

// FinalError is a failed Final check.
type FinalError struct {
	Name string
	Err  error
}

func (e *FinalError) Error() string { return fmt.Sprintf("final check %q failed: %v", e.Name, e.Err) }
func (e *FinalError) Unwrap() error { return e.Err }

// Report is the outcome of a sweep.
type Report struct {
	Runs             int
	Failed           []Result
	Nondeterministic []Divergence
	Coverage         map[string]int
	NeverHappened    []string // Expect-ed behaviours no seed reached
	SimTime          time.Duration
	Steps            uint64
	Wall             time.Duration
}

// OK reports whether the sweep found nothing wrong.
func (r Report) OK() bool {
	return len(r.Failed) == 0 && len(r.Nondeterministic) == 0 && len(r.NeverHappened) == 0
}

// Divergence is a seed whose two runs differed.
type Divergence struct {
	Seed uint64
	// Step is roughly where the runs diverged (to the nearest checkpoint).
	Step uint64
	A, B string // each run's outcome
}

func (d Divergence) String() string {
	return fmt.Sprintf("seed %d is nondeterministic: runs diverged by step ~%d (%s vs %s). Something outside the World decides behaviour: a raw goroutine, time.Now, math/rand, map iteration order, or a pointer used as a key.", d.Seed, d.Step, d.A, d.B)
}

func outcome(r Result) string {
	if r.Err == nil {
		return "pass"
	}
	return "fail: " + strings.SplitN(r.Err.Error(), "\n", 2)[0]
}

func compare(a, b Result) *Divergence {
	if a.Fingerprint == b.Fingerprint && outcome(a) == outcome(b) {
		return nil
	}
	d := &Divergence{Seed: a.Seed, A: outcome(a), B: outcome(b)}
	n := min(len(a.Checkpoints), len(b.Checkpoints))
	for i := 0; i < n; i++ {
		if a.Checkpoints[i] != b.Checkpoints[i] {
			d.Step = uint64(i+1) * 1000
			return d
		}
	}
	d.Step = uint64(n) * 1000
	return d
}

// Sweep runs the seeds and returns the report.
func Sweep(opts Options, setup func(h *Harness)) Report {
	opts.defaults()
	start := time.Now()
	seeds := make([]uint64, opts.Seeds)
	for i := range seeds {
		seeds[i] = opts.FirstSeed + uint64(i)
	}
	results := make([]Result, len(seeds))
	divs := make([]*Divergence, len(seeds))
	var mu sync.Mutex
	stop := false
	next := 0
	var wg sync.WaitGroup
	for range opts.Parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next >= len(seeds) || stop {
					mu.Unlock()
					return
				}
				i := next
				next++
				mu.Unlock()
				r := RunSeed(seeds[i], opts, setup, nil)
				if r.Err != nil || i < opts.DeterminismChecks {
					divs[i] = compare(r, RunSeed(seeds[i], opts, setup, nil))
				}
				results[i] = r
				if r.Err != nil && !opts.KeepGoing {
					mu.Lock()
					stop = true
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	rep := Report{Coverage: map[string]int{}}
	expect := map[string]bool{}
	for i, r := range results {
		if r.Seed == 0 {
			continue // not run (stopped early)
		}
		rep.Runs++
		rep.SimTime += r.SimTime
		rep.Steps += r.Steps
		for k, v := range r.Coverage {
			rep.Coverage[k] += v
		}
		for _, e := range r.Expect {
			expect[e] = true
		}
		if r.Err != nil {
			rep.Failed = append(rep.Failed, r)
		}
		if divs[i] != nil {
			rep.Nondeterministic = append(rep.Nondeterministic, *divs[i])
		}
	}
	if len(rep.Failed) == 0 {
		for _, e := range slices.Sorted(maps.Keys(expect)) {
			if rep.Coverage[e] == 0 {
				rep.NeverHappened = append(rep.NeverHappened, e)
			}
		}
	}
	rep.Wall = time.Since(start)
	return rep
}

// Run sweeps seeds as a test, failing t for failing seeds, nondeterminism
// and behaviours that never happened. With -dst.seed it runs one seed
// (with -dst.v, printing its event log).
func Run(t testing.TB, opts Options, setup func(h *Harness)) Report {
	t.Helper()
	if *flagSeed != 0 {
		var logf func(string, ...any)
		if *flagVerbose {
			logf = func(format string, args ...any) { fmt.Fprintf(os.Stdout, format+"\n", args...) }
		}
		r := RunSeed(*flagSeed, opts, setup, logf)
		t.Logf("seed %d: %s; %v simulated in %d steps\nfaults: %v", r.Seed, outcome(r), r.SimTime, r.Steps, r.Swarm)
		if r.Err != nil {
			t.Fatalf("seed %d: %v", r.Seed, r.Err)
		}
		return Report{Runs: 1, Coverage: r.Coverage}
	}
	rep := Sweep(opts, setup)
	t.Logf("%d seeds: %v simulated in %v (%.0fx), %d steps", rep.Runs, rep.SimTime.Round(time.Millisecond), rep.Wall.Round(time.Millisecond), float64(rep.SimTime)/float64(max(rep.Wall, 1)), rep.Steps)
	if len(rep.Coverage) > 0 {
		var b strings.Builder
		for _, k := range slices.Sorted(maps.Keys(rep.Coverage)) {
			fmt.Fprintf(&b, "\n  %-40s %d", k, rep.Coverage[k])
		}
		t.Logf("coverage (times, summed over seeds):%s", b.String())
	}
	name := t.Name()
	if i := strings.Index(name, "/"); i >= 0 {
		name = name[:i]
	}
	for i, r := range rep.Failed {
		if i == 3 && !opts.KeepGoing {
			t.Errorf("... and more failing seeds (use KeepGoing to list them all)")
			break
		}
		t.Errorf("seed %d failed: %v\nfaults: %v\nreplay: go test -run '^%s$' -dst.seed=%d -dst.v", r.Seed, r.Err, r.Swarm, name, r.Seed)
	}
	for _, d := range rep.Nondeterministic {
		t.Error(d)
	}
	for _, e := range rep.NeverHappened {
		t.Errorf("no seed ever reached %q: the sweep never tested it", e)
	}
	return rep
}

// IsDeadlock reports whether err is a deadlock report: every goroutine
// blocked, or workload goroutines that never finished.
func IsDeadlock(err error) bool {
	var d *sched.DeadlockError
	var s *StuckError
	return errors.As(err, &d) || errors.As(err, &s)
}
