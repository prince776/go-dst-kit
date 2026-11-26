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
// restarts machines, pauses them, partitions the network and lets disks
// lose unsynced data, at rates chosen per seed (swarm testing: some seeds turn whole
// fault classes off). During the recovery phase every machine is back and
// the network is whole: a correct system must recover. Then the Final
// checks run. A failing seed prints:
//
//	go test -run '^TestStore$' -dst.seed=2891 -dst.v
//
// which reruns exactly that seed with an event log.
//
// Flags: -dst.seed=N (one seed), -dst.seeds=N, -dst.first=N|random (where
// the sweep starts; nightly runs use random), -dst.v (event log),
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
//   - [workload-finishes] Workload goroutines still running when recovery
//     ends fail the seed. That covers h.Go goroutines and those they
//     started; the report names what each waits for, and Final checks run
//     only after the workload is done.
//   - [slow-is-not-stuck] Unfinished workload is reported as slow if it
//     finishes during one more fault-free recovery phase, else stuck.
//   - [never-crash] The runner never crashes a NeverCrash machine.
//   - [fresh-seeds] -dst.first=random starts each sweep from a new seed,
//     and -dst.first=N from N.
//   - [skew-explores] Machine clocks stay within the Clock bounds; fast,
//     slow and at-bound clocks all occur.
//   - [pauses-explore] With Pause faults the runner pauses machines and
//     resumes every one by the recovery phase.
//   - [points-only-in-fault-phase] Fault points fail and Buggify points
//     fire only during the fault phase, and never with Faults.Points and
//     Faults.Services off.
//   - [outages-explore] With Services faults the runner takes whole
//     services down: every point of the service fails together.
//   - [fault-classes-independent] Turning on pauses, outages or fault
//     points leaves the times of each seed's crashes and partitions as
//     they were.
//
// Depends on: dst, internal/simworld
package dsttest

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/points"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simnet"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

var (
	flagSeed     = flag.Uint64("dst.seed", 0, "run only this seed")
	flagSeeds    = flag.Int("dst.seeds", 0, "number of seeds to sweep (overrides Options.Seeds)")
	flagFirst    = flag.String("dst.first", "", `first seed of the sweep: a number, or "random" for a fresh one (printed)`)
	flagVerbose  = flag.Bool("dst.v", false, "print the event log (with -dst.seed)")
	flagParallel = flag.Int("dst.parallel", 0, "seeds run at once (default GOMAXPROCS)")
	flagKeep     = flag.Bool("dst.keepgoing", false, "sweep every seed, not stopping at the first failures (Options.KeepGoing)")
	flagJSON     = flag.String("dst.json", "", "append one JSON line per seed run to this file (for tools such as dstbench)")
	flagXProcess = flag.Bool("dst.xprocess", true, "rerun a seed in a new process of the test binary and check it replays the same")
)

// childEnv marks a test binary run by the cross-process replay check.
const childEnv = "DSTTEST_REPLAY_CHILD"

// Faults selects the fault classes the runner may inject. Rates are drawn
// per seed.
type Faults struct {
	Crash     bool // crash and restart machines (power loss: unsynced data may be lost)
	Partition bool // partition the network, symmetrically or one way
	Disk      bool // torn writes and power loss inside I/O calls
	Network   bool // connection resets, latency spikes, datagram loss and duplication
	Pause     bool // pause machines without crashing them (GC pauses, VM stalls)
	Points    bool // fault points (dst.Inject) and Buggify points fire
	Services  bool // service outages: every point of a service fails for a while
}

// AllFaults enables every fault class.
var AllFaults = Faults{Crash: true, Partition: true, Disk: true, Network: true, Pause: true, Points: true, Services: true}

// Point sets a fault point's behaviour for every seed, replacing the
// per-seed draw (see Options.Points). The zero Point never fires.
type Point struct {
	Fail      float64       // chance a call fails without the operation happening
	FailAfter float64       // chance a call fails after the operation happened
	Slow      float64       // chance a call is slowed
	Latency   time.Duration // mean added latency of a slowed call
	Fire      float64       // for Buggify points: chance a call returns true
}

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
	// Clock bounds the machines' clocks (see ClockOptions).
	Clock ClockOptions
	// Points overrides fault points by name prefix, the longest matching
	// prefix winning: {"objstore.": {}} silences the objstore points,
	// {"objstore.put": {FailAfter: 0.1}} makes 10% of puts lose their
	// reply. Points no prefix matches keep their per-seed draw: quiet in a
	// third of seeds, else failing 0.1% to 20% of calls (a random share of
	// those after acting), sometimes slowed by 1ms to 1s.
	Points map[string]Point
	// Features pins workload features (Harness.Feature) on or off for
	// every seed; the others are drawn per seed.
	Features map[string]bool
	// Start pins what clocks read at the start of every run (default:
	// drawn per seed, see Swarm.Start).
	Start time.Time
	// StrictIdentity checks on every scheduling call that the World is
	// used only from goroutines it started (about 1.8µs per call). Without
	// it, the determinism-checked seeds and replays are checked strictly
	// and the rest by sampling: a leftover raw goroutine misuses the World
	// in nearly every seed, so a few strict seeds catch it.
	StrictIdentity bool
}

// ClockOptions bound the machines' clocks. Each machine's clock starts
// within MaxOffset of true time and runs at a rate within MaxDrift of 1,
// drawn per machine and seed (a quarter of the draws sit exactly at a
// bound); it keeps going across restarts. The test machine, and with it
// every invariant, uses true time.
//
// The defaults are close to NTP-synced servers: 10ms and 100ppm. Set
// MaxDrift to the bound your protocol assumes to test right up to it.
type ClockOptions struct {
	MaxOffset    time.Duration
	MaxDrift     float64
	Synchronized bool // every machine reads true time
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
	if *flagFirst != "" {
		o.FirstSeed = firstSeedFlag()
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
	if o.Clock.MaxOffset == 0 {
		o.Clock.MaxOffset = 10 * time.Millisecond
	}
	if o.Clock.MaxDrift == 0 {
		o.Clock.MaxDrift = 100e-6
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
	if *flagKeep {
		o.KeepGoing = true
	}
}

var (
	firstOnce sync.Once
	firstSeed uint64
)

// firstSeedFlag parses -dst.first once per process, so every sweep in a
// test binary starts from the same (possibly random) seed.
func firstSeedFlag() uint64 {
	firstOnce.Do(func() {
		if *flagFirst == "random" {
			// Wall-clock randomness is fine here: it only picks where the
			// sweep starts, and the seed is printed so the sweep repeats.
			firstSeed = 1 + rand.Uint64()%(1<<53)
			return
		}
		n, err := strconv.ParseUint(*flagFirst, 10, 64)
		if err != nil || n == 0 {
			panic(fmt.Sprintf("dsttest: -dst.first=%q: want a positive number or \"random\"", *flagFirst))
		}
		firstSeed = n
	})
	return firstSeed
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
	points   *points.Registry
	expect   []string
	workload []*sched.Task
	seed     uint64
	features map[string]bool
}

type named[T any] struct {
	name string
	fn   T
}

// Machine is a simulated machine (a process) the runner may crash,
// restart and partition.
type Machine struct {
	m          *simworld.Machine
	h          *Harness
	neverCrash bool
}

// MachineOption configures a machine.
type MachineOption func(*Machine)

// NeverCrash exempts a machine from the runner's crashes: the nemesis never
// crashes it and its disk never loses power inside an I/O call. It can
// still be partitioned, and the test can still crash it with m.Crash.
var NeverCrash MachineOption = func(m *Machine) { m.neverCrash = true }

// Machine adds a machine running start (again after each restart), and
// boots it. Its network address is its name ("db1:8080").
func (h *Harness) Machine(name string, start func(w *dst.World), opts ...MachineOption) *Machine {
	m := &Machine{h: h}
	for _, o := range opts {
		o(m)
	}
	disk := h.swarm.disk()
	if m.neverCrash {
		disk.IOCrashProb = 0
	}
	sm := h.sim.AddMachine(name, simworld.MachineOptions{Disk: disk, Clock: h.drawClock(name)})
	sm.Disk.OnCrashLoss = func(_ int, torn bool) {
		h.Sometimes("disk lost unsynced writes")
		if torn {
			h.Sometimes("disk tore a write")
		}
	}
	sm.Start = start
	m.m = sm
	h.machines = append(h.machines, m)
	sm.Boot()
	return m
}

// drawClock picks a machine's clock from its own random stream, so adding
// a machine never changes another's clock.
func (h *Harness) drawClock(name string) sched.Clock {
	c := h.opts.Clock
	if c.Synchronized {
		return sched.Clock{Rate: 1}
	}
	r := h.sim.S.Rand("clock/" + name)
	draw := func(bound float64) float64 {
		if r.IntN(4) == 0 { // at the bound, where bugs hide
			if r.IntN(2) == 0 {
				return -bound
			}
			return bound
		}
		return (2*r.Float64() - 1) * bound
	}
	offset := time.Duration(draw(float64(c.MaxOffset)))
	return sched.Clock{Offset: offset, Rate: 1 + draw(c.MaxDrift)}
}

// Clock reports the machine's clock: local time = offset + rate x true
// time.
func (m *Machine) Clock() (offset time.Duration, rate float64) {
	c := m.m.M.Clock()
	return c.Offset, c.Rate
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

// Pause stops the machine without crashing it: its goroutines don't run,
// its timers fire late, while its clock and the network carry on.
func (m *Machine) Pause() {
	if !m.m.Dead() && !m.m.Paused() {
		m.h.Sometimes("machine paused")
		m.m.Pause()
	}
}

// Resume lets a paused machine run again.
func (m *Machine) Resume() { m.m.Resume() }

// Paused reports whether the machine is paused.
func (m *Machine) Paused() bool { return m.m.Paused() }

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

// Go starts a workload goroutine on the test machine. Workload goroutines,
// and every goroutine they start, must return by the end of the recovery
// phase: one still running then fails the seed, with what it waits for (a
// deadlock, or a leaked goroutine), and the Final checks run only
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

// Feature reports whether this seed enables the workload feature name
// (swarm testing over the workload, after Groce et al.: a seed that
// leaves some actions out does the others more often, and combinations
// get tried that a fixed mix dilutes). It is on in half the seeds,
// decided by the seed and the name alone, so adding a feature leaves
// every other draw of a seed unchanged; Options.Features pins it. A
// failing seed's report lists its features.
func (h *Harness) Feature(name string) bool {
	if on, ok := h.features[name]; ok {
		return on
	}
	on, pinned := h.opts.Features[name]
	if !pinned {
		f := fnv.New64a()
		f.Write([]byte(name))
		on = rand.New(rand.NewPCG(h.seed^0xfea7c0de, f.Sum64())).IntN(2) == 0
	}
	if h.features == nil {
		h.features = map[string]bool{}
	}
	h.features[name] = on
	return on
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

	DiskOrdered   bool          // writes reach the platter in order (else per-file, harsher)
	TornWriteProb float64       // chance the first lost write is torn rather than lost whole
	IOCrashProb   float64       // chance of power loss inside each disk I/O call
	SyncLatency   time.Duration // how long an fsync takes

	MinLatency  time.Duration // network latency floor
	MeanLatency time.Duration // mean of the latency tail
	ResetProb   float64       // chance a segment resets its connection

	PCTDepth   int    // 0: uniform scheduling; else PCT with this depth
	PCTHorizon uint64 // steps PCT's priority drops are drawn from

	PauseEvery time.Duration // mean time between machine pauses (0: none)
	PauseFor   time.Duration // mean pause length

	UDPLossProb float64 // chance a datagram is lost
	UDPDupProb  float64 // chance a datagram is delivered twice

	OutageEvery time.Duration // mean time between service outages (0: none)
	OutageFor   time.Duration // mean outage length

	// Start is what clocks read at the start of the run: in half the seeds
	// a random instant in 2030, in the other half just before a midnight,
	// a month or a new year, so the boundary falls inside the run.
	Start time.Time
	// CrashLatency: with crashes on and fault points off, points still add
	// latency (never failures), so crashes land between a request's steps.
	CrashLatency bool
}

func (s Swarm) disk() simfs.Options {
	return simfs.Options{Ordered: s.DiskOrdered, TornWriteProb: s.TornWriteProb, IOCrashProb: s.IOCrashProb, SyncLatency: s.SyncLatency}
}

func (s Swarm) net() simnet.Options {
	return simnet.Options{MinLatency: s.MinLatency, MeanLatency: s.MeanLatency, ResetProb: s.ResetProb, UDPLossProb: s.UDPLossProb, UDPDupProb: s.UDPDupProb}
}

func (s Swarm) String() string {
	return fmt.Sprintf("crash every ~%v (down ~%v), partition every ~%v (for ~%v, one-way %v), disk{ordered %v, torn %.2f, power-loss-in-io %.4f}, net{latency %v+%v, reset %.4f}",
		s.CrashEvery, s.Downtime, s.PartitionEvery, s.PartitionFor, s.OneWay,
		s.DiskOrdered, s.TornWriteProb, s.IOCrashProb, s.MinLatency, s.MeanLatency, s.ResetProb) + s.sched() +
		fmt.Sprintf(", pause every ~%v (for ~%v), udp{loss %.3f, dup %.3f}, outage every ~%v (for ~%v), start %s, crash latency %v",
			s.PauseEvery, s.PauseFor, s.UDPLossProb, s.UDPDupProb, s.OutageEvery, s.OutageFor, s.Start.Format(time.RFC3339Nano), s.CrashLatency)
}

func (s Swarm) sched() string {
	if s.PCTDepth == 0 {
		return ", sched{uniform}"
	}
	return fmt.Sprintf(", sched{pct depth %d, horizon %d}", s.PCTDepth, s.PCTHorizon)
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

func newSwarm(seed uint64, f Faults, runLen time.Duration) Swarm {
	r := rand.New(rand.NewPCG(seed, 0x5a4f5a4f))
	s := Swarm{MinLatency: 50 * time.Microsecond}
	s.MeanLatency = time.Duration(100+r.IntN(2000)) * time.Microsecond
	s.SyncLatency = time.Duration(r.IntN(2000)) * time.Microsecond
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
	s.DiskOrdered = ordered
	if f.Crash {
		s.CrashEvery, s.Downtime = crashEvery, downtime
	}
	if f.Partition {
		s.PartitionEvery, s.PartitionFor, s.OneWay = partEvery, partFor, oneWay
	}
	if f.Disk {
		s.TornWriteProb, s.IOCrashProb = torn, ioCrash
	}
	if f.Network {
		s.ResetProb = reset
		if spike {
			s.MeanLatency *= 20
		}
	}
	// Scheduling (not a fault class: on for every test). Drawn last, so
	// adding it left every earlier knob's value unchanged for each seed.
	// Half the seeds use PCT; its horizon is log-uniform over 100 to
	// 100,000 steps, since a run's length isn't known in advance.
	if r.IntN(2) == 0 {
		s.PCTDepth = 1 + r.IntN(3)
		s.PCTHorizon = uint64(math.Pow(10, 2+3*r.Float64()))
	}
	// Pauses, drawn after everything above for the same reason. Lengths
	// are log-uniform from 10ms (a GC pause) to 2s (a VM stall).
	pauseEvery := sometimes(r, time.Duration(500+r.IntN(4000))*time.Millisecond)
	pauseFor := time.Duration(math.Pow(10, 1+2.3*r.Float64())) * time.Millisecond
	if f.Pause {
		s.PauseEvery, s.PauseFor = pauseEvery, pauseFor
	}
	// UDP faults (part of Network), drawn last for the same reason: loss
	// up to 20%, duplication up to 5%.
	udpLoss := sometimes(r, 0.2*r.Float64())
	udpDup := sometimes(r, 0.05*r.Float64())
	if f.Network {
		s.UDPLossProb, s.UDPDupProb = udpLoss, udpDup
	}
	// Service outages, last again; lengths log-uniform from 10ms to 3s.
	// (Fault points draw their own settings, from streams named after
	// them.)
	outageEvery := sometimes(r, time.Duration(500+r.IntN(4000))*time.Millisecond)
	outageFor := time.Duration(math.Pow(10, 1+2.5*r.Float64())) * time.Millisecond
	if f.Services {
		s.OutageEvery, s.OutageFor = outageEvery, outageFor
	}
	// The start time, and crash latency, last again.
	s.Start = drawStart(r, runLen)
	crashLatency := r.IntN(3) != 0
	if f.Crash && !f.Points {
		s.CrashLatency = crashLatency
	}
	return s
}

// drawStart picks the start time: half the time anywhere in 2030, else
// just before a boundary date code buckets by (midnight, the first of a
// month, the new year), so it falls inside a run of runLen.
func drawStart(r *rand.Rand, runLen time.Duration) time.Time {
	year := simworld.Epoch
	if r.IntN(2) == 0 {
		return year.Add(time.Duration(r.Int64N(int64(365 * 24 * time.Hour))))
	}
	var boundary time.Time
	switch r.IntN(3) {
	case 0:
		boundary = year.AddDate(0, 0, 1+r.IntN(364))
	case 1:
		boundary = year.AddDate(0, 1+r.IntN(11), 0)
	default:
		boundary = year.AddDate(1, 0, 0)
	}
	return boundary.Add(-time.Duration(r.Int64N(int64(max(runLen, time.Millisecond)))))
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
	Wall        time.Duration   // the run's wall-clock time (set by Sweep)
	Features    map[string]bool // the workload features the run asked about, and whether they were on
	Coverage    map[string]int
	Expect      []string
}

// RunSeed runs one seed and returns its result. logf, if set, receives the
// event log.
func RunSeed(seed uint64, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	return runSeed(seed, opts, setup, logf, true)
}

// runSeed runs one seed; strict checks goroutine identity on every
// scheduling call (always with Options.StrictIdentity).
func runSeed(seed uint64, opts Options, setup func(h *Harness), logf func(format string, args ...any), strict bool) Result {
	opts.defaults()
	swarm := newSwarm(seed, opts.Faults, opts.FaultPhase+opts.RecoveryPhase)
	if !opts.Start.IsZero() {
		swarm.Start = opts.Start
	}
	h := &Harness{opts: opts, swarm: swarm, cover: map[string]int{}, seed: seed}
	h.sim = simworld.New(simworld.Options{
		Seed:           seed,
		Logf:           logf,
		MaxTime:        opts.FaultPhase + opts.RecoveryPhase + time.Hour,
		MaxSteps:       opts.MaxSteps,
		Net:            swarm.net(),
		StrictIdentity: strict || opts.StrictIdentity,
		PCTDepth:       swarm.PCTDepth,
		PCTHorizon:     swarm.PCTHorizon,
		Start:          swarm.Start,
		AfterStep: func() error {
			for _, a := range h.always {
				if err := a.fn(); err != nil {
					return &InvariantError{Name: a.name, Err: err}
				}
			}
			return nil
		},
	})
	h.points = newRegistry(seed, opts, swarm, h.Sometimes, h.Logf)
	h.sim.S.Points = h.points
	h.test = h.sim.Machine("test")
	r := h.sim.RunOn(h.test, func(w *dst.World) {
		setup(h)
		h.phase = Faulty
		h.points.SetActive(true)
		h.nemesis(w)
		h.points.SetActive(false) // and every outage with it
		h.phase = Recovery
		h.Heal()
		h.sim.Net.StopFaults()
		for _, m := range h.machines {
			m.m.Disk.StopFaults() // no more power loss inside I/O
			m.Resume()
			m.Restart()
		}
		w.Sleep(opts.RecoveryPhase)
		// The workload had the recovery phase to finish; Final checks then
		// look at a quiet end state.
		if err := h.unfinished(w); err != nil {
			h.Fail(err)
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
		Steps: r.Steps, SimTime: r.SimTime, Coverage: h.cover, Expect: h.expect, Features: h.features}
}

// unfinished reports workload goroutines (and goroutines they started)
// still running when the recovery phase ends. To tell slow from stuck, it
// lets the fault-free system run for one more recovery phase: goroutines
// that finish then were slow (reported with how late), the rest are stuck.
func (h *Harness) unfinished(w *dst.World) error {
	type late struct {
		t      *sched.Task
		name   string
		why    string
		doneAt time.Duration
	}
	var pending []*late
	for _, t := range h.sim.S.Tasks() {
		if t.Done() {
			continue
		}
		for _, wl := range h.workload {
			if t.DescendsFrom(wl) {
				name := t.Name
				if t != wl {
					name = fmt.Sprintf("%s (started by %s)", t.Name, wl.Name)
				}
				pending = append(pending, &late{t: t, name: name, why: t.Blocked(), doneAt: -1})
				break
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	start := w.Now()
	for w.Since(start) < h.opts.RecoveryPhase {
		left := 0
		for _, p := range pending {
			if p.doneAt < 0 {
				if p.t.Done() {
					p.doneAt = w.Since(start)
				} else {
					left++
				}
			}
		}
		if left == 0 {
			break
		}
		w.Sleep(10 * time.Millisecond)
	}
	e := &StuckError{}
	for _, p := range pending {
		if p.doneAt < 0 && p.t.Done() {
			p.doneAt = w.Since(start)
		}
		if p.doneAt >= 0 {
			e.Slow = append(e.Slow, fmt.Sprintf("%s: finished ~%v after the recovery phase ended (was: %s)", p.name, p.doneAt.Round(10*time.Millisecond), p.why))
		} else {
			e.Stuck = append(e.Stuck, fmt.Sprintf("%s: %s", p.name, p.t.Blocked()))
		}
	}
	return e
}

// nemesis injects faults until the fault phase ends.
func (h *Harness) nemesis(w *dst.World) {
	end := w.Now().Add(h.opts.FaultPhase)
	r := h.sim.S.Rand("nemesis")
	sw := h.swarm
	exp := func(mean time.Duration) time.Duration {
		return time.Duration(r.ExpFloat64() * float64(mean))
	}
	// Classes added later draw from their own streams, so turning them on
	// leaves the crash and partition schedule of each seed as it was.
	rp, ro := h.sim.S.Rand("nemesis/pause"), h.sim.S.Rand("nemesis/outage")
	expFrom := func(rr *rand.Rand, mean time.Duration) time.Duration {
		return time.Duration(rr.ExpFloat64() * float64(mean))
	}
	type event struct {
		at time.Time
		fn func()
	}
	var events []event
	schedule := func(after time.Duration, fn func()) {
		events = append(events, event{w.Now().Add(after), fn})
	}
	var crash, partition, pause, outage func()
	outage = func() {
		if svcs := h.points.Services(); len(svcs) > 0 {
			svc := svcs[ro.IntN(len(svcs))]
			h.Logf("nemesis: outage of %s", svc)
			h.Sometimes("service outage")
			h.points.SetDown(svc, true)
			schedule(expFrom(ro, sw.OutageFor), func() {
				h.Logf("nemesis: %s is back", svc)
				h.points.SetDown(svc, false)
			})
		}
		schedule(expFrom(ro, sw.OutageEvery), outage)
	}
	crash = func() {
		alive := slices.DeleteFunc(slices.Clone(h.machines), func(m *Machine) bool { return m.Dead() || m.neverCrash })
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
	pause = func() {
		up := slices.DeleteFunc(slices.Clone(h.machines), func(m *Machine) bool { return m.Dead() || m.Paused() })
		if len(up) > 0 {
			m := up[rp.IntN(len(up))]
			h.Logf("nemesis: pause %s", m.Name())
			m.Pause()
			schedule(expFrom(rp, sw.PauseFor), func() {
				h.Logf("nemesis: resume %s", m.Name())
				m.Resume()
			})
		}
		schedule(expFrom(rp, sw.PauseEvery), pause)
	}
	if sw.CrashEvery > 0 && slices.ContainsFunc(h.machines, func(m *Machine) bool { return !m.neverCrash }) {
		schedule(exp(sw.CrashEvery), crash)
	}
	if sw.PartitionEvery > 0 && len(h.machines) > 0 {
		schedule(exp(sw.PartitionEvery), partition)
	}
	if sw.PauseEvery > 0 && len(h.machines) > 0 {
		schedule(expFrom(rp, sw.PauseEvery), pause)
	}
	if sw.OutageEvery > 0 { // after pauses, for the same reason
		schedule(expFrom(ro, sw.OutageEvery), outage)
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

// StuckError reports workload goroutines still running when the recovery
// phase ended. Slow ones finished when the system was given another
// recovery phase (the phase is too short for them, or progress is too
// slow); stuck ones didn't (a deadlock, a leak, or lost liveness).
type StuckError struct {
	Stuck []string
	Slow  []string
}

func (e *StuckError) Error() string {
	var b strings.Builder
	if len(e.Stuck) > 0 {
		b.WriteString("workload goroutines stuck (deadlock, leak or lost liveness: still blocked after a further recovery phase):\n  ")
		b.WriteString(strings.Join(e.Stuck, "\n  "))
	}
	if len(e.Slow) > 0 {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString("workload goroutines too slow (finished only after the recovery phase: lengthen RecoveryPhase if that's expected):\n  ")
		b.WriteString(strings.Join(e.Slow, "\n  "))
	}
	return b.String()
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
	FirstSeed        uint64
	Runs             int
	Failed           []Result
	Nondeterministic []Divergence
	Coverage         map[string]int
	NeverHappened    []string // Expect-ed behaviours no seed reached
	SimTime          time.Duration
	Steps            uint64
	Wall             time.Duration
	Results          []Result // every seed run, in seed order
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
				checked := i < opts.DeterminismChecks
				t0 := time.Now()
				r := runSeed(seeds[i], opts, setup, nil, checked)
				r.Wall = time.Since(t0)
				if r.Err != nil || checked {
					// The rerun is strict too: a misuse may explain a failure.
					divs[i] = compare(r, runSeed(seeds[i], opts, setup, nil, true))
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
	rep := Report{FirstSeed: opts.FirstSeed, Coverage: map[string]int{}}
	expect := map[string]bool{}
	for i, r := range results {
		if r.Seed == 0 {
			continue // not run (stopped early)
		}
		rep.Runs++
		rep.Results = append(rep.Results, r)
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

// featureList is "\nfeatures: a on, b off", or "" with no features.
func featureList(f map[string]bool) string {
	if len(f) == 0 {
		return ""
	}
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(f)) {
		v := "off"
		if f[k] {
			v = "on"
		}
		parts = append(parts, k+" "+v)
	}
	return "\nfeatures: " + strings.Join(parts, ", ")
}

// seedRecord is one line of -dst.json.
type seedRecord struct {
	Test     string          `json:"test"`
	Call     int             `json:"call"` // which call of Run in the test (0 is the first)
	Seed     uint64          `json:"seed"`
	Trace    string          `json:"trace"` // the run's fingerprint, in hex
	Index    int             `json:"index"` // position in the sweep (0 is the first seed)
	Failed   bool            `json:"failed"`
	Error    string          `json:"error,omitempty"`
	Steps    uint64          `json:"steps"`
	SimNS    int64           `json:"sim_ns"`
	WallNS   int64           `json:"wall_ns"`
	Coverage map[string]int  `json:"coverage,omitempty"`
	Features map[string]bool `json:"features,omitempty"`
}

// writeJSON appends a line per run of rep to path.
func writeJSON(path, test string, call int, rep Report) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, r := range rep.Results {
		rec := seedRecord{Test: test, Call: call, Trace: fmt.Sprintf("%016x", r.Fingerprint), Seed: r.Seed, Index: int(r.Seed - rep.FirstSeed), Failed: r.Err != nil,
			Steps: r.Steps, SimNS: int64(r.SimTime), WallNS: int64(r.Wall), Coverage: r.Coverage, Features: r.Features}
		if r.Err != nil {
			rec.Error = strings.SplitN(r.Err.Error(), "\n", 2)[0]
		}
		if err := enc.Encode(rec); err != nil {
			f.Close()
			return err
		}
	}
	return f.Close()
}

var (
	runCallsMu sync.Mutex
	runCalls   = map[testing.TB]int{}
)

// runCall numbers the calls of Run within one execution of a test (a
// rerun with -count, or the child of the replay check, starts again at 0).
func runCall(test testing.TB) int {
	runCallsMu.Lock()
	defer runCallsMu.Unlock()
	n := runCalls[test]
	runCalls[test] = n + 1
	return n
}

// replayInChild runs seed r again in a new process of the test binary and
// checks it plays the same: a seed that replays in one process but not in
// another depends on something outside the World (keys or maps made per
// process, the environment, files), so a reported seed may not reproduce.
func replayInChild(test string, call int, r Result) error {
	dir, err := os.MkdirTemp("", "dsttest-replay-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	out := filepath.Join(dir, "seed.jsonl")
	parts := strings.Split(test, "/")
	for i, p := range parts {
		parts[i] = "^" + regexp.QuoteMeta(p) + "$"
	}
	cmd := exec.Command(os.Args[0], "-test.run="+strings.Join(parts, "/"), "-test.count=1",
		"-dst.seeds=1", fmt.Sprintf("-dst.first=%d", r.Seed), "-dst.parallel=1", "-dst.json="+out)
	cmd.Env = append(os.Environ(), childEnv+"=1")
	var log bytes.Buffer
	cmd.Stdout, cmd.Stderr = &log, &log
	runErr := cmd.Run() // fails when the seed fails: the record says how
	data, err := os.ReadFile(out)
	if err != nil && runErr == nil && strings.Contains(log.String(), "no tests to run") {
		return nil // Run under a testing.TB that isn't a test of this binary: nothing to rerun
	}
	if err != nil {
		return fmt.Errorf("replaying seed %d in a new process: no record (%v):\n%s", r.Seed, runErr, tail(log.String(), 2000))
	}
	want := fmt.Sprintf("%016x", r.Fingerprint)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var rec seedRecord
		if json.Unmarshal([]byte(line), &rec) != nil || rec.Call != call || rec.Seed != r.Seed {
			continue
		}
		if rec.Trace != want || rec.Failed != (r.Err != nil) {
			return fmt.Errorf("seed %d plays differently in a new process (trace %s, failed %v; here %s, failed %v): "+
				"the run depends on something outside the World made per process, such as keys or maps built at init, "+
				"the environment or files; a reported seed may not reproduce", r.Seed, rec.Trace, rec.Failed, want, r.Err != nil)
		}
		return nil
	}
	return fmt.Errorf("replaying seed %d in a new process: its record is missing", r.Seed)
}

func tail(s string, n int) string {
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
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
		t.Logf("seed %d: %s; %v simulated in %d steps\nfaults: %v%s", r.Seed, outcome(r), r.SimTime, r.Steps, r.Swarm, featureList(r.Features))
		if r.Err != nil {
			t.Fatalf("seed %d: %v", r.Seed, r.Err)
		}
		return Report{Runs: 1, Coverage: r.Coverage}
	}
	call := runCall(t)
	rep := Sweep(opts, setup)
	if opts.DeterminismChecks >= 0 && *flagXProcess && os.Getenv(childEnv) == "" && len(rep.Results) > 0 {
		check := []Result{rep.Results[0]}
		if len(rep.Failed) > 0 && rep.Failed[0].Seed != check[0].Seed {
			check = append(check, rep.Failed[0])
		}
		for _, r := range check {
			if err := replayInChild(t.Name(), call, r); err != nil {
				t.Errorf("dsttest: %v", err)
			}
		}
	}
	if *flagJSON != "" {
		if err := writeJSON(*flagJSON, t.Name(), call, rep); err != nil {
			t.Errorf("dsttest: -dst.json: %v", err)
		}
	}
	t.Logf("%d seeds from %d: %v simulated in %v (%.0fx), %d steps", rep.Runs, rep.FirstSeed, rep.SimTime.Round(time.Millisecond), rep.Wall.Round(time.Millisecond), float64(rep.SimTime)/float64(max(rep.Wall, 1)), rep.Steps)
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
		t.Errorf("seed %d failed: %v\nfaults: %v%s\ntrace: %016x\nreplay: go test -run '^%s$' -dst.seed=%d -dst.v", r.Seed, r.Err, r.Swarm, featureList(r.Features), r.Fingerprint, name, r.Seed)
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
// blocked, or workload goroutines stuck for good (not merely slow).
func IsDeadlock(err error) bool {
	var d *sched.DeadlockError
	var s *StuckError
	return errors.As(err, &d) || (errors.As(err, &s) && len(s.Stuck) > 0)
}

// newRegistry builds a seed's fault-point registry. With Faults.Points
// off, every point is quiet (service outages still fail them).
func newRegistry(seed uint64, opts Options, swarm Swarm, cover func(string), logf func(string, ...any)) *points.Registry {
	ov := map[string]points.Config{}
	for prefix, p := range opts.Points {
		ov[prefix] = points.Config(p)
	}
	slowOnly := false
	if !opts.Faults.Points {
		if swarm.CrashLatency {
			slowOnly, ov = true, nil // latency only, as when fault points are off: no overrides
		} else {
			ov = map[string]points.Config{"": {}}
		}
	}
	r := points.New(seed, ov)
	r.SlowOnly = slowOnly
	r.Cover, r.Logf = cover, logf
	return r
}
