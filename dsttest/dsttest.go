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
//   - [fault-classes-independent] Turning on pauses, outages, fault
//     points or losses leaves the times of each seed's crashes and
//     partitions as they were.
//   - [gray-explores] With Gray faults, across seeds: slow links, lossy
//     links losing segments, connections reset at a heal, slow machines
//     stalling, and slow disks all occur; none of the stalls, losses or
//     resets happen after the fault phase.
//   - [timeline-is-faithful] A machine's Downtime and Pauses, and a pair's
//     Cut, are exactly the crashes and kills to their restarts, the pauses
//     to their resumes (or the crash that ended them), and the blocks to
//     their heal, at the true times they happened; no machine runs inside
//     one of its Down or Pause intervals.
//   - [gray-mild] In about a third of the Gray seeds (Swarm.MildGray),
//     only one or two of the gray kinds that would be on (slow links,
//     lossy links, resets at heal, slow machines, slow disks, UDP
//     blackholes) stay on, and the rest are off; every other field, and
//     every field of the other seeds, is what the seed draws without it.
//   - [disk-holes] With Disk, about two thirds of the seeds draw a chance
//     (Swarm.DiskHoleProb) that a crash writes a file back out of order,
//     page by page; without Disk it is zero; every other field is what
//     the seed draws without it.
//   - [gray-udp] With Gray faults, across seeds, lossy links lose datagrams
//     and some links drop all UDP while TCP flows; neither happens after
//     the fault phase, nor without Gray.
//   - [losses-are-final] A lost machine never boots again, even when a
//     restart was already scheduled; the runner loses only machines the
//     test's OnLoss accepts at that moment, and reports each loss once.
//     With Faults.Loss and no OnLoss, the seed fails.
//   - [boots-are-no-ones-work] A machine restarted from a workload
//     goroutine doesn't count as that goroutine's work.
//   - [kill-keeps-schedule] Turning Kill on changes which crashes keep the
//     page cache, not when crashes happen or to which machine; Kill alone
//     only kills; and a killed machine finds everything it wrote.
//   - [goroutines-are-the-live-ones] A machine's Goroutines are exactly
//     its current incarnation's goroutines that haven't returned: a
//     returned one isn't listed, a parked one is, a crashed incarnation's
//     aren't after a restart, and another machine's never are.
//   - [panic-fail-stop] With OnPanic, a panic it accepts kills only its
//     machine's process (its other goroutines stop; its disk keeps the
//     page cache) and the machine restarts; a panic it rejects, any panic
//     without OnPanic, and any panic on a NeverCrash machine fails the run.
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
	flagPath     = flag.String("dst.path", "", `run only this path (a seed with forks, as failures print it: 17/"marker"#2:99)`)
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
	// Gray is gray failure: components that are slow or flaky rather than
	// down. Some links are slow (added latency) or lossy (lost segments,
	// retransmitted after a timeout: the stream stalls); a partition's heal
	// may reset the connections that crossed it (a middlebox that forgot
	// them); some machines are slow (their goroutines keep stalling for
	// short spells, a starved CPU); some disks are slow (fsync many times
	// slower). Slow links and disks are properties of a run, so they stay
	// in the recovery phase; losses, resets and stalls stop there.
	Gray bool
	// Kill kills machines' processes (kill -9, a panic, the OOM killer)
	// and restarts them: like Crash, but the host stays up, so the disk
	// keeps everything written, synced or not (a later power loss may
	// still lose what wasn't synced). With Crash too, each crash is a kill
	// or a power loss, by a share drawn per seed (some seeds all one or
	// the other); when crashes happen doesn't change.
	Kill bool
	// Loss loses machines for good: one crashes and never restarts, with
	// its disk (a dead server, replaced by a new one). The test says which
	// machines may be lost and what happens then with Harness.OnLoss.
	Loss bool
	// DiskErrors are the errors disks return: a disk fills up and stays
	// full until the faults stop (writes that don't fit fail with ENOSPC;
	// freeing space makes room again), and fsyncs, writes and reads fail
	// with EIO, a failed fsync losing the file's unsynced writes for good
	// though the next fsync succeeds, as on Linux (docs/fault-model.md).
	// Machines marked NeverCrash are spared.
	DiskErrors bool
}

// AllFaults enables every fault class but Loss, which only a system that
// replaces lost machines can take (see Harness.OnLoss). Gray failure is
// in: a correct system's timeouts must outlast its slowest path, and slow
// links and disks stay slow in the recovery phase.
var AllFaults = Faults{Crash: true, Partition: true, Disk: true, Network: true, Pause: true, Points: true, Services: true, Gray: true, DiskErrors: true}

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
	// every seed; the others are drawn per seed. The key "*" pins every
	// feature not named: a sweep of the fixed, original workload.
	Features map[string]bool
	// Explore, if positive, makes Run explore with this many guided runs
	// instead of sweeping Seeds (see Explore; -dst.explore).
	Explore int
	// ExploreFresh is the share of guided runs that are fresh seeds
	// (default 0.25); ExploreBudget caps the runs forked from one marker
	// (default max(8, Explore/10)); ExploreBatch is the generation size
	// (default 32; fixed, so an exploration replays on any machine).
	ExploreFresh  float64
	ExploreBudget int
	ExploreBatch  int
	// ExploreBy is what guided runs steer by, a comma-separated set of
	// "markers" (Sometimes), "order" (pairs of markers in the order runs
	// first reached them) and "pairs" (interleaving pairs: see
	// Result.FirstPairs). Default all three (-dst.explore.by).
	ExploreBy string
	// recordPairs makes runs record interleaving pairs (set by Explore).
	recordPairs bool
	// Corpus is the directory of saved failing paths, replayed by Run
	// before it sweeps (default testdata/dst, one directory per test).
	Corpus string
	// Start pins what clocks read at the start of every run (default:
	// drawn per seed, see Swarm.Start).
	Start time.Time
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
	if *flagExplore > 0 {
		o.Explore = *flagExplore
	}
	if o.ExploreFresh == 0 {
		o.ExploreFresh = 0.25
	}
	if o.ExploreBudget == 0 {
		o.ExploreBudget = max(8, max(o.Explore, o.Seeds)/10)
	}
	if o.ExploreBatch == 0 {
		o.ExploreBatch = 32
	}
	if *flagExploreBy != "" {
		o.ExploreBy = *flagExploreBy
	}
	if o.ExploreBy == "" {
		o.ExploreBy = "markers,order,pairs"
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
	canLose  func(*Machine) bool // OnLoss
	lost     func(*Machine)
	onPanic  func(v any) bool // OnPanic
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
	lost       bool
	slow       bool // Gray: keeps stalling during the fault phase
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
		disk.IOCrashProb, disk.SyncErrProb, disk.WriteErrProb, disk.ReadErrProb = 0, 0, 0, 0
	}
	// Gray: whether this machine, and its disk, are slow (configuration,
	// drawn per machine: kept at a fork).
	if sw := h.swarm; sw.SlowDiskShare > 0 || sw.SlowMachineShare > 0 {
		r := h.sim.S.ConfigRand("gray/" + name)
		if r.Float64() < sw.SlowDiskShare {
			disk.SyncLatency = time.Duration(float64(max(disk.SyncLatency, time.Millisecond)) * sw.SlowDiskFactor)
			h.Sometimes("a machine has a slow disk")
		}
		m.slow = r.Float64() < sw.SlowMachineShare
	}
	sm := h.sim.AddMachine(name, simworld.MachineOptions{Disk: disk, Clock: h.drawClock(name)})
	sm.Disk.OnError = func(kind string) {
		h.Sometimes(map[string]string{"full": "a write found the disk full", "sync": "an fsync failed (EIO)",
			"write": "a write failed (EIO)", "read": "a read failed (EIO)"}[kind])
	}
	sm.Disk.OnHoles = func() { h.Sometimes("disk wrote a file back out of order (holes)") }
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
	r := h.sim.S.ConfigRand("clock/" + name) // configuration: kept at a fork
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

// Kill kills the machine's process now: like Crash, but its host stays
// up, so its disk keeps everything written (synced or not).
func (m *Machine) Kill() {
	if !m.m.Dead() {
		m.h.Sometimes("machine killed")
		m.m.Kill()
	}
}

// Goroutines returns the names of the machine's live goroutines in its
// current incarnation, sorted: those started (with World.Go or GoNamed,
// or its boot) that haven't returned, parked ones included. A crashed
// incarnation's goroutines aren't listed, nor another machine's. A test
// takes it before and after some work to find goroutines the work
// leaked, such as ones a Close should have stopped.
func (m *Machine) Goroutines() []string { return m.m.M.Live() }

// Killed reports whether the machine's last crash was a kill (its disk
// kept everything written) rather than a power loss. A machine's start
// function reads it at boot to judge what must have survived.
func (m *Machine) Killed() bool { return m.m.M.Killed() }

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
	if m.m.Dead() && !m.lost {
		m.m.Restart()
	}
}

// Lose loses the machine for good: it crashes (if it hasn't) and never
// restarts, with its disk; a restart already scheduled does nothing. A
// system replaces a lost machine with a new one (another Machine, empty).
func (m *Machine) Lose() {
	if m.lost {
		return
	}
	m.Crash()
	m.lost = true
	m.h.Sometimes("machine lost")
}

// Lost reports whether the machine was lost for good.
func (m *Machine) Lost() bool { return m.lost }

// Downtime is when the machine was down: each crash, kill, power loss in
// an I/O call or loss, to its restart (To zero if it hasn't restarted).
func (m *Machine) Downtime() []Interval { return m.h.intervals(m.m.M.Downtime()) }

// Pauses is when the machine was paused (by the runner, the test, or a
// slow machine's stalls), each to its resume or the crash that ended it.
func (m *Machine) Pauses() []Interval { return m.h.intervals(m.m.M.Pauses()) }

// Slow reports whether the machine is slow (Faults.Gray): it stalls for
// short spells now and then, the whole fault phase.
func (m *Machine) Slow() bool { return m.slow }

// OnLoss enables permanent losses (Faults.Loss) for this test. At each loss
// the runner picks, among machines canLose accepts at that moment (lost
// ones aside, crashed ones included: a server that died and never came
// back), one to Lose, and then calls lost with it, on the runner's own
// goroutine: lost should hand the news to the system (an operator's
// ticket, say) rather than wait. canLose is where the test keeps losses
// survivable (a raft cluster: only while a majority of voters would
// remain). With Faults.Loss and no OnLoss, the seed fails: nothing says
// which machines a loss may take.
func (h *Harness) OnLoss(canLose func(m *Machine) bool, lost func(m *Machine)) {
	h.canLose, h.lost = canLose, lost
}

// OnPanic says which panics are a process dying on purpose (fail-stop): a
// system that panics when its disk fails, say, as hashicorp/raft does when
// it can't save its term. A panic in a machine's goroutine for which
// isFailStop returns true kills that machine's process (as a real panic
// kills a Go process; its disk keeps the page cache) and restarts it after
// a downtime, as a supervisor would. Any other panic, and any panic on a
// NeverCrash machine, fails the run as before: keep isFailStop narrow, so
// a real bug (a nil dereference) still fails.
func (h *Harness) OnPanic(isFailStop func(v any) bool) { h.onPanic = isFailStop }

// panicked is the scheduler's OnPanic: whether t's panic kills its process.
func (h *Harness) panicked(t *sched.Task, v any) bool {
	if h.onPanic == nil {
		return false
	}
	i := slices.IndexFunc(h.machines, func(m *Machine) bool { return m.m.M == t.Machine })
	if i < 0 || h.machines[i].neverCrash || h.machines[i].lost || !h.onPanic(v) {
		return false
	}
	m := h.machines[i]
	h.Logf("a panic killed the process of %s: %v", m.Name(), v)
	h.Sometimes("a process died of a panic")
	down := h.swarm.Downtime
	if down <= 0 {
		down = 100 * time.Millisecond
	}
	h.sim.S.SpawnRoot(h.test.M, "restart after a panic", func() {
		h.test.World.Sleep(down)
		m.Restart()
	})
	return true
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
		on, pinned = h.opts.Features["*"]
	}
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
func (h *Harness) Sometimes(name string) {
	h.cover[name]++
	h.sim.S.Mark(name)
}

// Count is how many times the behaviour name (a Sometimes marker, a fault
// or point the runner recorded) has happened so far in this run.
func (h *Harness) Count(name string) int { return h.cover[name] }

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

// Interval is a stretch of a run in true time, as h.World().Now() reads
// it. To is zero while it lasts.
type Interval struct{ From, To time.Time }

// Cut is when traffic from one machine to another (by name; "test" is the
// test machine) was blocked by a partition, each to the heal that ended
// it. A one-way partition shows in one direction only.
func (h *Harness) Cut(from, to string) []Interval { return h.intervals(h.sim.Net.Cuts(from, to)) }

// LinkInfo is how a link (one direction between two machines) behaves in
// this run, drawn once per seed (Faults.Gray).
type LinkInfo struct {
	Extra      time.Duration // added latency: a slow link (stays slow in recovery)
	Loss       float64       // chance a segment is lost (retransmitted) or a datagram lost, in the fault phase
	UDPBlocked bool          // drops every datagram, TCP still flows, in the fault phase
}

// Link reports how the link from one machine to another behaves.
func (h *Harness) Link(from, to string) LinkInfo {
	extra, loss := h.sim.Net.Link(from, to)
	return LinkInfo{Extra: extra, Loss: loss, UDPBlocked: h.sim.Net.UDPBlocked(from, to)}
}

// intervals converts the scheduler's spans (true durations) to times on
// the test machine's clock, which is true time.
func (h *Harness) intervals(ss []sched.Span) []Interval {
	base := h.World().Now().Add(-h.sim.S.Now()) // true time zero
	out := make([]Interval, len(ss))
	for i, sp := range ss {
		out[i].From = base.Add(sp.From)
		if !sp.Open {
			out[i].To = base.Add(sp.To)
		}
	}
	return out
}

// ---- swarm ----

// Swarm is one seed's fault mix.
type Swarm struct {
	CrashEvery     time.Duration // mean time between crashes (0: none)
	Downtime       time.Duration // mean time a crashed machine stays down
	KillShare      float64       // share of crashes that are kills (the host stays up)
	PartitionEvery time.Duration // mean time between partitions (0: none)
	PartitionFor   time.Duration // mean partition length
	OneWay         bool          // partitions may be one way

	DiskOrdered   bool          // writes reach the platter in order (else per-file, harsher)
	TornWriteProb float64       // chance the first lost write is torn rather than lost whole
	IOCrashProb   float64       // chance of power loss inside each disk I/O call
	SyncLatency   time.Duration // how long an fsync takes
	DiskHoleProb  float64       // chance, per crash and file, of out-of-order page write-back (harsh mode; [disk-holes])

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

	LossEvery time.Duration // mean time between permanent losses (0: none)

	// Gray failure (Faults.Gray).
	SlowLinkShare    float64       // share of links that are slow
	SlowLinkMean     time.Duration // a slow link's added latency (per link, half to 1.5x this)
	LossyLinkShare   float64       // share of links that are lossy
	LinkLossProb     float64       // a lossy link's chance of losing a segment
	HealResetProb    float64       // chance a connection crossing a partition is reset at heal
	SlowMachineShare float64       // share of machines that are slow
	SlowStall        time.Duration // a slow machine's mean stall
	SlowStallEvery   time.Duration // and mean time between its stalls
	SlowDiskShare    float64       // share of machines whose disk is slow
	SlowDiskFactor   float64       // a slow disk's fsync latency, as a multiple of the others' (at least 1ms)
	UDPBlockShare    float64       // share of links that drop every datagram while TCP flows (a firewall)
	MildGray         bool          // only one or two of the gray kinds above were kept on ([gray-mild])

	// Disk errors (Faults.DiskErrors).
	FullEvery    time.Duration // mean time between disks filling up (0: never); a full disk stays full until the faults stop
	SyncErrProb  float64       // chance an fsync with data to write fails with EIO
	WriteErrProb float64       // chance a write fails with EIO
	ReadErrProb  float64       // chance a read fails with EIO

	// Start is what clocks read at the start of the run: in half the seeds
	// a random instant in 2030, in the other half just before a midnight,
	// a month or a new year, so the boundary falls inside the run.
	Start time.Time
	// CrashLatency: with crashes on and fault points off, points still add
	// latency (never failures), so crashes land between a request's steps.
	CrashLatency bool
}

func (s Swarm) disk() simfs.Options {
	return simfs.Options{Ordered: s.DiskOrdered, TornWriteProb: s.TornWriteProb, IOCrashProb: s.IOCrashProb, SyncLatency: s.SyncLatency, HoleProb: s.DiskHoleProb,
		SyncErrProb: s.SyncErrProb, WriteErrProb: s.WriteErrProb, ReadErrProb: s.ReadErrProb}
}

func (s Swarm) net() simnet.Options {
	return simnet.Options{MinLatency: s.MinLatency, MeanLatency: s.MeanLatency, ResetProb: s.ResetProb, UDPLossProb: s.UDPLossProb, UDPDupProb: s.UDPDupProb,
		Links:         simnet.LinkOptions{SlowShare: s.SlowLinkShare, SlowMean: s.SlowLinkMean, LossyShare: s.LossyLinkShare, LossProb: s.LinkLossProb, UDPBlockShare: s.UDPBlockShare},
		HealResetProb: s.HealResetProb}
}

func (s Swarm) String() string {
	return fmt.Sprintf("crash every ~%v (down ~%v), partition every ~%v (for ~%v, one-way %v), disk{ordered %v, torn %.2f, power-loss-in-io %.4f, holes %.2f}, net{latency %v+%v, reset %.4f}",
		s.CrashEvery, s.Downtime, s.PartitionEvery, s.PartitionFor, s.OneWay,
		s.DiskOrdered, s.TornWriteProb, s.IOCrashProb, s.DiskHoleProb, s.MinLatency, s.MeanLatency, s.ResetProb) + s.sched() +
		fmt.Sprintf(", pause every ~%v (for ~%v), udp{loss %.3f, dup %.3f}, outage every ~%v (for ~%v), start %s, crash latency %v",
			s.PauseEvery, s.PauseFor, s.UDPLossProb, s.UDPDupProb, s.OutageEvery, s.OutageFor, s.Start.Format(time.RFC3339Nano), s.CrashLatency) +
		fmt.Sprintf(", loss every ~%v, kills %.2f", s.LossEvery, s.KillShare) +
		fmt.Sprintf(", gray{slow links %.2f (+%v), lossy links %.2f (%.3f), heal resets %.2f, slow machines %.2f (stall %v every %v), slow disks %.2f (x%.0f), udp blocked %.2f, mild %v}",
			s.SlowLinkShare, s.SlowLinkMean, s.LossyLinkShare, s.LinkLossProb, s.HealResetProb, s.SlowMachineShare, s.SlowStall, s.SlowStallEvery, s.SlowDiskShare, s.SlowDiskFactor, s.UDPBlockShare, s.MildGray) +
		fmt.Sprintf(", disk errors{full every ~%v, fsync %.3f, write %.4f, read %.4f}", s.FullEvery, s.SyncErrProb, s.WriteErrProb, s.ReadErrProb)
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
	if f.Crash || f.Kill {
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
	if (f.Crash || f.Kill) && !f.Points {
		s.CrashLatency = crashLatency
	}
	// Permanent losses, last: a mean of 1 to 6s between them (Harness.OnLoss
	// bounds how many a test allows).
	lossEvery := sometimes(r, time.Duration(1000+r.IntN(5000))*time.Millisecond)
	if f.Loss {
		s.LossEvery = lossEvery
	}
	// Gray failure, last again. Shares up to half the links and 40% of
	// machines; slow links add 5ms to 300ms (log-uniform); lossy links lose
	// 1% to 20% of segments; slow machines stall 2ms to 50ms every 10ms to
	// 100ms; slow disks fsync 10x to 100x slower.
	g := Swarm{
		SlowLinkShare:    sometimes(r, 0.5*r.Float64()),
		SlowLinkMean:     time.Duration(5*math.Pow(60, r.Float64())) * time.Millisecond,
		LossyLinkShare:   sometimes(r, 0.5*r.Float64()),
		LinkLossProb:     0.01 + 0.19*r.Float64(),
		HealResetProb:    sometimes(r, r.Float64()),
		SlowMachineShare: sometimes(r, 0.4*r.Float64()),
		SlowStall:        time.Duration(2+r.IntN(48)) * time.Millisecond,
		SlowStallEvery:   time.Duration(10+r.IntN(90)) * time.Millisecond,
		SlowDiskShare:    sometimes(r, 0.4*r.Float64()),
		SlowDiskFactor:   10 + 90*r.Float64(),
	}
	if f.Gray {
		s.SlowLinkShare, s.SlowLinkMean, s.LossyLinkShare, s.LinkLossProb = g.SlowLinkShare, g.SlowLinkMean, g.LossyLinkShare, g.LinkLossProb
		s.HealResetProb = g.HealResetProb
		s.SlowMachineShare, s.SlowStall, s.SlowStallEvery = g.SlowMachineShare, g.SlowStall, g.SlowStallEvery
		s.SlowDiskShare, s.SlowDiskFactor = g.SlowDiskShare, g.SlowDiskFactor
	}
	// Kills, last of all: with Crash as well, a quarter of the seeds kill
	// every time, a quarter never, the rest a uniform share.
	killShare := []float64{1, 0, r.Float64(), r.Float64()}[r.IntN(4)]
	switch {
	case f.Kill && f.Crash:
		s.KillShare = killShare
	case f.Kill:
		s.KillShare = 1
	}
	// Disk errors, last again: a disk fills every 1 to 6s on average (and
	// stays full), fsyncs with data to write fail up to 5% of the time,
	// writes and reads up to 0.5%.
	fullEvery := sometimes(r, time.Duration(1000+r.IntN(5000))*time.Millisecond)
	syncErr := sometimes(r, 0.05*r.Float64())
	writeErr := sometimes(r, 0.005*r.Float64())
	readErr := sometimes(r, 0.005*r.Float64())
	if f.DiskErrors {
		s.FullEvery, s.SyncErrProb, s.WriteErrProb, s.ReadErrProb = fullEvery, syncErr, writeErr, readErr
	}
	// UDP blackholes (Gray), last again: in a quarter of the seeds up to
	// 30% of links drop datagrams while TCP flows; lifted at recovery.
	udpBlock := 0.0
	if r.IntN(4) == 0 {
		udpBlock = 0.3 * r.Float64()
	}
	if f.Gray {
		s.UDPBlockShare = udpBlock
	}
	if !noMildGray {
		s = mildGray(s, seed, f)
	}
	if !noDiskHoles {
		s = diskHoles(s, seed, f)
	}
	return s
}

// noDiskHoles skips diskHoles's draws (tests of [disk-holes] compare with it).
var noDiskHoles bool

// diskHoles draws the chance that a crash writes a file back out of order,
// page by page (simfs's HoleProb): in two thirds of the seeds with disk
// faults, uniform in [0, 1). It draws from a stream of its own, whatever
// Faults says, so every other field is what the seed draws without it.
func diskHoles(s Swarm, seed uint64, f Faults) Swarm {
	r := rand.New(rand.NewPCG(seed, 0x686f6c65)) // "hole"
	p := sometimes(r, r.Float64())
	if f.Disk {
		s.DiskHoleProb = p
	}
	return s
}

// noMildGray skips mildGray's draws (tests of [gray-mild] compare with it).
var noMildGray bool

// mildGray makes about a third of the Gray seeds mild: of the gray kinds
// that are on (a share above zero), only one or two, picked at random,
// stay on, so survivable gray failure is common (a test's checks that
// hold only inside an envelope of gray failure need such seeds). It draws
// from a stream of its own, whatever Faults says, so every other field is
// what the seed draws without it.
func mildGray(s Swarm, seed uint64, f Faults) Swarm {
	r := rand.New(rand.NewPCG(seed, 0x6d696c64)) // "mild"
	mild := r.IntN(3) == 0
	keep := 1 + r.IntN(2)
	kinds := []*float64{&s.SlowLinkShare, &s.LossyLinkShare, &s.HealResetProb, &s.SlowMachineShare, &s.SlowDiskShare, &s.UDPBlockShare}
	order := r.Perm(len(kinds))
	if !f.Gray || !mild {
		return s
	}
	s.MildGray = true
	for _, i := range order {
		if *kinds[i] == 0 {
			continue
		}
		if keep > 0 {
			keep--
			continue
		}
		*kinds[i] = 0
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
	// Path is the run's path; Forked how many of its forks the run reached
	// (a fork whose anchor never came leaves the rest unapplied).
	Path      Path
	Forked    int
	ForkSteps []uint64 // the step each reached fork was applied at
	// FirstMarks is the step at which each Sometimes marker was first
	// reached: where an explorer forks to go further from there.
	FirstMarks map[string]uint64
	// FirstPairs is the step at which each interleaving pair first ran
	// (recorded only when exploring by pairs): a pair is the code site
	// where one goroutine handed off and the site where the next resumed.
	FirstPairs map[uint64]uint64
}

// RunSeed runs one seed and returns its result. logf, if set, receives the
// event log.
func RunSeed(seed uint64, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	return runSeed(seed, opts, setup, logf)
}

// RunPath runs one path (a seed, with forks) and returns its result.
func RunPath(p Path, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	return runPath(p, opts, setup, logf)
}

func runSeed(seed uint64, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	return runPath(Path{Seed: seed}, opts, setup, logf)
}

// runPath runs one path. The root seed
// decides the configuration (faults, features, points, clocks); forks
// reseed only the choices.
func runPath(path Path, opts Options, setup func(h *Harness), logf func(format string, args ...any)) Result {
	seed := path.Seed
	opts.defaults()
	swarm := newSwarm(seed, opts.Faults, opts.FaultPhase+opts.RecoveryPhase)
	if !opts.Start.IsZero() {
		swarm.Start = opts.Start
	}
	h := &Harness{opts: opts, swarm: swarm, cover: map[string]int{}, seed: seed}
	netOpts := swarm.net()
	netOpts.Note = func(event string) { h.Sometimes(event) }
	h.sim = simworld.New(simworld.Options{
		Seed:       seed,
		Logf:       logf,
		MaxTime:    opts.FaultPhase + opts.RecoveryPhase + time.Hour,
		MaxSteps:   opts.MaxSteps,
		Net:        netOpts,
		PCTDepth:   swarm.PCTDepth,
		PCTHorizon: swarm.PCTHorizon,
		Start:      swarm.Start,
		Forks:      path.schedForks(),
		Pairs:      opts.recordPairs,
		AfterStep: func() error {
			for _, a := range h.always {
				if err := a.fn(); err != nil {
					return &InvariantError{Name: a.name, Err: err}
				}
			}
			return nil
		},
	})
	// Fault-point firings count as coverage, but aren't markers: they
	// look rare (the swarm turns points off) and forking from them rarely
	// leads anywhere; exploration guides on the test's own Sometimes.
	h.points = newRegistry(seed, opts, swarm, func(name string) { h.cover[name]++ }, h.Logf)
	h.points.NewRand = h.sim.S.Rand // per-call decisions fork with the run
	h.sim.S.Points = h.points
	h.sim.S.OnPanic = h.panicked
	h.test = h.sim.Machine("test")
	r := h.sim.RunOn(h.test, func(w *dst.World) {
		setup(h)
		h.phase = Faulty
		h.points.SetActive(true)
		h.nemesis(w)
		h.points.SetActive(false) // and every outage with it
		h.phase = Recovery
		h.sim.Net.StopFaults() // first, so the last heal resets nothing
		h.Heal()
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
		Steps: r.Steps, SimTime: r.SimTime, Coverage: h.cover, Expect: h.expect, Features: h.features,
		Path: path, Forked: h.sim.S.ForksApplied(), ForkSteps: h.sim.S.ForkSteps(), FirstMarks: h.sim.S.FirstMarks(), FirstPairs: h.sim.S.FirstPairs()}
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
	rp, ro, rl, rk := h.sim.S.Rand("nemesis/pause"), h.sim.S.Rand("nemesis/outage"), h.sim.S.Rand("nemesis/loss"), h.sim.S.Rand("nemesis/kill")
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
	var crash, partition, pause, outage, loss, fill func()
	rf := h.sim.S.Rand("nemesis/full")
	filled := map[*Machine]bool{}
	fill = func() {
		cands := slices.DeleteFunc(slices.Clone(h.machines), func(m *Machine) bool { return m.lost || m.neverCrash || filled[m] })
		if len(cands) > 0 {
			m := cands[rf.IntN(len(cands))]
			h.Logf("nemesis: the disk of %s fills up", m.Name())
			h.Sometimes("a disk filled up")
			m.m.Disk.Fill()
			filled[m] = true
		}
		schedule(expFrom(rf, sw.FullEvery), fill)
	}
	loss = func() {
		cands := slices.DeleteFunc(slices.Clone(h.machines), func(m *Machine) bool { return m.lost || m.neverCrash || !h.canLose(m) })
		if len(cands) > 0 {
			m := cands[rl.IntN(len(cands))]
			h.Logf("nemesis: lose %s for good", m.Name())
			m.Lose()
			h.lost(m)
		}
		schedule(expFrom(rl, sw.LossEvery), loss)
	}
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
			if sw.KillShare > 0 && rk.Float64() < sw.KillShare {
				h.Logf("nemesis: kill %s", m.Name())
				m.Kill()
			} else {
				h.Logf("nemesis: crash %s", m.Name())
				m.Crash()
			}
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
	// Slow machines stall for short spells (a starved CPU), each on its own
	// rhythm, from their own stream.
	if sw.SlowMachineShare > 0 {
		rs := h.sim.S.Rand("nemesis/slow")
		for _, m := range h.machines {
			if !m.slow {
				continue
			}
			var stall func()
			stall = func() {
				if !m.Dead() && !m.Paused() {
					h.Sometimes("a slow machine stalled")
					m.m.Pause()
					schedule(expFrom(rs, sw.SlowStall), func() {
						if !m.lost {
							m.m.Resume()
						}
					})
				}
				schedule(expFrom(rs, sw.SlowStallEvery), stall)
			}
			schedule(expFrom(rs, sw.SlowStallEvery), stall)
		}
	}
	if sw.FullEvery > 0 && slices.ContainsFunc(h.machines, func(m *Machine) bool { return !m.neverCrash }) {
		schedule(expFrom(rf, sw.FullEvery), fill)
	}
	if sw.LossEvery > 0 {
		if h.canLose == nil {
			h.Fail(errors.New("dsttest: Faults.Loss is on but the test never called OnLoss to say which machines a loss may take"))
			return
		}
		schedule(expFrom(rl, sw.LossEvery), loss)
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
	paths := make([]Path, opts.Seeds)
	for i := range paths {
		paths[i] = Path{Seed: opts.FirstSeed + uint64(i)}
	}
	results, divs := runAll(paths, opts, setup, opts.DeterminismChecks)
	rep := aggregate(opts, results, divs)
	rep.Wall = time.Since(start)
	return rep
}

// runAll runs paths in parallel (opts.Parallel at once); the first checks
// of them, and every failing one, run twice to check determinism. Without
// KeepGoing it stops starting runs after a failure: those not run have a
// zero Seed.
func runAll(paths []Path, opts Options, setup func(h *Harness), checks int) ([]Result, []*Divergence) {
	results := make([]Result, len(paths))
	divs := make([]*Divergence, len(paths))
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
				if next >= len(paths) || stop {
					mu.Unlock()
					return
				}
				i := next
				next++
				mu.Unlock()
				checked := i < checks
				t0 := time.Now()
				r := runPath(paths[i], opts, setup, nil)
				r.Wall = time.Since(t0)
				if r.Err != nil || checked {
					divs[i] = compare(r, runPath(paths[i], opts, setup, nil))
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
	return results, divs
}

// aggregate builds a Report from runs in order (skipping those not run).
func aggregate(opts Options, results []Result, divs []*Divergence) Report {
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
	Index    int             `json:"index"` // position in the sweep (0 is the first run)
	Path     string          `json:"path"`
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
	for i, r := range rep.Results {
		rec := seedRecord{Test: test, Call: call, Trace: fmt.Sprintf("%016x", r.Fingerprint), Seed: r.Seed, Index: i, Path: r.Path.String(), Failed: r.Err != nil,
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
		"-dst.path="+r.Path.String(), "-dst.json="+out)
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
	if *flagSeed != 0 || *flagPath != "" {
		var logf func(string, ...any)
		if *flagVerbose {
			logf = func(format string, args ...any) { fmt.Fprintf(os.Stdout, format+"\n", args...) }
		}
		path := Path{Seed: *flagSeed}
		if *flagPath != "" {
			var err error
			if path, err = ParsePath(*flagPath); err != nil {
				t.Fatalf("%v", err)
			}
		}
		r := RunPath(path, opts, setup, logf)
		if os.Getenv(childEnv) != "" && *flagJSON != "" {
			if err := writeJSON(*flagJSON, t.Name(), runCall(t), Report{FirstSeed: r.Seed, Results: []Result{r}}); err != nil {
				t.Errorf("dsttest: -dst.json: %v", err)
			}
		}
		if len(r.Path.Forks) > 0 && r.Forked < len(r.Path.Forks) {
			t.Logf("path %v: only %d of its %d forks were reached (the code changed since it was found?)", r.Path, r.Forked, len(r.Path.Forks))
		}
		t.Logf("seed %d: %s; %v simulated in %d steps\nfaults: %v%s", r.Seed, outcome(r), r.SimTime, r.Steps, r.Swarm, featureList(r.Features))
		if r.Err != nil {
			t.Fatalf("seed %d: %v", r.Seed, r.Err)
		}
		return Report{Runs: 1, Coverage: r.Coverage}
	}
	call := runCall(t)
	if *flagCorpus && os.Getenv(childEnv) == "" {
		replayCorpus(t, t.Name(), opts, setup, *flagSave)
	}
	var rep Report
	if opts.Explore > 0 || *flagExplore > 0 {
		rep = Explore(opts, setup)
	} else {
		rep = Sweep(opts, setup)
	}
	if *flagSave {
		saveFailures(t, t.Name(), opts, rep)
	}
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
		t.Errorf("seed %d failed: %v\nfaults: %v%s\ntrace: %016x\nreplay: go test -run '^%s$' %s -dst.v", r.Seed, r.Err, r.Swarm, featureList(r.Features), r.Fingerprint, name, r.Path.flag())
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
