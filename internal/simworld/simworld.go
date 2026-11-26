// Package simworld builds simulated Worlds on the scheduler: one per
// machine, sharing a clock and a seed, each machine with its own disk.
// The runner (dsttest) is the public way to use it.
//
// Verified:
//   - [crash-restart] Restart reruns the machine's Start function on the
//     disk the crash left. Synced data and names are there; the rest is
//     whatever the disk's crash rules allow.
//   - [power-loss-is-a-crash] Power loss inside an I/O call crashes the
//     machine; the call never returns.
//   - [dead-timers-stop] Timers and tickers die with the incarnation that
//     created them.
//   - [runs-end] A run that exceeds its time or step limit fails instead
//     of running forever.
//
// Depends on: dst, internal/sched, internal/simfs
package simworld

import (
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simnet"
	"github.com/prince776/go-dst-kit/internal/worldhook"
)

// Epoch is what a simulated World's clock reads at the start of a run.
var Epoch = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// Options configure a simulation.
type Options struct {
	Seed     uint64
	Watchdog time.Duration
	Logf     func(format string, args ...any)
	MaxTime  time.Duration
	MaxSteps uint64
	// Net configures the network. Zero latencies get defaults (50µs + a
	// 1ms mean tail).
	Net simnet.Options
	// AfterStep runs after every scheduling step (see sched.Options).
	AfterStep func() error
	// StrictIdentity checks the caller's goroutine on every scheduling
	// call (see sched.Options).
	StrictIdentity bool
	// PCTDepth and PCTHorizon select PCT scheduling (see sched.Options).
	PCTDepth   int
	PCTHorizon uint64
}

// Sim is one simulated run.
type Sim struct {
	S        *sched.Scheduler
	Net      *simnet.Network
	machines map[string]*Machine
	order    []*Machine
}

// Machine is one simulated process: its goroutines, its disk (which
// outlives crashes) and its network address.
type Machine struct {
	Name  string
	M     *sched.Machine
	World *dst.World
	Disk  *simfs.FS
	Host  *simnet.Host
	// Start is the machine's main function, run on Restart.
	Start func(w *dst.World)
	sim   *Sim
}

// MachineOptions configure a machine.
type MachineOptions struct {
	Disk simfs.Options
	// Clock is the machine's clock (default: true time). It persists
	// across restarts.
	Clock sched.Clock
}

// New starts a simulation.
func New(opts Options) *Sim {
	if opts.Net.MinLatency == 0 && opts.Net.MeanLatency == 0 {
		opts.Net.MinLatency, opts.Net.MeanLatency = 50*time.Microsecond, time.Millisecond
	}
	s := &Sim{
		S: sched.New(sched.Options{Seed: opts.Seed, Watchdog: opts.Watchdog, Logf: opts.Logf,
			MaxTime: opts.MaxTime, MaxSteps: opts.MaxSteps, AfterStep: opts.AfterStep, StrictIdentity: opts.StrictIdentity,
			PCTDepth: opts.PCTDepth, PCTHorizon: opts.PCTHorizon}),
		machines: map[string]*Machine{},
	}
	s.Net = simnet.New(s.S, Epoch, opts.Net)
	return s
}

// Machine returns the named machine, creating it (with default options)
// on first use.
func (s *Sim) Machine(name string) *Machine {
	if m, ok := s.machines[name]; ok {
		return m
	}
	return s.AddMachine(name, MachineOptions{})
}

// AddMachine creates a machine.
func (s *Sim) AddMachine(name string, opts MachineOptions) *Machine {
	if _, ok := s.machines[name]; ok {
		panic("simworld: duplicate machine " + name)
	}
	sm := s.S.NewMachine(name)
	sm.SetClock(opts.Clock)
	if c := sm.Clock(); c != (sched.Clock{Rate: 1}) {
		s.S.Logf("machine %s clock: offset %v, rate %.6f", name, c.Offset, c.Rate)
	}
	m := &Machine{Name: name, M: sm, sim: s}
	m.Disk = simfs.New(s.S, sm, opts.Disk)
	m.Host = s.Net.Attach(sm)
	m.World = worldhook.NewWorld(worldhook.Spec{Sched: s.S, Machine: sm, Epoch: Epoch, FS: m.Disk, Net: m.Host}).(*dst.World)
	s.machines[name] = m
	s.order = append(s.order, m)
	return m
}

// Machines lists the machines in creation order.
func (s *Sim) Machines() []*Machine { return s.order }

// Boot runs the machine's Start function in a new goroutine.
func (m *Machine) Boot() {
	if m.Start == nil {
		panic("simworld: machine " + m.Name + " has no Start function")
	}
	m.sim.S.Spawn(m.M, m.Name+".main", func() { m.Start(m.World) })
}

// Crash kills the machine: its goroutines stop for good and its disk loses
// what a power failure may lose. If called from the machine itself, it
// doesn't return.
func (m *Machine) Crash() { m.sim.S.Crash(m.M) }

// Restart brings a crashed machine back and boots it.
func (m *Machine) Restart() {
	m.sim.S.Restart(m.M)
	m.Boot()
}

// Dead reports whether the machine is crashed.
func (m *Machine) Dead() bool { return m.M.Dead() }

// Result is the outcome of a run.
type Result struct {
	Seed        uint64
	Err         error
	Fingerprint uint64
	Checkpoints []uint64
	Steps       uint64
	SimTime     time.Duration
}

// Run runs main as the first goroutine of machine "main" until it returns
// (like a Go program's main), a goroutine panics, or the run deadlocks,
// then ends every remaining goroutine.
func (s *Sim) Run(main func(w *dst.World)) Result { return s.RunOn(s.Machine("main"), main) }

// RunOn is Run with main on machine m.
func (s *Sim) RunOn(m *Machine, main func(w *dst.World)) Result {
	t := s.S.Spawn(m.M, "main", func() { main(m.World) })
	err := s.S.Run(t.Done)
	s.S.Shutdown()
	return Result{
		Seed:        s.S.Seed(),
		Err:         err,
		Fingerprint: s.S.Fingerprint(),
		Checkpoints: s.S.Checkpoints(),
		Steps:       s.S.Steps(),
		SimTime:     s.S.Now(),
	}
}

// Run is New(opts).Run(main).
func Run(opts Options, main func(w *dst.World)) Result { return New(opts).Run(main) }

var (
	_ dst.FS  = (*simfs.FS)(nil)
	_ dst.Net = (*simnet.Host)(nil)
)
