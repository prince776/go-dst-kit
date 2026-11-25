// Package simworld builds simulated Worlds on the scheduler: one per
// machine, sharing a clock and a seed. The runner (dsttest) is the public
// way to use it.
package simworld

import (
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/worldhook"
)

// Epoch is what a simulated World's clock reads at the start of a run.
var Epoch = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

// Options configure a simulation.
type Options struct {
	Seed     uint64
	Watchdog time.Duration
	Logf     func(format string, args ...any)
}

// Sim is one simulated run.
type Sim struct {
	S        *sched.Scheduler
	machines map[string]*Machine
	order    []*Machine
}

// Machine is one simulated process: its goroutines, and (later) its disk
// and network address.
type Machine struct {
	Name  string
	M     *sched.Machine
	World *dst.World
}

// New starts a simulation.
func New(opts Options) *Sim {
	return &Sim{
		S:        sched.New(sched.Options{Seed: opts.Seed, Watchdog: opts.Watchdog, Logf: opts.Logf}),
		machines: map[string]*Machine{},
	}
}

// Machine returns the named machine, creating it on first use.
func (s *Sim) Machine(name string) *Machine {
	if m, ok := s.machines[name]; ok {
		return m
	}
	sm := s.S.NewMachine(name)
	m := &Machine{Name: name, M: sm}
	m.World = worldhook.NewWorld(worldhook.Spec{Sched: s.S, Machine: sm, Epoch: Epoch}).(*dst.World)
	s.machines[name] = m
	s.order = append(s.order, m)
	return m
}

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
func (s *Sim) Run(main func(w *dst.World)) Result {
	m := s.Machine("main")
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
