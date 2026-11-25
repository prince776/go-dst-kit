# go-dst-kit: plan and progress

The approved design is `PROPOSAL.md`. This file tracks milestones, the
decisions made while building (each with its reason), and what was
deferred. Prince is AFK; I work through milestones and step back after
each one.

## Milestones

| # | Milestone | State |
|---|---|---|
| 1 | Spike | done (reviewed; removed after M2) |
| 2 | World core: scheduler, channels, select, time, sync, context, rand; real + sim | **done** |
| 3 | Machines, disk, crash/restart | next |
| 4 | Network streams, HTTP adapter, gRPC adapter (own module) | |
| 5 | Runner (`dsttest`) | |
| 6 | Oracles (`check`) | |
| 7 | `dstlint` | |
| 8 | Examples: workerpool, wal, dedupe, lease, gcounter, twophase | |
| 9 | Docs, CI, sys view | |

## Decisions made while building (for Prince's review)

- **`*dst.World` is a concrete type, not an interface.** Users can't build a
  World that breaks the scheduler's contract, and every operation is a
  nil check away from the plain Go call. The real World and the
  simulator construct it through an internal hook, so `dst` exports no
  constructor. (Spike used an interface.)
- **Zero-value `dst.Mutex`, `RWMutex`, `WaitGroup`, `Once`** (no World
  argument, like `sync`). A contended Lock or a Wait finds its scheduler
  through a goroutine-ID registry (~1µs, only on slow paths). Uncontended
  Lock is a plain `sync.Mutex` TryLock (14ns) and is not a scheduling point.
- **`dst.Go` names goroutines automatically** from the function
  (`pool.Buggy.func1`); `GoNamed` overrides. Names appear in deadlock
  reports and event logs.
- **Simulated contexts are backed by a standard cancel context**, so
  standard `context.WithCancel` children (even below `context.WithValue`)
  are cancelled synchronously, without the goroutine the standard library
  would otherwise start. Deviation, documented: such a standard child of
  a context that hit its deadline reports `Canceled` (Cause:
  `DeadlineExceeded`). `context.WithTimeout`/`AfterFunc` stay unusable in
  simulated code (real timers/goroutines); dstlint will flag them.
- **Per-consumer randomness is derived from (seed, label)**, so adding a
  consumer never changes another's stream.
- **Crash runs no deferred functions** (a process losing power doesn't);
  the goroutines are released after the run.

## Findings

- **Uniform random scheduling makes "one goroutine delayed for many steps"
  interleavings rare**: one outcome in the model test first appeared at
  seed 5,122. Later: a PCT-style strategy (random priorities with change
  points) as an option, measured on the examples' planted bugs.
- **Real-World Select costs 220ns vs 53ns raw** (reflect.Select). Channels
  (280 vs 255ns) and Mutex (14ns) are at parity. Later, if needed: typed
  `Select2`/`Select3` helpers with a native select inside.
- **Sim speed**: ~3.5µs per channel round trip (4 baton handoffs).

## Verification status (M2)

| Law | Test |
|---|---|
| chan-matches-go | 398 random programs: sim outcomes = brute-force model outcomes, both directions |
| same-code-both-worlds, real-is-plain-go, select-picks-ready, timers-follow-world-clock | 22-check contract suite against both Worlds (sim: 50 seeds each) |
| seed-replays | 20 seeds × whole suite, identical under GOMAXPROCS 1 and 8; negative: map-order leak changes the fingerprint |
| deadlock-reported, panic-reported, leak-reported, crash-is-final, nothing-leaks, one-at-a-time | direct tests, each with its negative case |

All tests pass with `-race`.

## Deferred

- The 3 inconclusive raftkv seeds and the independent bug hunt (v0; Prince: later).
- Goroutine-identity check on every World operation (debug mode).
- `dst.Cond`.
