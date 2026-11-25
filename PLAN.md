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
| 3 | Machines, disk, crash/restart | **done** |
| 4 | Network streams, HTTP adapter, gRPC adapter (own module) | **done** |
| 5 | Runner (`dsttest`) | next |
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

- **Disk crash model (M3).** Directory changes are one ordered journal
  (a prefix survives; a directory Sync commits it up to that directory's
  last change; rename is atomic), as on journaling filesystems. Harsh mode
  (default): each file's writes survive independently and a file Sync
  covers only its data. Ordered mode: one queue, any Sync commits
  everything before it (ext4 data=ordered's journal commit). Harsh mode
  is deliberately not maximally harsh for directories (POSIX would allow
  independent directories); keeping one journal makes cross-directory
  rename atomic, which every real filesystem guarantees.
- **`File.ReadDir` returns a seeded-random order in simulation** (the OS
  order is unspecified; `os.ReadDir` sorts). Found by the contract suite.
- **Run limits** (`MaxTime`, `MaxSteps`) and incarnation-scoped timers, so
  a crashed machine's ticker can't keep a deadlocked run alive forever.

- **Network model (M4).** TCP-like streams: in order, exactly once;
  partitions stall (TCP retransmits) and Heal delivers what was held; a
  crashed machine is silent and its restart answers with a reset; resets
  travel in order and discard unread data (RFC 7230 §6.6). No datagrams
  yet (optional in the proposal).
- **HTTP adapter**: one connection per request in simulation (no pooling);
  handler panics fail the run (net/http would log them). Use request
  contexts, not `http.Client.Timeout` (real timers), in simulated code.
- **gRPC adapter** in its own module (`dstgrpc`, grpc-go v1.84.0, the newest
  that supports Go 1.25). One connection per call; status details aren't
  carried; interceptors via dstgrpc options (grpc-go's are opaque).

## Findings

- **Uniform random scheduling makes "one goroutine delayed for many steps"
  interleavings rare**: one outcome in the model test first appeared at
  seed 5,122. Later: a PCT-style strategy (random priorities with change
  points) as an option, measured on the examples' planted bugs.
- **Real-World Select costs 220ns vs 53ns raw** (reflect.Select). Channels
  (280 vs 255ns) and Mutex (14ns) are at parity. Later, if needed: typed
  `Select2`/`Select3` helpers with a native select inside.
- **Sim speed**: ~3.5µs per channel round trip (4 baton handoffs).
- **The simulator caught a bug in my own gRPC adapter (M4)**: the server
  closed connections while the client's last frame was in flight, which
  resets the connection and can destroy the response. Fixed with a linger
  (wait for the client to close). It also showed the network model was
  too kind (a reset couldn't destroy unread data) and slightly wrong (a
  reset could overtake earlier data); both fixed.
- **Real gRPC races on deadline**: a server's context ends with
  `DeadlineExceeded` or `Canceled` depending on whether the client's reset
  arrives first. The law is that the deadline reaches the server.

## Verification status (M2)

| Law | Test |
|---|---|
| chan-matches-go | 398 random programs: sim outcomes = brute-force model outcomes, both directions |
| same-code-both-worlds, real-is-plain-go, select-picks-ready, timers-follow-world-clock | 22-check contract suite against both Worlds (sim: 50 seeds each) |
| seed-replays | 20 seeds × whole suite, identical under GOMAXPROCS 1 and 8; negative: map-order leak changes the fingerprint |
| deadlock-reported, panic-reported, leak-reported, crash-is-final, nothing-leaks, one-at-a-time | direct tests, each with its negative case |
| behaves-like-os | 13-check FS contract suite against os.Root and the simulated disk |
| crash-is-legal, sync-is-durable | 2000 random op sequences × 3 seeds × 2 modes against an independent model of the crash rules |
| crash-explores | every legal crash state reached (both modes); sanity: a kinder disk and a Sync that forgets data both fail |
| crash-restart, power-loss-is-a-crash, dead-timers-stop, runs-end | simworld tests: 30 seeds of crash/restart/power loss with every acked record surviving |
| behaves-like-net | 5-check net contract suite against the OS network and simnet |
| stream-is-exact, partition-stalls, crash-is-silent, reset-discards-unread | simnet tests, each with its negative case (e.g. a read across a partition times out) |
| handlers-run-unchanged, client-honours-context, handler-panic-fails-run | same handlers and client on net/http and in simulation; deadline and cancel end requests; negative: no deadline waits for the slow server |
| services-run-unchanged, interceptors-run, deadline-propagates | grpc-go's health service, unary + streaming + metadata + status + interceptors, on real grpc-go and in simulation; partitioned call ends with DeadlineExceeded |

All tests pass with `-race`.

## Deferred

- The 3 inconclusive raftkv seeds and the independent bug hunt (v0; Prince: later).
- Goroutine-identity check on every World operation (debug mode).
- `dst.Cond`.
