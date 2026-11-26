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
| 5 | Runner (`dsttest`) | **done** |
| 6 | Oracles (`check`) | **done** |
| 7 | `dstlint` | **done** |
| 8 | Examples (own module): wal, dedupe, lease, gcounter, twophase, workerpool | **done** |
| 9 | Docs, CI, sys view | **done** (65 laws, all linked and passing) |

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

- **Runner (M5).** `dsttest.Run(t, opts, setup)`: machines are crashable
  and partitionable; workload goroutines (`h.Go`) run on a "test" machine
  that never fails and **must finish by the end of the run**. Without that,
  a deadlock among workload goroutines went unnoticed (the controller kept
  sleeping through its phases), so stuck workload is now a failure that
  names what each goroutine waits for. Background loops use
  `h.World().Go`. Swarm draws every knob regardless of which fault classes
  are on, so a seed keeps its meaning when classes are toggled.
  Determinism is checked on the first 5 seeds and on every failing seed
  (each check doubles a seed's cost).

- **`check` (M6)** is v0's checker and KV model unchanged; `History` is new:
  timed by a World's clock, safe for concurrent use (real World clients),
  and returns a `Call` handle (`Return`/`Fail`/`Abandon`). `Result.Err()`
  turns a verdict into an error for `h.Final`; inconclusive wraps
  `ErrInconclusive` so a test can choose to tolerate it. v0's `Coverage`
  is replaced by `h.Sometimes`/`h.Expect`.

- **`dstlint` (M7)** is its own module (needs golang.org/x/tools v0.40.0).
  Packages opt in with `//dst:simulated`; `//dst:ok reason` excuses a line.
  It flags go/select/channel ops/make(chan)/range over chan, time, waiting
  sync types, context timers, global math/rand and crypto/rand, os file
  functions, net, net/http entry points and grpc constructors, each with
  its World replacement. It can't see map-order dependence; the
  determinism check catches that at run time.

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
- **Writing the examples found three bugs in my own code (M8)**: the WAL
  test harness created a directory without syncing its parent (the log
  vanished with it); the runner ran Final checks before checking that the
  workload had finished (an in-flight retry looked like a lost message);
  and the runner left power loss inside I/O on during the recovery phase,
  so a machine could crash after the last restart and stay down. All
  fixed, the last one with a strengthened test shown to fail without the
  fix.
- **The determinism check caught my own example**: twophase's coordinator
  ranged over a map of deltas, so map order decided which participant was
  asked to prepare first. Every seed was flagged on its first run. Fixed
  with a fixed order (and sorted in-doubt transactions).
- **Workloads must fit the run**: a fixed-count workload with long
  timeouts can outlast the recovery phase and is (rightly) reported as
  stuck. The documented pattern is to stop starting operations once
  `h.Phase()` leaves `Faulty`.
- **Examples scoreboard (M8)**, each correct version clean over its
  seeds and every planted bug caught for the right reason:

  | Example | Seeds, faults | Planted bugs caught |
  |---|---|---|
  | wal | 300, crash+disk | NoFsync, NoDirSync, NoTruncateTornTail |
  | dedupe | 200, all | InMemorySeen, SeparateSeenFile |
  | lease | 300, crash+partition+net | ClockFromReply, NoStepDown, ServerForgets |
  | gcounter | 200, all | MergeSum, NoFsyncBeforeRename |
  | twophase | 300, all | DecideAlone, SendBeforeLog, NoPrepareLog |
  | workerpool | 300, none (scheduling only) | AddInWorker, ReturnOnError (leak), CloseEarly |

  All pass with -race; dstlint reports nothing on them (and flags a
  planted time.Now).
- **Leak detection (M8)**: tasks now record their parent, and goroutines
  started by workload goroutines must finish too, so a leaked goroutine
  is reported with its parent and what it waits for
  ("workerpool.Map.func1 (started by workload2): send on chan#3").
  Background loops started from setup (`h.World().Go`) are exempt.
- **The nightly-sized sweep (20,000 seeds per example) passes** for all
  six. Its first run found that dedupe's 5s recovery phase was too short:
  a correct producer on a slow-network seed (30ms/hop) hit one 300ms
  timeout with its backoff already at the 1s cap, and didn't finish. Not
  a system bug; the recovery phase is now 10s, and `StuckError` says that
  a goroutine waiting on a timeout may just be slow.
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
| failures-replay, finds-planted-bugs, nondeterminism-reported, always-checked, recovery-phase, expected-coverage, workload-finishes | dsttest tests: a log that acks before fsync is caught (and the correct one isn't), a re-enqueueing pool deadlocks, map-order leaks are reported, invariants fail at the breaking step, the report prints the replay command |
| matches-brute-force, accepts-real-executions, partitions-independently, budget-is-deterministic, kv-semantics | ported from v0 with their tests (20k random histories vs brute force) |
| first-ending-wins, unfinished-is-unknown, failed-is-absent, times-are-the-worlds | History tests (now World-timed and safe for concurrent use) |
| flags-escapes, respects-opt-in, respects-ok | analysistest: every expected report and nothing else, in an opted-in and a plain package |
| services-run-unchanged, interceptors-run, deadline-propagates | grpc-go's health service, unary + streaming + metadata + status + interceptors, on real grpc-go and in simulation; partitioned call ends with DeadlineExceeded |

All tests pass with `-race`.

## Approved next work (Prince, 2026-11-26)

Order: the small items (fresh nightly seeds, NeverCrash, stuck versus
slow), then 10 Cond, 11 identity check + mutex scheduling point, 12 typed
Select, 13 PCT, 14 clocks, 15 pauses, 16 UDP, 17 fault points. Each gets
a step-back when done.

| # | Item | Design | Verification goals |
|---|---|---|---|
| 10 | `dst.Cond` | Wait/Signal/Broadcast over dst.Mutex/RWMutex; real = sync.Cond, sim = scheduler wait queue; dstlint points sync.Cond users to it | contract checks in both Worlds: Signal wakes exactly one, Broadcast all, Wait reacquires the lock, a Signal with no waiter is lost |
| 11 | Goroutine-identity check | in simulation every World operation asserts the caller holds the baton; a mismatch fails with "World operation from a goroutine this World didn't start" and the stack (also catches an Always that uses the World) | a raw goroutine calling Send gets the error; all suites pass (no false positives); cost measured |
| 12 | `Select2` / `Select3` | generic over case element types; real World compiles to a native select per combination of case kinds (generated); sim reuses Select | select contract checks in both Worlds; benchmark near native (53ns) |
| 13 | PCT scheduler | per-seed choice of uniform or PCT (random priorities, d-1 priority drops, d = 1..3, chosen by the swarm); demote a goroutine after N consecutive steps so polling loops can't starve others | channel model check still two-way (PCT stays legal); a planted ordering bug found in far fewer seeds than uniform; replay holds |
| 14 | Per-machine clocks | one true timeline drives the simulation; each machine's clock is offset + rate x true time, kept across restarts; w.Now is local, Sleep/timers fire by the local clock; the test machine and all invariants use true time; `Clock{MaxOffset, MaxDrift}` with realistic defaults, some seeds at the bound. No wall-clock jumps (Go's monotonic reading can't be faked; approved) | clocks only move forward; offset and drift within bounds; Sleep(d) takes d/rate of true time; clock survives restart; fast, slow and at-bound clocks all occur; lease gets planted bug NoDriftMargin, caught only with skew on |
| 15 | UDP | ListenPacket and Dial("udp"); per-datagram latency (so reordering); loss and duplication rates per seed; partitions and dead hosts drop; plus a heartbeat failure-detector example with a planted bug (approved) | contract checks against real loopback UDP; whole-or-nothing datagrams; no faults means exactly once; loss, duplication and reordering each occur in some seed |
| 17 | Fault points (approved 2026-11-26) | `dst.Inject(w, name) Fault` returns None, Fail (didn't happen) or FailAfter (happened, caller sees an error), sleeping any injected latency itself; real World always None. Per-point rates and latency drawn per seed, some seeds off; only in the fault phase; counted as coverage; per-point random streams derived from the name. Service outages: the nemesis fails every point under a prefix for a window (`Faults.Services`). `Options.Points` overrides rates by prefix. `w.Buggify(name)` shares the registry. Example: fake object store with conditional writes and a client library; planted blind retry (duplicate) and timeout-as-failure (lost ack); correct version uses ETag-style conditional writes | real World never injects; nothing outside the fault phase; each point fails in some seeds and is off in others; adding a point leaves other points' decisions unchanged; replay holds; planted bugs caught, correct client passes |
| - | Fresh nightly seeds | `-dst.first=random` picks and prints a random first seed; the nightly CI sweep uses it, so each night explores new seeds (failures still print an exact replay) | flag test: random first seed differs across runs and is reported |
| - | BUGGIFY | `w.Buggify(name)`: false in the real World; in simulation true at a per-seed rate for each named point, only during the fault phase, counted as coverage | real World always false; no firing outside the fault phase; each point fires in some seeds and is off in others; replay holds |
| - | Process pauses | the nemesis freezes a machine for a drawn duration (GC pause, VM stall): its goroutines don't run, its timers fire late at resume, data sent to it queues, its clock keeps going. Lease example gains a storage server and a planted NoFencing bug | a paused machine takes no steps while paused; its timers fire at resume, not before; other machines run normally; NoFencing caught, the fenced version passes |
| - | Stuck versus slow | the stuck-workload report splits goroutines waiting on a timer or deadline (likely slow) from those with nothing left to wake them (stuck) | a test with one of each classifies both correctly |
| - | Mutex as a scheduling point | in simulation `Lock`/`RLock` yield before acquiring (a process-wide "simulation running" flag keeps the real World free); folded into the identity-check milestone (11) | a lost update across two locked sections is found; real-World mutex benchmark unchanged |
| - | `NeverCrash` machine option | `h.Machine(name, start, dsttest.NeverCrash)` exempts a machine from the nemesis's crashes (still partitionable) | a NeverCrash machine is never crashed across a crash-heavy sweep; others still are |

## Performance (Apple M1, Go 1.25; dst/bench_compare_test.go)

| Operation | Bare Go | Real World | Simulated |
|---|---|---|---|
| channel round trip | 266ns | 295ns | 3.7µs |
| select (2 ready cases) | 54ns | 225ns | - |
| spawn + wait | 344ns | 390ns | 6.3µs |
| mutex, 4 goroutines contending | 67ns | 52ns | 14ns (never contends: see finding) |
| context WithTimeout + cancel | 298ns | 295ns | 347ns |
| timer arm + stop | 131ns | 156ns | 166ns |
| 4 KiB write + read | 1.7µs | 1.6µs | 2.2µs |
| open + close | 14.5µs | 12.8µs | 0.6µs |
| 4 KiB write + fsync | 3.0ms | 3.0ms | 2.0µs |
| TCP 64 B round trip | 16.6µs | 18.4µs | 3.9µs |
| HTTP GET | 37.7µs | 37.7µs | 33.3µs |

Finding: `dst.Mutex.Lock` on a free mutex isn't a scheduling point, so in
simulation two critical sections with no other World operation between
them are never interleaved (a lost update across two locked sections
can't be found). Fix proposed to Prince, not yet approved.

## Deferred

- The 3 inconclusive raftkv seeds and the independent bug hunt (v0;
  Prince, reconfirmed 2026-11-26: later).
- After the normal dsttest exploration is hardened (Prince, 2026-11-26):
  guided exploration (coverage-guided via Go's fuzzer; forking from rare
  states by deterministic replay), shrinking failing seeds, and more
  storage faults (I/O errors, full disk, bit rot, misdirected writes).
