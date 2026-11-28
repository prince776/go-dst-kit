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
- **Stuck versus slow, as built**: instead of guessing from pending
  timers (a goroutine woken by a timer through a channel looks blocked),
  the runner gives the fault-free system one more recovery phase when the
  workload hasn't finished, and reports what finished then as slow (with
  how late) and the rest as stuck. Exact, deterministic, and it costs
  extra only on failing seeds.
- **Identity check, as built (M11)**: a goroutine ID costs 1.8µs (Go has
  no cheaper way), against ~0.9µs per scheduling step, so checking every
  call would make simulations ~3x slower. A cheap structural check (the
  task must still be running) missed a real case in testing: a raw
  goroutine that steals one grant and exits leaves every state
  consistent. So the check was strict on the determinism-checked seeds,
  failing seeds' reruns and replays (a leftover raw goroutine misuses the
  World in nearly every seed), sampled elsewhere, and strict everywhere
  with `Options.StrictIdentity`. (2026-11-27, Prince: with goroutine
  identity read from the g pointer, a few nanoseconds on amd64 and arm64,
  the check runs on every World operation there, sampled elsewhere; the
  option is gone. Measured on raft's 5-node sweep, 200 seeds: 19.5-21.7s
  strict against 18.5-25.6s sampled, within noise.) A misuse aborts the run at
  once (no waiting for the watchdog). An Always invariant that uses the
  World is always caught. The raw-goroutine test can't run under -race:
  the misuse is a data race, which -race reports itself.
- **Typed select, as built (M12)**: `OnRecv`/`OnSend`/`OnDone` return
  `Op[T]`, a small struct that is also a `Case`, so existing `Select`
  calls are unchanged and `Select2`/`Select3` infer their types. A first
  version made `Op[T]` an interface; boxing cost 2 allocations and 95ns,
  so it became a value, with the simulated path's per-case wait state
  moved into a completion closure. The 8 direction combinations of
  `Select3` are written by hand, not generated (few enough; the
  exhaustive contract check runs every direction and position with one
  case ready, and fails fast instead of hanging if an arm is miswired;
  shown by swapping two branches). Mixing up two cases' values doesn't
  compile: each case has its own type parameter.
- **PCT, as built (M13)**: priorities are floats (spawned goroutines get
  a random one in (1, 2); the k-th drop sets 1 - k/d), so goroutines
  spawned mid-run need no renumbering. The run length k that PCT's drops
  should spread over isn't known in advance, so each seed draws a
  log-uniform horizon over 100 to 100,000 steps. Starvation guard: 100
  consecutive picks while others are runnable demotes the goroutine
  (removing it makes the polling test spin to the step limit). Measured:
  a bug needing one goroutine starved for 50 steps is found by uniform
  choice in 0 of 1,000 seeds, by PCT in 20 of 50. Swarm draws PCT last,
  so every earlier knob keeps its per-seed value.
- **Clocks, as built (M14)**: `local = offset + rate x true`, on the
  scheduler's Machine (so it survives restarts). Converting a local
  deadline to true time rounds up and checks itself, so a timer never
  fires before its machine's clock reaches the deadline (the network
  re-checks deadlines after its timers fire, so 1ns early could loop).
  Each machine draws its clock from a stream named after it. The lease
  example taught a real lesson: a drift margin large enough for the
  drift bug also hides smaller timing mistakes (counting from the reply),
  so the client's drift bound is a setting and each planted bug runs at
  the bound that exposes it (100ppm or an exaggerated 5%). Added a fifth
  clock bug beyond the plan: AbsoluteExpiry (trusting the server's
  timestamp), caught by clock offset; both clock bugs pass with
  synchronized clocks.
- **Performance regression found and fixed (M14)**: sweeps had become
  ~3x slower since M11. A profile showed half the CPU in runtime.printlock:
  reading a goroutine ID goes through runtime.Stack, which takes a
  process-wide lock, so parallel seeds serialized on it. 127k lookups per
  300-seed lease sweep: 74k from 1-in-16 identity sampling (now 1 in
  256; the strict seeds do the real work) and 54k from Lock (now one per
  mutex: a primitive caches its scheduler and trusts it while that
  scheduler is mid-step). Lease sweep 1.7s -> 0.74s; simulated contended
  mutex 3.1µs -> 0.65µs. A primitive now belongs to one World
  (documented). The contended-mutex benchmark was also unfair (mutex and
  counter as separate allocations); side by side, dst.Mutex equals
  sync.Mutex.
- **Pauses, as built (M15)**: the scheduler simply skips a paused
  machine's goroutines; sleeps, timers and socket buffers then behave
  like a stopped process with no extra code. The nemesis schedules pauses
  after crashes and partitions so turning them on doesn't shift those.
  The lease example gained Kleppmann's fencing: tokens of
  generation<<32|grant, with the generation bumped on disk by atomic
  replace before any grant, and a NeverCrash store. NoFencing is caught
  in 16 of 300 seeds with pauses and 0 of 300 without: pauses find what
  partitions and latency didn't.
- **UDP, as built (M16)**: a datagram is a scheduled delivery with its
  own latency; loss and duplication are drawn at send. Partitions drop
  rather than hold (unlike TCP streams), and a paused host buffers 256.
  `dst.Net` gained `ListenPacket` (anyone implementing Net must add it).
  The heartbeat example: incarnation on disk (atomic replace) in the
  sequence number, static membership. NoSeq is caught in 3.5% of seeds
  with UDP faults and 0% without (it needs reordering beyond the gap
  between a worker's last active heartbeat and its leave); a second
  leaving worker doubled that rate. Two gaps from M14/M15 fixed here:
  their laws had tests but weren't in any Verified: list.
- **Fault points, as built (M17)**: `internal/points` is a registry on
  the scheduler; each point draws its settings and decisions from streams
  derived from the seed and its name, and a quiet Inject neither yields
  nor touches the scheduler's stream. Added `dst.Do` and `ErrInjected`
  (small helpers for fakes) beyond the plan. Services are the name up to
  the first dot. Faults.Points (rates) and Faults.Services (outages) are
  separate classes; with Points off every point is quiet.
- **A wrong claim found (M17)**: M15 said scheduling pauses last left
  crash and partition timing unchanged, but pauses drew from the same
  nemesis stream at run time, so they shifted later crashes. Pauses and
  outages now have their own streams, and a test compares schedules
  (it fails with the shared stream).
- **A flaky real-World check fixed (M17)**: context/timeout started its
  stopwatch after creating the context, so under load the deadline came
  less than 2 ticks after it (seen in 2 of 6 loaded runs).
- **objstore example**: the client re-reads after a timeout and looks for
  its entry ID; ETag preconditions prevent lost updates. Every append
  rereads the whole log, so clients space appends 20ms (5ms cost 12s per
  300 seeds). BlindRetry and TimeoutAsFailure pass every seed without
  fault points: the ambiguous timeout is what finds them.
- **Fault wrappers (after M17, Prince 2026-11-26)**: fakes stay plain and
  a wrapper adds faults, as with pre- and post-call error hooks. Added
  `dst.Call(ctx, w, name, Errors{Before, After}, op)`: Fail never runs
  op, FailAfter always does, injected latency ends with the caller's
  context (op still runs unless Fail: a received slow request takes
  effect), and a context already done isn't sent. `Errors` is per call,
  so a wrapper maps reads and writes differently. Found while designing
  it: `Inject` slept its latency past the caller's deadline. The objstore
  example now has a plain fake, a `Faulty` wrapper, and per-request
  deadlines (220 puts per 300 seeds are cut by one); the client treats
  every error but a definite refusal as ambiguous.
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
| 10 | `dst.Cond` (**done**) | Wait/Signal/Broadcast over dst.Mutex/RWMutex; real = sync.Cond, sim = scheduler wait queue; dstlint points sync.Cond users to it | contract checks in both Worlds: Signal wakes exactly one, Broadcast all, Wait reacquires the lock, a Signal with no waiter is lost |
| 11 | Goroutine-identity check (**done**, with the mutex fix) | in simulation every World operation asserts the caller holds the baton; a mismatch fails with "World operation from a goroutine this World didn't start" and the stack (also catches an Always that uses the World) | a raw goroutine calling Send gets the error; all suites pass (no false positives); cost measured |
| 12 | `Select2` / `Select3` (**done**) | generic over case element types; real World compiles to a native select per combination of case kinds (generated); sim reuses Select | select contract checks in both Worlds; benchmark near native (53ns) |
| 13 | PCT scheduler (**done**) | per-seed choice of uniform or PCT (random priorities, d-1 priority drops, d = 1..3, chosen by the swarm); demote a goroutine after N consecutive steps so polling loops can't starve others | channel model check still two-way (PCT stays legal); a planted ordering bug found in far fewer seeds than uniform; replay holds |
| 14 | Per-machine clocks (**done**) | one true timeline drives the simulation; each machine's clock is offset + rate x true time, kept across restarts; w.Now is local, Sleep/timers fire by the local clock; the test machine and all invariants use true time; `Clock{MaxOffset, MaxDrift}` with realistic defaults, some seeds at the bound. No wall-clock jumps (Go's monotonic reading can't be faked; approved) | clocks only move forward; offset and drift within bounds; Sleep(d) takes d/rate of true time; clock survives restart; fast, slow and at-bound clocks all occur; lease gets planted bug NoDriftMargin, caught only with skew on |
| 16 | UDP (**done**) | ListenPacket and Dial("udp"); per-datagram latency (so reordering); loss and duplication rates per seed; partitions and dead hosts drop; plus a heartbeat failure-detector example with a planted bug (approved) | contract checks against real loopback UDP; whole-or-nothing datagrams; no faults means exactly once; loss, duplication and reordering each occur in some seed |
| 17 | Fault points (**done**) | `dst.Inject(w, name) Fault` returns None, Fail (didn't happen) or FailAfter (happened, caller sees an error), sleeping any injected latency itself; real World always None. Per-point rates and latency drawn per seed, some seeds off; only in the fault phase; counted as coverage; per-point random streams derived from the name. Service outages: the nemesis fails every point under a prefix for a window (`Faults.Services`). `Options.Points` overrides rates by prefix. `w.Buggify(name)` shares the registry. Example: fake object store with conditional writes and a client library; planted blind retry (duplicate) and timeout-as-failure (lost ack); correct version uses ETag-style conditional writes | real World never injects; nothing outside the fault phase; each point fails in some seeds and is off in others; adding a point leaves other points' decisions unchanged; replay holds; planted bugs caught, correct client passes |
| - | Fresh nightly seeds | `-dst.first=random` picks and prints a random first seed; the nightly CI sweep uses it, so each night explores new seeds (failures still print an exact replay) | flag test: random first seed differs across runs and is reported |
| - | BUGGIFY (**done**, in M17) | `w.Buggify(name)`: false in the real World; in simulation true at a per-seed rate for each named point, only during the fault phase, counted as coverage | real World always false; no firing outside the fault phase; each point fires in some seeds and is off in others; replay holds |
| - | Process pauses (**done**, as M15) | the nemesis freezes a machine for a drawn duration (GC pause, VM stall): its goroutines don't run, its timers fire late at resume, data sent to it queues, its clock keeps going. Lease example gains a storage server and a planted NoFencing bug | a paused machine takes no steps while paused; its timers fire at resume, not before; other machines run normally; NoFencing caught, the fenced version passes |
| - | Stuck versus slow | the stuck-workload report splits goroutines waiting on a timer or deadline (likely slow) from those with nothing left to wake them (stuck) | a test with one of each classifies both correctly |
| - | Mutex as a scheduling point | in simulation `Lock`/`RLock` yield before acquiring (a process-wide "simulation running" flag keeps the real World free); folded into the identity-check milestone (11) | a lost update across two locked sections is found; real-World mutex benchmark unchanged |
| - | `NeverCrash` machine option | `h.Machine(name, start, dsttest.NeverCrash)` exempts a machine from the nemesis's crashes (still partitionable) | a NeverCrash machine is never crashed across a crash-heavy sweep; others still are |

## Performance (Apple M1, Go 1.25; dst/bench_compare_test.go)

| Operation | Bare Go | Real World | Simulated |
|---|---|---|---|
| channel round trip | 266ns | 295ns | 3.7µs |
| select (2 ready cases) | 54ns | 250ns (`Select`), 61ns (`Select2`, 0 allocs) | - |
| spawn + wait | 344ns | 390ns | 6.3µs |
| mutex, 4 goroutines contending | 67ns | 67ns | 0.65µs (Lock yields; scheduler cached per mutex) |
| context WithTimeout + cancel | 298ns | 295ns | 347ns |
| timer arm + stop | 131ns | 156ns | 166ns |
| 4 KiB write + read | 1.7µs | 1.6µs | 2.2µs |
| open + close | 14.5µs | 12.8µs | 0.6µs |
| 4 KiB write + fsync | 3.0ms | 3.0ms | 2.0µs |
| TCP 64 B round trip | 16.6µs | 18.4µs | 3.9µs |
| HTTP GET | 37.7µs | 37.7µs | 33.3µs |

Real World passthrough (2026-11-28, [real-is-std]; same session for
both columns, a loaded machine: compare within a row, not with the
table above; medians of 6):

| Uncontended operation | Bare Go | Real World before | Real World after |
|---|---|---|---|
| Mutex Lock+Unlock | 21.5ns | 19.7ns | 22.4ns |
| RWMutex RLock+RUnlock | 22.3ns | 20.8ns | 22.8ns |
| WaitGroup Add+Done+Wait | 21.6ns | 32.8ns | 22.0ns |
| Once.Do (done) | 0.77ns | 3.34ns | 0.86ns |
| Cond.Signal (no waiter) | 3.9ns | 3.9ns | 6.2ns |
| Mutex Lock+Unlock, a simulation running in the process | - | - | 40.8ns |

WaitGroup kept a simulation count beside sync's; Once couldn't inline
its fast path. Cond.Signal now always calls sync.Cond's (before, a Cond
no real goroutine had waited on skipped it), and can't inline: two call
sites exceed the inliner's budget, so one frame more than std (about
2ns). With a simulation running in the same process, each real-World
call pays the registry lookup that says "not simulated" (about 18ns);
production never runs one. Allocations equal std's in every case
(TestRealPathAllocs), alone and alongside a simulation.

Finding (fixed in M11): `dst.Mutex.Lock` on a free mutex wasn't a
scheduling point, so in simulation two critical sections with no other
World operation between them were never interleaved, and a lost update
across them couldn't be found (200 of 200 seeds missed it; now both
outcomes occur). Lock and RLock now yield first in simulation; the real
World checks an inlined atomic and matches bare Go (66ns contended,
13.8ns uncontended). A first version called the non-inlined lookup and
cost 110ns contended; the benchmark caught it.

## Adoption-driven hardening (Prince, 2026-11-26)

The base is built; from here the library hardens by adopting it in real
projects and adding what they need. North star: hashicorp/raft passing a
long DST sweep. First two:

1. **go-diskqueue** (fork prince776/go-diskqueue of nsqio's, MIT): a
   disk-backed FIFO; one I/O goroutine, channels, ticker, fsync, metadata
   by rename. Crash and disk faults go straight at its durability.
2. **llm-tor** (prince776/LLM-Tor backend, ~5k lines): blind-signature
   token issuance and redemption over Azure Cosmos DB, Key Vault, Dodo
   Payments and Gemini; fakes with fault points against real guarantees.

Library gaps found along the way go in the Todo below.

**go-diskqueue (done, 2026-11-26):** migrated (needed `Chan.Raw`: APIs that
return channels return `dst.Chan`, `Raw()` gives production code the
native one; and World filesystems are rooted, so `New(dataPath)` roots a
real World there). Its upstream tests pass on the real World. DST tests
state its durability contract and found four bugs, each fixed and each
caught again when its fix is reverted: no directory fsync after the
metadata rename (synced consumption redelivered), `Close` persisting
metadata without fsyncing data (acked, synced messages lost), a data file
left by a crash before the first sync reused in place (messages lost to
the reader's stale buffer), and upstream issue #38 (a false corruption
report at a file roll, found by the no-fault sweep alone). Model bugs on
the way: counting an in-progress Put as durable, and counting pre-crash
deliveries as synced consumption; `Depth()` under-reports after crash
recovery. Released:
v0.1.0 (main module), dstgrpc/v0.1.0, dstlint/v0.1.0.

**llm-tor (in progress, 2026-11-26):** the backend's payment, issuance
and redemption flows on a World (branch `dst`). A real HTTP service
(chi over dsthttp) with a crashable server, never-crash clients and Dodo,
and cloud fakes outside every machine behind `dst.Call`. Bugs confirmed
by traced seeds and fixed (tracked in llm-tor's DST.md): double credit
on a webhook retry, lost updates on the user document (webhook,
issuance, abuse token issuance, sign-in), a double charge on an issuance
retry, a token buying two LLM answers, a request hash that didn't cover
the prompt, and a regression in one of the fixes. Open: transient abuse
token epochs (clock skew at month boundaries; client-chosen epochs).
Library findings:
- **Process-level nondeterminism isn't caught.** RSA keys generated per
  process made seeds replay differently across processes (blinding's
  rejection sampling takes a key-dependent number of bytes from the
  World's stream). The determinism check reruns a seed in the same
  process, so it can't see this. Fixed in the test with testdata keys.
  Candidates: independent named random streams (so crypto consumption
  can't shift the workload), and a cross-process replay check (a trace
  hash printed with each failure).
- **Crashes alone barely test a request/response service.** With
  instantaneous fakes a request is atomic in virtual time, so crashes
  land only between requests: 1,000 crash-only seeds passed while a sweep
  adding latency (no failures) failed in most seeds. Candidate: the swarm
  pairs Crash with service latency, or the docs say so loudly.
- `NeverCrash` client machines, cloud fakes as plain maps outside
  machines, and dsthttp under chi all worked unchanged.
- **Every simulation starts on a month and year boundary**
  (`simworld.Epoch` = 2030-01-01 00:00 UTC) with machine clocks up to
  10ms apart, so date-bucketing code (llm-tor's monthly epochs) straddles
  a boundary at t=0 in every seed. It found a real clock-skew bug, but by
  accident, and it also hides bugs that need a mid-month start.
  Candidate: draw the start time per seed (sometimes just before a
  boundary: midnight, month, year, DST change), from its own stream.
- **Fixing exposed the next layer.** Fixing a bug changed the request
  hash's role (spent before asking the provider), and the same sweep then
  caught a regression in the fix (the hash covered credentials the client
  renews between retries). Broadening the workload (clients rotating
  abuse tokens, as the real client does) found another instance of an
  already-fixed bug class in a path the first workload never wrote.
  Lesson for docs: when a bug class is fixed, grep for every other writer
  of the same state and make the workload exercise them.
- **Seeds aren't a corpus.** A seed is entropy, not an input: after a fix
  or any workload change, the same seed explores a different run, so a
  pinned failing seed stops reproducing its bug (every llm-tor seed
  number above means nothing after the fix commits). A regression list of
  seeds is still cheap insurance; a durable corpus needs the recorded
  choice sequence (Hypothesis-style), which is also what guided
  exploration and shrinking need (Deferred).

**Disk errors (2026-11-27, raft phase 1 item 5; Prince approved the
design):** `Faults.DiskErrors`, in `AllFaults` from the start (Prince).
A full disk is a moment, not a size model (Prince): at a drawn time the
disk is capped at what it holds, stays full until the faults stop, and
frees space on remove and truncate; the cap lifts when the faults stop so
liveness checks stay fair (my push-back on "stays full forever"). EIO on
fsync follows Linux (Prince chose it over "retrying works"): the file's
unsynced writes are dropped from durability and the next fsync succeeds.
EIO on writes and reads does nothing. Laws [full-stays-full],
[fsync-error-forgets], [io-error-does-nothing] in simfs, each broken by
hand once. Turning it on in `AllFaults` broke four of the examples, each
a real mistake the examples had been making: the WAL appended after a
failed append (garbage or a hole before later acknowledged records; now
it truncates back, and `NoRepairAfterError` plants the mistake);
gcounter decremented its own count after a failed persist (a G-counter
must never go back: the next increment would reuse a count peers
already hold); heartbeat panicked on any disk error (now it rewrites the
state file until it succeeds, which is also the right answer to a failed
fsync); twophase gave up logging a decision (the transaction stayed in
flight and its participants in doubt for good) and left a failed begin
in flight. Harnesses now retry startups on ENOSPC and EIO.

**Fail-stop panics (2026-11-27):** `Harness.OnPanic(isFailStop)`. raft
panics on purpose when it can't persist its term or vote; with disk
errors that is the process dying, not a test failure. A panic the
predicate accepts kills its machine's process (Kill semantics: the page
cache survives) and restarts it after the downtime; any other panic, any
without OnPanic, and any on a NeverCrash machine still fails the run. Law
[panic-fail-stop], broken by hand (no kill: the machine kept running and
never restarted).

**Power-loss log names files (2026-11-28; Prince approved):** after a
power loss the event log says what was dropped, by path: per file its
lost writes and truncates and whether one tore, then the lost directory
operations (`disk of n1 lost: raft.log (3 writes, one torn); directory:
create snap/x/meta.json`). Proposed by the rosedb adoption (it needed a
local patch to find finding 7's mechanism); it would have shortened raft's
6.12 too. Only with `-dst.v`; draws nothing from the seed.

**rosedb (concluded 2026-11-28, prince776/rosedb `dst`, DST.md):** a
subagent's adoption on rosedb and its WAL. 11 bugs, 7 not reported
before; 4 reproduced on unmodified upstream with plain tests on the real
filesystem (an unfinished merge installed: 500 of 1000 keys wrong; Close
racing a merge: every key lost; a torn tail: Open fails or panics;
`BytesPerSync` ignored: 0 fsyncs over 256KB), fixes on `fix/*`. Disk
errors found three of them (fsyncgate, a half-built index served after a
failed merge, a re-run merge install deleting its own segments). Blind
benchmark against all rosedb and wal issues: of the reported bugs it
missed, two were outside the workload, one outside the fault model (an
ignored rename failure), one a torn record spanning blocks it exercised
but missed, and data races (one goroutine runs at a time: out of reach).
Library pieces it brought: a file's Name in the real World, a goroutine
identity check cheap enough for every call, kills that keep the page cache
and close the process's sockets, flock-style locks. Lessons for adopters:
anything in a start function that is a scheduling point (a directory
listing, a lock) moves the whole run, so gate debug logging or log after
the crash; and a fault model of an engine's knob (BytesPerSync) must
follow the engine's real accounting, or it overclaims bugs.

**memberlist (concluded 2026-11-28):** merged from `adopt/memberlist`:
UDP on gray links (slow links delay datagrams, lossy ones lose them, from
their own stream), UDP blackholes (`LinkOptions.UDPBlockShare`, in a
quarter of Gray seeds), the fault timeline (`m.Downtime()`, `m.Pauses()`,
`m.Slow()`, `h.Cut`, `h.Link`, law [timeline-is-faithful]), and machines
named by IP literals getting the net package's address types ([ip-hosts]).
dstmigrate learned nil channels in composite literals and parameters,
math/rand/v2's N, crypto/rand.Reader.

memberlist's conclusion (its DST.md §14–16): 3 bugs and a doc gap; the
1,800-seed baseline clean; a blind benchmark of 11 open issues found
none, 7 reachable, so the harness gaps are named (a refusing merge
delegate, configs with ProbeTimeout ≥ ProbeInterval, mass restarts). Its
library proposals, not approved or built (each would come to Prince
first): P1 a mass-restart nemesis on its own stream; P2 callback-latency
faults; P3 `Machine.Incarnation()`/`DownAt(t)` for dead-process
bookkeeping; P4 per-seed counts for Sometimes (how many seeds a relaxed
check relied on, not how many polls); P5 stuck reports naming the lock
holder, not only the waiter; P6 a doc recipe for checks conditioned on
a fault envelope.

**P1 built (Prince approved it 2026-11-28; on adopt/memberlist, awaiting
review):** `Faults.MassRestart` and four `Swarm` fields
(`MassRestartEvery`, `Share`, `Spread`, `Down`), drawn from a stream of
their own ("mass") and run from another ("nemesis/mass"); law
[mass-restart], `TestMassRestart`. Not in `AllFaults`, so existing
sweeps keep their seeds (a decision for Prince). Ordinary crashes treat
a machine a mass restart took as up, so they pick, and draw, as without
mass restarts ([fault-classes-independent] now covers them, with a
check that ordinary crashes do land during mass restarts). Hand-broken
six ways, each caught: the share ignored, an event's crashes run into
recovery, a NeverCrash machine taken, a mass field written into another
(`Downtime`), the nemesis's mass draws on its main stream, and ordinary
crashes skipping mass-downed machines.

**dst.WaitGroup was wrong in production (2026-11-28, found by pebble's
suite on the migrated tree):** outside a simulation, `Wait` returned as
soon as the simulation's own counter read zero, but `Done` updates that
counter before the inner `sync.WaitGroup`, so `Wait` could return while
the last `Done` was still running; a group reused at once (pebble's
batches) corrupted the inner counter ("negative WaitGroup counter",
"WaitGroup is reused before previous Wait has returned"). Fixed: outside
a simulation `Wait` is exactly `sync.WaitGroup`'s ([wait-follows-done],
white-box test, broken by hand). No adopter ships its fork, so nothing
deployed ran it. Audit after it: Mutex, RWMutex, Once and Cond are sound
outside a simulation (plain `sync` paths). The lesson: the contract suite
runs both Worlds through the same operations but never stresses the real
World's concurrency, and a race inside an adapter primitive needs that.
Todo: a real-World stress test per primitive (reuse right after the wait,
many goroutines, `-race`, thousands of rounds) in `internal/worldtest`.

**dstmigrate read go statements' operands late (2026-11-28, found by
pebble's suite):** `go f(a, b)` evaluates its function, receiver and
arguments at the go statement; the rewrite to `w.Go(func(){ f(a, b) })`
bound only composite operands, so a plain variable changed after the go
statement was read inside the goroutine (pebble's TestWALHardCrashRandomized
modelled the wrong keys, 20 of 20 runs). Fixed (de4bc27): every operand is
bound unless it can't change; a before-and-after test of eight cases.
Audit: raft's regenerated tree changed only upstream test files; rosedb,
wal, go-diskqueue and LLM-Tor had no affected library go statement;
memberlist regenerates after its baseline. The lesson for a rewriter:
test it by running a program before and after the rewrite and comparing.

**Real-World stress tests (2026-11-28, `adopt/hardening`; Prince
approved the direction):** `internal/worldtest/stress_test.go` drives
every real-World primitive as production code does (thousands of
rounds, many goroutines, reuse straight after a wait, unlocks in defers
during panics, closes racing sends, Stop racing timers), under `-race`,
each test twice: alone and with a simulation running alongside (the
real World's slow paths, as in an adopter's test binary). Each test was
broken by hand once and failed. They found three production-path bugs,
each fixed with a law: dst's unlocks and `WaitGroup.Done` read their
primitive after releasing it, so a struct reset or recycled by whoever
went next raced them under `-race` where sync's are quiet
([no-touch-after-release]); on macOS, concurrent creates through
os.Root's openat(2) fail spuriously with ENOENT, and Go 1.25.0's
`os.Root.MkdirAll` fails on a directory created meanwhile
([create-like-os]). Also: dstmigrate rewrote `os.Lstat` to the World's
Stat, which follows symbolic links in production ([lstat-keeps-links]);
`dst.RWMutex` lacked `TryLock`, `TryRLock` and `RLocker` (code using them
didn't compile after migration). Lesson: the race detector reports a
race once per process, so a race under `-count=50` fails one run in
fifty; count failures over separate processes.

**pebble (started 2026-11-28):** cockroachdb/pebble at 4b082f41, a
subagent, same method; blind benchmark held outside the agents' reach.

**pebble's library pieces (2026-11-28, `adopt/pebble`; Prince approved
the five at pebble's checkpoint 1):**
- `w.Yield()` for `runtime.Gosched` spins (pebble's commit queue and
  block cache). A plain scheduling point wasn't enough: while the
  goroutine waited for sleeps, a spin on it never lets virtual time
  advance (the run hits its step limit at t=0). So Yield parks until
  another goroutine takes a step that doesn't end in Yield, or a timer
  fires; a spin nothing can end is a deadlock naming Yield. Law
  [yield-waits-for-progress]. dstmigrate and dstlint map Gosched.
- `dst.Current()`, a last resort for leaf code no World reaches (pebble's
  `invariants.Sometimes`, 44 sites, no owner). Prince's conditions: the
  doc says so, a goroutine the World didn't start gets the real World
  (the determinism check is the backstop), and dstlint reports every
  use in every package. Laws [current-is-the-callers], [flags-current].
- `dst.Link` (hard links; pebble's checkpoints and ingestion): a create
  of the existing inode in the directory journal. Laws
  [link-shares-data], [link-is-a-dir-change] (links in the crash
  model's sequences). The model taught me POSIX's rule that renaming one
  link onto another of the same file does nothing.
- Holes on power loss, in `Faults.Disk` (harsh mode): out-of-order page
  write-back within a file, which pebble's MemFS crash clones already
  assume. Draws on their own streams (Prince's condition): every other
  Swarm field identical to main over 4 fault sets x 300 seeds, and a
  disk whose hole draws all say no crashes as before. Laws
  [holes-own-stream], [disk-holes], hole states in [crash-is-legal] and
  [crash-explores]. Two findings: my first version took pages from the
  live content, resurrecting writes a failed fsync had dropped (now from
  the pending writes; TestFsyncErrorForgets covers it); and the wal
  example's checksum didn't cover the length, so a zero-filled tail read
  as valid empty records (CRC-32 of nothing is 0). Fixed; the old
  format is the planted bug ChecksumDataOnly, caught with holes and in 0
  of 300 seeds without.
- dstmigrate `-map` for a project's own wrappers (pebble's `crtime`, 64
  sites): calls become a function taking the World first; and receivers
  of generic types with several type parameters.

## Exploration plan (approved by Prince, 2026-11-26)

Goal: find more bugs, measurably. Targets: llm-tor (request/response
service with cloud fakes: interleavings, lost replies) and go-diskqueue
(single-process storage: crash timing around fsync). Milestones in order
A to F; stop for review at the end of each.

**Guards (Prince: agreed).** The known-bug benchmark is a floor that
catches regressions in exploration, never the target. Some bugs are held
out (not looked at while tuning; checked at the end). The real measure is
new bugs found in code not yet explored; coverage of rare states
(`Sometimes` reached) is tracked alongside.

**Background (research).** Swarm testing (Groce et al., ISSTA 2012:
random per-run configurations beat all-features-on); PCT (Burckhardt et
al., ASPLOS 2010: bounded priority changes, probability of a depth-d bug
per run ≥ 1/(n·k^(d−1))); Antithesis (sometimes assertions as
checkpoints, restarting runs from them by deterministic replay; Moonpool
adds a budget per assertion); Hypothesis (shrink the choice sequence,
MacIver & Donaldson, ECOOP 2020); TigerBeetle VOPR (faults aimed at risky
moments, a liveness mode); Magma (ground-truth bugs, reached vs
triggered) and Klees et al., CCS 2018 (repeated trials, distributions).

- **A. Test design.** Docs (`writing-a-test.md`): designing the workload
  and fakes: model the real client (retries, credential renewal,
  resending after a lost reply), cover every writer of shared state,
  several clients per identity, fakes answering with every documented
  error (4xx, 412, 429 with retry-after, 5xx, partial results), a
  checklist per service type. llm-tor: two devices per user, permanent
  abuse tokens and backups, Cosmos 429 throttling, the OpenAI path,
  moderation that blocks. go-diskqueue: several producers and consumers,
  `PeekChan`, a `Depth()` check.
  *Status: done 2026-11-26.* Found: llm-tor finding 10 (moderation
  chunks cut at byte offsets let a flagged word straddling a boundary
  through; 4 of 2,000 no-fault seeds) and go-diskqueue bug 5 (the writer
  reuses a crashed run's file at offset 0 after a read error; an acked,
  synced message lost; ~1 in 400 concurrent crash seeds, two crashes in a
  row). Both reported, not fixed. Every other failure while building A was
  the model's: recording deliveries in an order nothing could observe, a
  backup counted only once acknowledged (a PUT whose reply is lost still
  stores it), a consumer judging "quiet" on a queue closed meanwhile, a
  crash epoch kept per consumer instead of per consumer and producer.
- **B. Baseline benchmark.** Known bugs re-insertable by **switches in
  code** behind a `dstbugs` build tag (Prince: whichever iterates faster;
  one build runs every bug, no worktrees; a constant false in normal
  builds). Bugs: llm-tor's 8 fixed ones (double credit, lost update
  webhook/issuance, lost update via abuse rotation, double charge, two
  answers, prompt not hashed, the hash regression, the saved 503) and
  go-diskqueue's 4; 2 + 1 held out. Per bug: failure rate per seed with a
  confidence interval, median seeds and wall time to first failure,
  reached (its `Sometimes` precondition) vs triggered; repeated trials
  for stateful strategies. Not CI: run on demand to judge an exploration
  change.
  *Status: done 2026-11-26.* `dsttest`: `Report.Results`, per-seed wall
  time, `-dst.keepgoing`, `-dst.json`; `cmd/dstbench`; docs. Switches for
  9 llm-tor and 5 go-diskqueue bugs. Baseline at 300 seeds per sweep:
  every bug not held out is found by its own sweep; the swarm with every
  fault on (`TestDSTEverything`) is much weaker for some (lost-update 79%
  → 10%, rotation-blind-write 24% → 0.7%), Groce's dilution; hard cells
  (reached often, rarely triggered): token-spent-after-answer 1.3–1.7%
  with its precondition reached in 83–98% of seeds,
  reuse-file-without-metadata 0.3%, and two go-diskqueue bugs the
  concurrent sweep never finds. Faithfulness check: each re-inserted
  go-diskqueue bug first fails at the seed that originally found it.
  Building it found a hole in llm-tor's checks (a token answered for two
  prompts wasn't caught once option (a) allowed a second answer).
- **C. Swarm improvements, each measured against B.** Start time drawn
  per seed (sometimes just before a day, month or year boundary);
  crashes come with latency (the swarm draws slow calls when crashes are
  on); named random streams, `w.Rand().Stream(name)` (Prince: this
  form); a cross-process replay check (a trace hash with every failure,
  a few seeds rerun in a subprocess); swarm over workload features,
  `h.Feature(name)` reporting whether this seed enables it (Prince:
  agreed); PCT depth drawn per seed.
  *Status: done 2026-11-26.* Measured with dstbench, 300 seeds per sweep.
  - Start time per seed, crash latency, `Rand().Stream`: no change on
    either benchmark (none of its bugs depends on the date; their sweeps
    already had latency; go-diskqueue has no fault points). Kept: they
    open behaviours the benchmark doesn't hold (finding 7 needed a
    boundary start), and a seed's other draws are unchanged.
  - Cross-process replay check: with llm-tor's per-process keys put back
    it fails on the first seed.
  - `h.Feature`: go-diskqueue's concurrent crash sweep went from never
    finding two bugs to finding them (close-without-fsync 0 -> 9,
    reuse-file-without-metadata 0 -> 1); the #38 bug fell 300 -> 152, as
    lagging consumers hide it (0/148). llm-tor: rotation-blind-write
    under every fault 2 -> 8; bugs whose trigger is a feature halve
    (prompt-not-hashed 279 -> 143, needs reuse). Learning: make a
    feature of what can crowd a bug out (actions, pace), not of rare
    failure-path triggers (token-spent-after-answer needs two; as
    features it fell 5 -> 2, back to 8 without). dstbench now lists
    features that change a bug's rate 3x.
  - PCT depth per seed: already there (half the seeds, depth 1-3).
- **D. Paths and a corpus** (design agreed with Prince 2026-11-27).
  - A run is a *path*: a root seed plus forks, each "from here, seed s".
    A fork is anchored to a marker (just after the nth `Sometimes(name)`)
    or, with no marker, a scheduler step: `17/"rotated"#2:99/@9000:3`.
    Marker anchors survive most code changes; step numbers don't
    (Antithesis keys assertion history by message for the same reason).
  - A fork keeps the root's configuration (swarm mix, features, point
    configurations, clocks) and reseeds every stream of choices
    (scheduling, PCT, nemesis timing, machine Rand and its Streams, the
    disk, the network, fault-point calls), each from the new seed and
    its own name.
  - `RunPath`, `-dst.path=...`; failures print their path.
  - Corpus: failing paths in `testdata/dst/<Test>/` (committed, like Go's
    `testdata/fuzz`), replayed first by `Run`; the explorer's frontier
    (rare-state paths) in the user cache dir, not committed (Go keeps its
    generated corpus in the build cache). An entry stores its trace and
    checkpoints: on replay the same trace is exact, a different one is
    *drifted* (the code changed) and reported so, never as "fixed". A
    saved path speeds exploration; it is not a regression test (a found
    bug gets a targeted test; Hypothesis invalidates its database when
    a test's source changes and recommends explicit examples for the
    same reason). `-dst.save` adds and refreshes entries.
  - Also: `Options.Features` can pin every feature on (Prince: one such
    sweep per target, as a floor for the easy bugs).
  - Verification: a fork matches the unforked run up to its anchor and
    differs after; a path replays exactly in and across processes; a
    stream that isn't reseeded makes that test fail.
  *Status: done 2026-11-27.* Paths, forks (marker and step anchors),
  RunPath/-dst.path, first-mark steps; the corpus (-dst.save,
  -dst.corpus, exact/drifted); Features["*"] and a fixed-workload sweep
  per target. Benchmark unchanged within noise (all bugs found; the
  floors work: read-past-rolled-file 300/300 at fixed pace vs 152 with
  pace features, prompt-not-hashed 205 vs 119). Found on the way: an old
  Shutdown hang (about 1 run in 30, fixed); the trace hashes scheduling,
  not values (documented); dstbench's feature notes fired on noise (now
  a two-proportion test at p < 0.01: the C note that lagging consumers
  expose close-without-fsync, 7/148 vs 2/152, doesn't pass it).
- **E. Guided exploration.** Antithesis's model: a `Sometimes` marker
  is a checkpoint, and a share of runs restart from where it first
  fired. Runs go in generations (the next generation's choices depend
  only on earlier ones, so an exploration replays from its first seed
  and budget). Each run is a fresh seed with probability p, else a fork
  just after a rare marker's first hit, markers weighted inversely by
  how many runs reached them (AFLFast's power schedule), with a budget
  per marker. `-dst.explore=N`. Compared on B with plain swarm and with
  Go's fuzzer driving the choices (fuzz bytes as a choice tape, falling
  back to the generator), by CPU time to first failure; held-out bugs
  at the end.
  *Result of part 1 (marker-only guidance), 2026-11-27: no gain.* On
  both benchmarks (300 runs per cell) exploration never found a bug
  earlier than a sweep; easy bugs got more failing runs but about a third
  of the distinct failing roots; hard cells got worse
  (token-spent-after-answer 8 -> 3 roots, first failure run #125 vs #58;
  reuse-file-without-metadata 1 -> 0; rotation-blind-write under every
  fault 6 -> 1). Why: these tests' markers are common (preconditions
  reached in 85-99% of runs), so rarity steers nowhere, and each fork
  costs a fresh root while these bugs depend on configuration. Forks from
  the right marker in the right root did fail (12 of 13). I should have
  objected to marker-only guidance up front: B's numbers already showed
  the markers weren't rare. Proposed next (to Prince): steer by
  interleaving pairs, markers and failure signatures (code coverage
  later: Go's counters are process-global), and hashicorp/raft as a
  target with deep, rare states.
  *Part 2 (Prince agreed: pairs, markers, failure signatures; code
  coverage later), built 2026-11-27, not yet measured.* `ExploreBy`
  (`-dst.explore.by`) picks the signals: markers; order (two markers
  first reached one after the other); pairs (interleaving pairs: the
  code site where one goroutine handed off and the site where the next
  resumed, the first frame outside go-dst-kit as function:line, hashed,
  so a pair names the same handoff in every process). All three are one
  kind of feature in the explorer (1/reached weight, budget, carriers);
  markers fork at marker anchors, order and pairs at the step they first
  ran. Pairs cost a stack walk per scheduling point (cached per PC), so
  only exploration records them. Failure signatures are not a steering
  signal (a run can't be forked after it fails): `dsttest.Signature`
  normalizes a failure (numbers, and quoted values after the check's
  name), and dstbench reports distinct kinds beside distinct roots.
  Tests: by pairs, a rare state with no marker (it only waits at another
  line) is found 4 times in 600 runs against 1 by sweep and by markers;
  with both branches on one line, 0 (broken by hand). Next: measure on
  raft's benchmark, sweep vs markers vs all.
  *First raft measurement (all signals, 100 runs per cell, 2026-11-27):
  no gain.* Distinct failing roots, exploration vs sweep: vote-twice
  5/13/13 vs 7/27/18 (partitions/everything/4 nodes); vote-ignores-log
  9/4/7/2 vs 10/3/8/3; accept-stale-leader 8/4/1 vs 7/5/2;
  skip-prev-log-term 2/2/3/3 vs 1/1/5/3; readindex-without-barrier
  6/1/6/3 vs 9/1/6/5. More failing runs (forks of failing roots), about
  the same or fewer roots; first failures mostly identical, since the
  first generation is the sweep's first 32 seeds (skip-prev-log-term on
  4 nodes: run #38 vs #60; accept-stale-leader on 4 nodes: #51 vs #38).
  quorum-off-by-one under amnesia: 0/100 (the sweep: 1/300, first at
  #118). Prince (2026-11-27): improve the DSTs' fault model first, then
  return to guided exploration; this is the baseline.
  *Raft phase 4 (2026-11-28, raft DST.md 10c):* abstract states
  (Harness.State) and root budgets (ExploreRootBudget) widen state
  coverage 11-16% but find no more bugs than plain in 2000 runs. Forking a
  failing run 10-50% of the way in fails the same way 30-100% of the time
  (fresh: 0.1-2%; a fork at step 1: about fresh), so forking works and
  selection is what fails: rarity marks only 6.18's runs.
  *What others do (research, 2026-11-28):* Antithesis: boolean sometimes
  assertions as checkpoints, "a percentage of tests" restart there; advice:
  assert rare, tricky situations, tune config so rare paths run. moonpool
  (Rust DST): forks only on discoveries, priority progress (numeric
  watermark improves, compound frontier: more conditions true at once,
  bucket quality) over state novelty over code coverage; a productive run
  expanded once (4 children), barren children die; per-root frontier;
  later roots get effort only for discoveries no earlier run made. IJON:
  IJON_MAX annotations (fork on a new maximum). Importance splitting
  (RESTART): split runs as they cross levels of a score function; works
  only if the score tracks the probability of the rare event. Go-Explore:
  coarse cells, archive, return then explore. RL on abstract states
  (Borgarelli et al. 2024, BonusMaxRL; Coyote's QL; Mallory, CCS 2023):
  +15-30% unique states over random on etcd and RedisRaft, but BonusMaxRL
  10 bugs against random's 11 ("randomized exploration is surprisingly
  effective"); developer waypoints (ordered predicates, e.g. a leader
  elected, then logs differing by x) 13. ModelFuzz (OOPSLA 2025): TLA+
  model-state coverage; unique states equal to random on etcd, bugs
  faster. FoundationDB, TigerBeetle: no guidance; swarm, BUGGIFY, volume
  and strong oracles. Common thread: coverage alone doesn't buy bugs;
  progress signals toward a dangerous region (watermarks, compound
  frontiers, waypoints) do, and they carry protocol knowledge.
- **F. Shrinking.** Minimize fault events, then fork points, then phase
  lengths (shortlex); measured by trace length on B's failures.

## Todo (Prince, 2026-11-26; propose each design before building)

- **Wall-clock jumps** (Prince, 2026-11-28, from raft's step 5b): an NTP
  step or a VM resumed from a snapshot moves the wall clock while Go's
  monotonic clock (timers, `time.Since`, deadlines) goes on. Go's
  `time.Time` can't move one without the other, so the World would hand
  out wall readings without a monotonic part (`t.Round(0)`) and jump
  those, keeping `Since`/timers on the monotonic clock: code that
  compares wall readings (leases, TTLs, expiry stamps written to disk)
  sees the jump, the rest doesn't. Needs a law that each half moves as
  claimed, a negative test (a lease read on wall time breaks under a
  backward step), and a per-seed fault. Today: offsets and drift only.
- **World atomics** (Prince, 2026-11-28). The scheduler switches
  goroutines only at World operations, so code that synchronizes through
  `sync/atomic` (lock-free queues, published flags, spin loops) gets no
  interleavings between its atomic operations, and a spin loop on an
  atomic hangs the simulation (pebble's commit pipeline; `w.Yield()`
  covers the spin itself). Proposal to write: `dst.Int32`, `dst.Int64`,
  `dst.Uint32`, `dst.Uint64`, `dst.Bool`, `dst.Pointer[T]`, `dst.Value`
  mirroring `sync/atomic`'s types, zero values usable (found the way
  `dst.Mutex` finds its scheduler), each operation a scheduling point in
  simulation and the plain atomic in production (the cost to measure:
  one `sched.Active()` check); dstmigrate maps the types and the
  functions; a law that interleavings at atomics are explored (a
  lock-free bug planted in an example that only atomics scheduling
  finds). Precedent: Rust's loom model checker requires its own atomic
  types for the same reason. Data races (unsynchronized access) stay out
  of reach; `-race` covers them.

- ~~**Gray failure finds the examples' fixed timeouts**~~ **done**
  (2026-11-27, Prince: Gray goes into AllFaults, fix the examples). With
  Faults.Gray three examples' correct systems lost liveness, each a
  classic gray-failure bug:
  - dedupe gave every delivery attempt 300ms, shorter than a slow path,
    so every attempt timed out forever. Now a `Sender` doubles an
    address's timeout after a failed attempt (up to 5s) and keeps what it
    learned (shrinking it by a quarter after a success); starting from
    300ms for every message made delivery too slow.
  - twophase resolved in-doubt transactions with a fixed 200ms request
    timeout; now it doubles while requests fail and resets on success.
  - gcounter held its mutex across the state file's two fsyncs, so a
    slow disk starved merges, reads and gossip (41 gossips in an 8s run
    instead of hundreds) and nodes never converged. The lock now guards
    only the counts; a separate one serializes writes. Its gossip and its
    final check's reads also back off per peer.
  Gray is in AllFaults; every example passes, planted bugs still caught.

- ~~Machine.Restart from a workload goroutine~~ **done** (found on
  raft): a machine's boot now has no parent task, so a restarted
  machine isn't the caller's work ([boots-are-no-ones-work]).

- ~~`Rand.CryptoRead`~~ **done**: `Rand.Read` became `CryptoRead`
  (crypto/rand in the real World), `dst.CryptoReader(w)` is the
  `io.Reader` for libraries, dstlint maps `crypto/rand.Read`/`Reader`/
  `Int`/`Text` and now prints standard-library paths in full
  (`crypto/rand.Read`, not `rand.Read`). Checked Go 1.26: most crypto
  functions ignore the reader they're passed (`cryptocustomrand`
  GODEBUG; `testing/cryptotest.SetGlobalRandom` is process-wide), so key
  generation can't replay per seed; the docs say to use fixed keys.
- **Atomics**: `sync/atomic` operations aren't scheduling points, so the
  interleavings of lock-free code aren't explored, and a goroutine
  spinning on an atomic between World calls holds the baton. Options: a
  `dst.Atomic` family (yield before each operation in simulation, plain
  atomics in the real World, mapped by dstlint), or yielding only on
  some operations (CompareAndSwap, Load in a loop). Verification: a
  lock-free bug (lost CAS update, publication before initialization)
  that is caught with it and missed without.
- **`dstwrap` generator**: writes a fault-injecting wrapper from an
  interface, one `dst.Call` per method (`//go:generate dstwrap -type
  Bucket -service objstore`), so an SDK client with dozens of methods
  needs no hand-written wrapper. To settle: naming points from methods,
  mapping errors per method (reads vs writes), methods without a context
  or an error, and streaming results.
- **Bit rot (opt-in disk fault)**: corruption of data already on disk:
  occasionally flip bits in synced data (and perhaps misdirected or
  unreadable sectors). Opt-in (`Faults.BitRot`, not in `AllFaults`): code
  without checksums fails it by design. First use: go-diskqueue, which has
  no checksums. Pulled forward from the deferred storage faults (Prince,
  2026-11-26).
- **GC-shaped example**: a small pack store with dedupe and a
  consolidating GC as a template (checks gated on durable completion,
  faults off then on).
- **Map order** (open question: flag every map range, or skip loops
  judged order-independent): today map iteration order is only detected, when it
  changes behaviour on a determinism-checked seed. Options: dstlint flags
  `range` over a map in simulated packages unless the keys are sorted or
  the line says `//dst:ok`; run more seeds twice; a `dst.SortedKeys`
  helper. Also fix CLAUDE.md, which says dstlint checks map order (it
  doesn't yet).

## Deferred

- The 3 inconclusive raftkv seeds and the independent bug hunt (v0;
  Prince, reconfirmed 2026-11-26: later).
- More storage faults (I/O errors, full disk, misdirected writes; bit
  rot is in the Todo). Guided exploration and shrinking are now in the
  exploration plan (E, F).
