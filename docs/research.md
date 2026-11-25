# Deterministic simulation testing: the state of the art, and Go

Researched November 2026 (updated for the World design). What other
projects do, what makes Go hard, and why go-dst-kit takes the approach it
does.

## What DST is

Deterministic simulation testing runs a whole distributed system (every
node, the network, the disks, the clock) inside one process on one thread,
where a single seed decides every scheduling choice, delay, fault and
random number. A failure replays exactly from `(seed, commit)`, and
simulated time runs hundreds to thousands of times faster than wall time,
because the simulator jumps straight to the next event instead of waiting.

## Three ways to get there

**Restructure the code** so it is deterministic by construction: a sans-IO
core that never starts threads, reads the clock or touches the OS, with
every effect behind an interface the test swaps for a simulator.
FoundationDB, TigerBeetle and S2 work this way. go-dst-kit's first version
did too (an event-loop state machine per node); it was dropped because it
asks for a rearchitecture most Go code won't get.

**Substitute the concurrency API**: keep goroutines and channels, but take
them (with time, randomness and I/O) from an object the test can replace.
Temporal's workflow SDK does this for workflow code. go-dst-kit does it
for whole services: the `dst.World`.

**Control the runtime** so unchanged code runs deterministically:
source translation (gosim), a single-threaded WebAssembly runtime with a
patched Go runtime (Polar Signals), or a deterministic hypervisor
(Antithesis).

| Approach | Determinism | Code changes | Status for Go (Nov 2026) |
|---|---|---|---|
| **World substitution** (this library): real goroutines run one at a time on a seeded scheduler, behind `dst` APIs | complete for code that uses the World; map order excepted (detected) | mechanical substitution, package by package | depends on nothing in the runtime but goroutine IDs from `runtime.Stack` |
| **Sans-IO state machines** (go-dst-kit v0, TigerBeetle, FoundationDB) | complete | restructure the core | stable, but a rewrite |
| [gosim](https://github.com/jellevandenhooff/gosim): rewrites your code *and the standard library* onto a cooperative seeded scheduler with simulated syscalls, machines, network and disk | complete (seeded scheduler, deterministic maps, fake time) | none; it has run a 3-node etcd cluster | experimental; upstream pinned to Go 1.23, quiet since Dec 2024; forks for 1.25–1.27 are partial |
| [Polar Signals](https://www.polarsignals.com/blog/posts/2024/05/28/mostly-dst-in-go): `GOOS=wasip1` (one OS thread, no async preemption), `-tags=faketime`, a ~10-line runtime patch to seed the runtime's RNG | "mostly": occasional divergence they couldn't explain | must build for wasip1; a forked toolchain | found 3 data-loss and 2 duplication bugs in FrostDB |
| [Antithesis](https://antithesis.com): deterministic hypervisor over containers, coverage-guided exploration | complete, at the VM level | none (optional SDK assertions) | commercial; WarpStream found a long-lived race in 233 s and a rare data-loss bug |
| [`testing/synctest`](https://go.dev/blog/testing-time) (Go 1.25) | fake time only; goroutine order is still random | fake net and fs needed | good for flake-free timeout tests; not DST on its own |
| [proposal #82785 `runtime/bubble`](https://github.com/golang/go/issues/82785): host-stepped goroutines, select and timers on synctest's machinery | scheduling, select and time; maps and addresses excluded | none | open proposal; its 2019 predecessor (#33702) was declined |

**Why go-dst-kit substitutes the API.** It keeps code ordinary Go
(goroutines, channels, `select`, blocking calls), so migrating is a
substitution a linter can check rather than a redesign. It is fully
deterministic today without depending on Go runtime internals that change
every release. It is fast: the examples simulate 1,500 to 14,000 times
faster than real time. And being inside the process lets invariants check
every replica on every step, not just the client's view. The costs: only
code that uses the World is simulated (a dependency that starts goroutines
or does I/O escapes; the watchdog and the determinism check catch this,
and `dstlint` finds it in your own code), and the real World adds a little
overhead per operation. Use gosim or Antithesis for code you can't change
at all, and `synctest` for ordinary timeout tests.

## What makes Go hard to simulate

- **Scheduler.** Goroutine order is nondeterministic even with
  `GOMAXPROCS=1`: threads blocked in syscalls don't count against it, and
  async preemption and the runtime's internal RNG (seeded from the OS, no
  override) add more variation.
- **`select`** picks uniformly at random among ready cases, by spec.
- **Map iteration** is deliberately randomized.
- **Time.** `time.Now`, timers and the netpoller follow the wall clock.
- **Global randomness.** `math/rand` seeds itself (since 1.20) and
  `rand.Seed` is a no-op (since 1.24). Libraries call `crypto/rand`: etcd
  raft does, for election timeouts, so it needs a one-function patch to
  run under any simulator that doesn't control the runtime.
- **Encodings.** gob encodes maps in iteration order; protobuf-go's text and
  JSON formats add random whitespace on purpose (`detrand`). Binary proto
  with `Deterministic: true` and `encoding/json` on structs are stable.
- **Memory addresses** (pointer-keyed maps, `%p`) vary between runs. The Go
  team raised this on #82785; it's why runtime-level determinism within one
  address space is hard.

## Who does what

**FoundationDB (C++).** Flow, an actor extension to C++ compiled to
callbacks, lets the whole cluster run single-threaded in simulation with
shimmed network, disk and time. `BUGGIFY` enables rare code paths only in
simulation. The team didn't send a packet over a real network for the
first 18 months.
([testing docs](https://apple.github.io/foundationdb/testing.html),
[Will Wilson's talk](https://alex-ii.github.io/notes/2018/04/29/distributed_systems_with_deterministic_simulation.html))

**TigerBeetle (Zig)**, read from source. `vsr.ReplicaType(StateMachine,
MessageBus, Storage, AOF)` is generic at compile time over its I/O, so the
simulator instantiates the same replica with a simulated storage, message
bus and time: no runtime dispatch, no production cost. Everything is
single-threaded with static allocation. `options_swarm` in `src/vopr.zig`
randomizes replica and client counts, latency, packet loss (0–30 %),
replay (0–50 %), path clogging, partition mode and symmetry, and read,
write and misdirect fault rates per seed. A fault atlas decides which zones
may be faulty on which replicas, so the cluster is never asked to survive
impossible correlated failures. Runs go from a safety phase to a liveness
phase, where a core of replicas, up and connected, must converge.
Assertions are dense (at least two per function in TigerStyle) and stay on
in production; a protocol-aware checker hooks every replica's commits. The
VOPR runs around the clock on 1024 cores.
([safety](https://docs.tigerbeetle.com/concepts/safety/),
[protocol-aware DST](https://tigerbeetle.com/blog/2026-08-20-protocol-aware-dst),
[TIGER_STYLE](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md))

**Zig in general.** The new `std.Io` interface is passed in like an
`Allocator`, which makes "pass a deterministic `Io` in tests" an ecosystem
convention rather than per-project discipline.
[Marionette](https://sb2bg.github.io/marionette/) is an early DST backend
for it. `dst.World` is go-dst-kit's equivalent.

**Rust.** [madsim](https://github.com/madsim-rs/madsim) (RisingWave) is a
deterministic tokio replacement that also overrides libc `getrandom` and
`clock_gettime`, so dependencies behave. [turmoil](https://github.com/tokio-rs/turmoil)
simulates hosts and a network on a single-threaded runtime.
[S2](https://s2.dev/blog/dst) combines them and runs a CI meta-test that
re-runs each seed and diffs trace logs byte for byte; it found 17 notable
bugs. loom and shuttle explore thread interleavings (concurrency model
checking, not whole-system DST).

**Go.** Besides the table above: [Temporal's Go SDK](https://github.com/temporalio/sdk-go)
replaces goroutines, channels, select and time in workflow code with
deterministic versions and ships a static analyzer (`workflowcheck`): the
World approach applied to workflow code, and the motivation for #82785.
Small DST libraries in the same style as this one exist
([faultline](https://github.com/hmdsefi/faultline), dstsim,
[EquinoxWN/deterministic-sim-testing](https://github.com/EquinoxWN/deterministic-sim-testing),
[gosimnet](https://github.com/glycerine/gosimnet)); all are early.

**Hypervisor level.** Antithesis makes a container fleet deterministic,
injects faults and branches from snapshots toward rare behaviour.
WarpStream ran its whole SaaS under it and simulated about 280 application
hours in 6 wall hours. Its `Always`/`Sometimes` assertions inspired
`dsttest`'s `Always`, `Sometimes` and `Expect`.

## Ideas borrowed

| Idea | From | In go-dst-kit |
|---|---|---|
| replace the concurrency API, keep the code's shape | Temporal workflow SDK | `dst.World` |
| injected I/O, passed like an allocator | FoundationDB, TigerBeetle, Zig `std.Io` | `w.FS()`, `w.Net()` |
| one seed for everything, derived per subsystem | TigerBeetle | `Rand(label)` streams |
| simulated machines, network and disk | gosim, turmoil | `dsttest` machines, `simnet`, `simfs` |
| swarm testing of fault rates | TigerBeetle, Groce et al. 2012 | `dsttest.Swarm` |
| fault phase, then a liveness phase | TigerBeetle | `dsttest` phases |
| adversarial but legal disk | FoundationDB, TigerBeetle | `simfs` |
| crashes inside I/O calls | ALICE, CrashMonkey, gosim's syscall layer | power loss inside I/O |
| determinism self-check | S2, gosim | runs seeds twice, compares fingerprints |
| always/sometimes assertions | Antithesis | `h.Always`, `h.Sometimes`, `h.Expect` |
| linearizability of client histories | Jepsen/Knossos, porcupine | `check.Linearizable` |
| static check for escapes | Temporal's `workflowcheck` | `dstlint` |
