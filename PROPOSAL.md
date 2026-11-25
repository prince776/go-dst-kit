# go-dst-kit: proposal (for Prince's review; nothing is built until approved)

## Goal

Make ordinary Go code deterministic-simulation-testable by **incremental
migration**, not redesign. Code runs against a **World**. In production the
World is real (goroutines, time, os, net). In tests it is simulated: one
seed decides every goroutine interleaving, delay, fault and random number,
so a failing seed replays exactly. A **runner** sweeps seeds, injects
faults, checks correctness and reports replayable failures.

Non-goals (for now): simulating code we can't migrate (net/http, gRPC and
other libraries that start their own goroutines), coverage-guided search,
source rewriting.

## The three pieces

### 1. World: what your code uses

Your code keeps its shape (goroutines, channels, select, sleeps, blocking
calls) but takes these from the World instead of the language/stdlib:

| Today | With the World |
|---|---|
| `go f()` | `w.Go(f)` |
| `chan T`, `<-c`, `c <- v`, `close(c)` | `dst.Chan[T]` with `Recv`, `Send`, `Close` |
| `select { ... }` | `dst.Select(w, cases...)` |
| `sync.Mutex`, `sync.WaitGroup`, `sync.Cond` | `dst.Mutex`, `dst.WaitGroup`, ... (only where a wait happens) |
| `time.Now`, `time.Sleep`, timers, tickers | `w.Now()`, `w.Sleep(d)`, `w.NewTimer(d)`, `w.NewTicker(d)` |
| `context.WithTimeout/WithDeadline` | `w.WithTimeout(ctx, d)` (deadlines follow the World's clock) |
| `math/rand`, `crypto/rand` (behavioural) | `w.Rand()` |
| `os` files | `w.FS()` |
| `net.Listen/Dial` between your own processes | `w.Net()` (net.Conn-compatible streams) |

Mechanical substitution: agents can do it; a linter checks it.

### 2. Two Worlds

- **Real World**: thin wrappers over the real thing, cheap enough for production.
- **Sim World**: built on a single-threaded event loop with a virtual clock.
  - **Scheduler**: every `w.Go` is a real goroutine, but only one runs at a
    time (a baton). It runs until it blocks on a World operation, then the
    seed picks the next runnable goroutine. Time jumps when nothing is
    runnable. This explores interleavings, not just faults.
  - **Machines** (only for distributed code): a group of goroutines with its
    own disk, network address and clock. Faults hit a whole machine: crash
    (its goroutines stop forever, unsynced disk writes are lost per the disk
    model), restart (run its start function again), partition, clock skew.
    Single-process code has one machine and never sees the concept.
  - **Disk**: page-cache model, two crash modes (POSIX-harsh per file;
    ordered like ext4), torn writes, power loss inside an I/O call.
  - **Network**: streams (in-order per connection; connections can stall,
    break, reset; partitions) and optionally datagrams (loss, dup, reorder).

### 3. Runner: how you test

```go
func TestStore(t *testing.T) {
	dsttest.Run(t, dsttest.Options{Seeds: 1000, Machines: 3}, func(h *dsttest.Harness) {
		for _, m := range h.Machines() {
			m.Start(func(w dst.World) { store.Serve(w) })   // your normal code
		}
		h.Client(func(w dst.World, c *dsttest.Client) {     // a workload
			c.Do("put", func() (any, error) { return store.Put(w, "k", "v") })
		})
		h.Always("no two leaders", func() bool { ... })     // checked after every step
		h.Sometimes("leader changed")                       // must happen in some seed
		h.Linearizable(check.KV())                          // judge the client history
	})
}
```

The runner owns the engine: per-seed fault mix (swarm), safety phase (faults
on) then liveness phase (faults off, must recover), automatic tracing of
every scheduling decision and I/O, determinism check, coverage report, and
`FAIL seed 2891: go test -run TestStore -dst.seed=2891 -dst.v` to replay
with a full event log.

## Package layout (proposed)

| Package | Who imports it | What |
|---|---|---|
| `dst` | your code | World interface, Chan, Select, Mutex, WaitGroup, ... |
| `dst/realworld` | your `main` | the production World |
| `dsttest` | your tests | runner, harness, assertions, sim World construction |
| `check` | your tests | linearizability checker, models (KV, register, ...) |
| `internal/sim...` | nobody | event loop, rand, scheduler, disk, network |
| `cmd/dstlint` | CI | flags raw `go`/chan/select/time/os/net in migrated packages |

Two modules as before: the library (zero dependencies) and `examples/`.

## Verification goals (how we know the library itself is right)

| # | Goal | How it's checked |
|---|---|---|
| V1 | **Determinism**: same seed, same run, byte for byte, regardless of GOMAXPROCS, `-race`, OS | every sweep runs each seed twice and compares traces; CI runs across GOMAXPROCS values |
| V2 | **Scheduler is sound and complete**: it produces only interleavings Go allows, and every one of them for small programs | small programs checked against a brute-force model of channel/select/mutex semantics, both directions |
| V3 | **Fault models are legal and complete**: disk (both modes) and network do only what reality may, and everything it may | independent reference models, both directions (v0 already does this for the disk) |
| V4 | **Real/Sim parity**: both Worlds pass the same contract suite | one suite, run against both |
| V5 | **It finds bugs**: each example has planted bugs; each must be caught within N seeds | a bug-finding scoreboard in CI (bug → seeds needed) |
| V6 | **Oracles are right** | linearizability checker vs brute force |
| V7 | **Tests can fail** | every law test has a mutant it kills |
| V8 | **Cost**: real World overhead small enough for production; sim fast enough for big sweeps | benchmarks with budgets (targets set after the spike) |
| V9 | **Leaks are caught, not silent**: a raw `go`/chan/time/os inside simulated code is reported with its location | dstlint at compile time; at run time, a watchdog for a goroutine blocked outside the World, plus the determinism check |
| V10 | **Crashes are real**: a crashed machine's goroutines never act again | law + test |

All as PAP laws in doc comments (`Verified:` with tests and mutants);
your decisions recorded under `Decisions:`.

## Milestones (I stop for your review after each)

1. **Spike** (throwaway): baton scheduler, Chan/Select/Sleep, real World;
   run the worker pool as plain goroutine code; measure overhead and
   determinism. You judge the API feel and numbers before anything is built
   for real.
2. **World core**: concurrency, time, context, rand; Sim and Real; V1, V2, V4.
3. **Machines, disk, crash/restart**: V3 (disk), V10.
4. **Network**: streams, datagrams, partitions; V3 (network).
5. **Runner**: seeds, swarm, phases, tracing, replay, coverage, assertions.
6. **Oracles**: linearizability, models; V6.
7. **dstlint**; V9.
8. **Examples**, in your order: workerpool, wal, dedupe, lease, gcounter,
   twophase; each with planted bugs; V5.
9. **Docs, CI, sys view.**

## What can't be simulated (said up front)

Anything that blocks outside the World: libraries that start goroutines
or do their own I/O (net/http, gRPC, most database drivers). Put an
interface at that boundary and simulate your side of it. Only gosim
(rewriting the code and stdlib) or Antithesis (a deterministic hypervisor)
go further.

## Decisions for you

1. **Is this the right shape?** World + two implementations + runner, with
   the state-machine style dropped from the public API (v0 stays on branch
   `v0-reference`).
2. **World channels.** Simulated code must use `dst.Chan` / `dst.Select`
   instead of raw `chan` / `select`; Go gives no way to schedule raw channel
   operations deterministically. Acceptable? The spike will show how
   `dst.Select` reads.
3. **Network level.** Streams (net.Conn-compatible, so your codecs work)
   as the main API, datagrams optional; no HTTP/gRPC in simulation.
4. **Carry over from v0** (Apache-2, Go 1.25, zero deps, own
   linearizability checker, examples as a separate module)? And may I
   copy v0's verified pieces (event loop, rand, disk, network, checker,
   mutation tool) when their milestone comes, each re-reviewed?
5. **Package names**: `dst`, `dst/realworld`, `dsttest`, `check`.
6. **Milestones and order** as above, starting with the spike.
