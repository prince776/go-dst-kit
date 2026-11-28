# go-dst-kit

Deterministic simulation testing for ordinary Go.

Write your service as normal goroutine-and-channel Go, but take a
`*dst.World` and use it for everything that waits or is nondeterministic:
goroutines, channels, `select`, time, locks, contexts, randomness, files
and the network. In production the World is real and every call is the
plain Go one. In tests the World is simulated: a whole cluster runs in one
process, one seed decides every goroutine interleaving, delay and fault,
and a failing seed replays exactly.

```go
func TestStore(t *testing.T) {
	dsttest.Run(t, dsttest.Options{Seeds: 500, Faults: dsttest.AllFaults}, func(h *dsttest.Harness) {
		for _, name := range []string{"db1", "db2", "db3"} {
			h.Machine(name, func(w *dst.World) { store.Serve(w, name) }) // rerun after each restart
		}
		h.Go(func(w *dst.World) { workload(w, h) })  // clients, on a machine that never fails
		h.Always("one leader per term", checkLeaders) // after every scheduling step
		h.Final("converged", checkConverged)          // once faults stop and the workload is done
	})
}
```

```
--- FAIL: TestStore
    seed 2891 failed: final check "converged" failed: db2 is missing key k17
    faults: crash every ~672ms (down ~480ms), partition every ~2.7s (for ~2.9s, one-way false), disk{ordered false, torn 0.22, power-loss-in-io 0.0018}, ...
    replay: go test -run '^TestStore$' -dst.seed=2891 -dst.v
```

## Why

Distributed-systems bugs hide in rare interleavings: a crash between two
writes, a partition during a handover, a retry that arrives after a
timeout. Ordinary tests don't reach them, and when a flaky test does, you
can't reproduce it. Under simulation:

- **Everything is explored**: goroutine order (uniform and PCT), network
  latency, partitions, resets, datagram loss and duplication, process
  crashes and pauses, power loss that drops unsynced writes or tears
  them, full disks and I/O errors (a failed fsync losing its data, as on
  Linux), process kills that keep the page cache, clock skew between
  machines, and failures, timeouts and outages of the services you fake
  (`dst.Call`), and rare paths in your own code (`w.Buggify`). Each seed
  draws its own mix of faults (swarm testing).
- **Everything replays**: same seed, same run, step for step, with an
  event log (`-dst.v`).
- **Time is free**: the simulator jumps to the next timer, so seconds of
  protocol time cost milliseconds (1,500–14,000× real time in the
  examples).
- **Bugs are named**: deadlocks and leaked goroutines are reported with
  what each goroutine waits for; panics with your stack; nondeterminism
  with the step where two runs diverged.

## Migrating

It's a mechanical substitution, and it can be done one package at a time
([docs/migrating.md](docs/migrating.md)):

| Go | World |
|---|---|
| `go f()` | `w.Go(f)` |
| `make(chan T, n)`, `c <- v`, `<-c`, `range c` | `dst.MakeChan[T](w, n)`, `c.Send(v)`, `c.Recv()`, `c.All()` |
| `select { ... }` | `dst.Select(w, dst.OnRecv(c, fn), dst.OnSend(c, v, fn), dst.OnDone(ctx, fn), dst.Default(fn))` |
| `time.Now`, `time.Sleep`, `time.NewTimer`, `time.After`, `time.Tick` | `w.Now()`, `w.Sleep(d)`, `w.NewTimer(d)`, `w.After(d)`, `w.Tick(d)` |
| `sync.Mutex`, `RWMutex`, `WaitGroup`, `Once`, `Cond`, `Pool` | `dst.Mutex`, `RWMutex`, `WaitGroup`, `Once`, `Cond`, `Pool` (zero values work) |
| `sync.OnceFunc`, `OnceValue`, `OnceValues` | `dst.OnceFunc`, `OnceValue`, `OnceValues` |
| `context.WithTimeout`, `WithCancel`, their `Cause` variants, `AfterFunc` | `dst.WithTimeout(w, ctx, d)`, `dst.WithCancel(w, ctx)`, `dst.WithTimeoutCause`, ..., `dst.AfterDone` |
| `runtime.Gosched` | `w.Yield()` |
| `math/rand/v2`, `math/rand` | `w.Rand()`, `w.MathRand()` (same names; `rand.N` is `dst.N(w.Rand(), n)`) |
| `crypto/rand.Read`, `Reader`, `Text` | `w.Rand().CryptoRead`, `dst.CryptoReader(w)`, `dst.CryptoText(w)` |
| `os.OpenFile`, `os.Rename`, ... | `w.FS().OpenFile`, `w.FS().Rename`, ... |
| `net.Listen`, `net.Dial` | `w.Net().Listen`, `w.Net().Dial` |
| `http.Serve`, `http.Client` | `dsthttp.Serve(w, ln, h)`, `dsthttp.Client(w)` |
| `grpc.NewServer`, `grpc.NewClient` | `dstgrpc.NewServer(w)`, `dstgrpc.NewClient(w, addr)` |

`dstlint` checks a package that says `//dst:simulated` for anything left
over:

```
go run github.com/prince776/go-dst-kit/dstlint/cmd/dstlint ./...
wal/log.go:42:9: time.Now escapes the World; use w.Now()
```

## Documentation

- [Migrating](docs/migrating.md): threading the World through, the
  substitutions, what still escapes.
- [Writing a simulation test](docs/writing-a-test.md): the harness,
  phases, turning an end-to-end test into a simulation test, histories,
  faults in your own fakes, replaying a failure.
- [Fault model](docs/fault-model.md): exactly what is simulated, and what
  isn't.
- [Research](docs/research.md): how other approaches to deterministic
  simulation compare.

## Packages

| Package | What |
|---|---|
| [`dst`](dst) | the World and everything code uses from it |
| [`realworld`](realworld) | the production World: plain goroutines, `os`, `net` |
| [`dsttest`](dsttest) | the runner: seeds, machines, faults, phases, invariants, replay |
| [`check`](check) | oracles: a linearizability checker, a KV model, a history recorder |
| [`dsthttp`](dsthttp) | `net/http` servers and clients over the World's network |
| [`dstgrpc`](dstgrpc) | gRPC servers and clients over it (own module: depends on grpc-go) |
| [`dstlint`](dstlint) | the analyzer (own module: depends on x/tools) |
| [`examples`](examples) | six systems, each with planted bugs the runner must catch (own module) |

The library module has no dependencies and needs Go 1.25.

## What it finds

Each example is a small, correct system plus switches for the classic
mistakes in it. The test asserts the correct one passes its seeds and
every planted bug is caught, for the right reason.

| Example | Planted bugs, all caught |
|---|---|
| [wal](examples/wal): checksummed write-ahead log | no fsync, no directory fsync, appending after a torn tail, a checksum that doesn't cover the length (zeros read as records; found only with holes) |
| [dedupe](examples/dedupe): exactly-once over retries | dedupe set in memory, "seen" logged separately from the effect |
| [objstore](examples/objstore): a fake object store with fault points, and a log client on it | blind retry after a timeout (duplicates), timeout reported as failure (lost ack), writes without an ETag precondition (lost update) |
| [heartbeat](examples/heartbeat): UDP failure detector | no sequence numbers (found only with UDP faults), sequence numbers that restart with the process, a monitor that learns members from heartbeats |
| [lease](examples/lease): lock service with leases and fenced writes | lease timed from the reply, no step-down, restarted server forgets, no drift margin, trusting the server's timestamp, no fencing (found only with pauses) |
| [gcounter](examples/gcounter): gossiped CRDT counter | merge by sum, rename without fsync |
| [twophase](examples/twophase): two-phase commit | participant decides alone, decision sent before logged, prepare not logged |
| [workerpool](examples/workerpool): bounded parallel map | `wg.Add` in the worker, goroutine leak on error, channel closed early |

Building them also found bugs in this library's own adapters and harnesses
(see [PLAN.md](PLAN.md#findings)).

## Adopted on real systems

Each adoption migrates an existing project onto a World (mostly
mechanically, with `dstmigrate`), models the faults inside its contract,
writes workloads and checks (each shown to fail on a bug planted for it),
and sweeps seeds. Every bug below has a test that fails on the original
code and passes with the fix; the forks' own records hold the evidence and
replay commands. Nothing has been reported upstream yet.

| Project | What it is | Found |
|---|---|---|
| [hashicorp/raft](https://github.com/prince776/raft/tree/dst) ([DST.md](https://github.com/prince776/raft/blob/dst/DST.md)) | Raft consensus (Consul, Nomad, Vault) | 9 bugs: a live majority that can never elect a leader (a lagging voter rejects the only candidate that could win); a crash inside bootstrap that strands a new server; a follower that trips over its own snapshot, rolls its state machine back, livelocks and panics (overlaps Antithesis's [#697](https://github.com/hashicorp/raft/issues/697)); a demoted leader that shuts down (a question for upstream); an unreadable newest snapshot that makes startup panic; a heartbeat handled off the main loop that sets a node's term back, on disk too (Antithesis's [#695](https://github.com/hashicorp/raft/issues/695), reached in ordinary sweeps on about 5% of seeds); a leadership transfer stuck on a channel send, after which the re-elected leader refuses every write (Antithesis's [#696](https://github.com/hashicorp/raft/issues/696), with its mechanism traced); a rejected pipelined append that deadlocks replication to that follower for good while heartbeats go on (the root cause of open [#612](https://github.com/hashicorp/raft/issues/612)); a snapshot install that keeps a log conflicting with the snapshot (Figure 13 rule 7, the other half of #697). Fixes with upstream-style tests on `fix/*` branches. |
| [rosedb](https://github.com/prince776/rosedb/tree/dst) ([DST.md](https://github.com/prince776/rosedb/blob/dst/DST.md)) | Bitcask key-value store and its WAL | 11 bugs, 7 not reported before, most of them losing acknowledged data: no directory fsync for new segments; merge output never fsynced; a torn tail that makes Open fail or panic; iterators racing a merge; a Close racing a merge that installs an empty merge (every key lost); `BytesPerSync` ignored; an unfinished merge installed on the next Open; a finished merge deleted unsafely; and, under disk errors, a failed merge that keeps serving a half-built index, writing on after a failed fsync (fsyncgate), and a re-run merge install that deletes its own segments. Four reproduced on unmodified upstream with plain tests, fixes on `fix/*` branches. |
| [go-diskqueue](https://github.com/prince776/go-diskqueue) ([README](https://github.com/prince776/go-diskqueue#bugs-found)) | NSQ's disk-backed queue | 5 bugs: no directory fsync after the metadata rename; Close persisting metadata before syncing the data; a crashed run's file reused in place, then read as corrupt (no power loss needed); reading past a rolled file (known upstream, [#38](https://github.com/nsqio/go-diskqueue/issues/38), found unprompted); a second path to overwriting a crashed run's file. |
| [LLM-Tor](https://github.com/prince776/LLM-Tor) ([DST.md](https://github.com/prince776/LLM-Tor/blob/dst/DST.md)) | A backend's payment and anonymous-token flows | 10 findings, 8 fixed: payments credited twice on webhook redelivery; lost updates handing out free tokens or erasing payments; retries charged twice; tokens left unspent after a failure; provider errors saved as answers; moderation that missed text across chunk boundaries; two open protocol decisions about token epochs. |
| [hashicorp/memberlist](https://github.com/prince776/memberlist/tree/dst) ([DST.md](https://github.com/prince776/memberlist/blob/dst/DST.md)) | Gossip membership and failure detection (SWIM, Lifeguard) | 3 bugs and a doc gap, each with a test that fails upstream and a fix: an ack meant for a restarted node's previous process completes its new process's probe of another node, so a dead node looks alive; `SendReliable` returns success for messages the receiver drops, against its "delivery is guaranteed" doc (251 losses in the baseline); events delivered after `Shutdown` leave the node lock held when a `ChannelEventDelegate` reader stops, hanging later calls; `GossipToTheDeadTime` only helps while a dead message is still queued. A blind benchmark of 11 open issues: none found, 7 reachable, each with the missing fault or check named. |

Each adoption also hardened this library: kills that keep the page cache,
file locks, gray failure, clock offsets and drift, a faster goroutine
identity check, and dstmigrate's `os` file I/O all came from what one of
them needed.

## How it works

Every simulated goroutine is a real goroutine, but only one runs at a
time: each World operation hands a baton back to the scheduler, which
picks the next runnable goroutine using the seed. Virtual time advances
only when every goroutine is blocked, straight to the next timer. Machines
group goroutines with a disk and a network host; crashing one kills its
goroutines without running defers and drops what its disk hadn't made
durable. [docs/fault-model.md](docs/fault-model.md) says exactly what
can and can't happen.

Because the code under test is ordinary Go, the same package runs in
production on the real World, and the same contract suite runs against
both Worlds to keep them honest.

## Limits

- **Only what goes through the World is simulated.** A dependency that
  starts its own goroutines, sleeps or does I/O escapes. The simulator
  notices a goroutine that blocks outside it (a watchdog) and runs that
  differ (the determinism check), and `dstlint` flags escapes in your own
  code; libraries need an adapter like `dsthttp`.
- **Map iteration order** can't be controlled. If it decides behaviour,
  the determinism check reports the seed; sort the keys.
- **Exploration is random**, not exhaustive: rare interleavings need many
  seeds. Run a long sweep nightly (`-dst.seeds=20000`).
- **Not modelled yet:** wall-clock jumps, bit rot.
- **Overhead in production** is small but not zero: a channel round trip
  costs about 295ns against 266ns raw and an uncontended mutex 14ns. The
  general `dst.Select` uses reflection (about 250ns against 54ns native);
  `dst.Select2` and `dst.Select3` are native (about 61ns, no allocation).

## Status

Pre-release: the API may still change. See [PLAN.md](PLAN.md) for what's
done, the decisions behind it, and what's next. Apache-2.0.
