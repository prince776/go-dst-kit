# Writing a simulation test

A `dsttest` test describes a system (machines running your code),
traffic (workload goroutines) and what must hold (invariants), and the
runner does the rest: it runs many seeds, injects faults, checks, and
prints a replay command for any failure.

```go
func TestKV(t *testing.T) {
	dsttest.Run(t, dsttest.Options{Seeds: 300, Faults: dsttest.AllFaults}, func(h *dsttest.Harness) {
		servers := map[string]*kv.Server{} // the current incarnation of each
		for _, name := range []string{"kv1", "kv2", "kv3"} {
			h.Machine(name, func(w *dst.World) {
				s := kv.Start(w, name, peers(name)) // recovers from w.FS()
				servers[name] = s
				s.Serve()
			})
		}
		hist := check.NewHistory[check.KVInput, check.KVOutput](h.World())
		for c := range 3 {
			h.Go(func(w *dst.World) { clientLoop(w, h, hist, c) })
		}
		h.Always("at most one leader per term", func() error { return oneLeader(servers) })
		h.Final("linearizable", func(*dst.World) error {
			return check.Linearizable(check.KVModel(), hist.Ops(), check.Options{}).Err()
		})
		h.Expect("leader changed")
	})
}
```

## The setup function

It runs **once per seed**, possibly in parallel with other seeds, so all
state the run uses must be created inside it.

- `h.Machine(name, start)` adds a machine and boots it. `start` runs on
  every boot, again after each restart, with a World whose disk keeps what
  was durable and whose network host is `name`. Other machines reach it at
  `name:port`. Pass `dsttest.NeverCrash` to keep the runner from crashing
  it (it can still be partitioned).
- `h.Go(fn)` starts a **workload** goroutine on the `test` machine, which
  never crashes. Workload goroutines, and any goroutines they start, must
  finish by the end of the run: one still blocked is reported as a
  deadlock or leak, with what it waits for. Loops meant to run forever go
  on `h.World().Go`.
- `h.Always(name, fn)` checks an invariant after **every** scheduling step.
  It runs between steps, so it may read your servers' fields without
  locks, but it must not use the World: no sleeps, channels, or methods
  that take a `dst.Mutex`. Keep it cheap.
- `h.Final(name, fn)` runs once, after the workload has finished. It may
  use the World: query the servers, crash them to judge their durable
  state (`m.Crash()` then read `m.World().FS()`).
- `h.Sometimes(name)` counts a behaviour; `h.Expect(names...)` fails the
  sweep if one never happened in any seed, so a test can't quietly stop
  exercising what it's for.
- `h.Fail(err)` or `h.Failf` from anywhere fails the seed (for checks the
  workload makes as it goes).

## Phases

Each seed runs:

1. **Faults** (`FaultPhase`, default 10s of simulated time): the runner
   crashes and restarts machines, pauses them, partitions the network
   (symmetric or one way), and lets disks tear writes or lose power
   mid-I/O, at rates drawn per seed. `Options.Faults` chooses the classes.
   Every machine also has its own clock, skewed within `Options.Clock`.
2. **Recovery** (`RecoveryPhase`, default 10s): every machine is up, the
   network is whole, and no more faults happen. A correct system must
   make progress here; this is where liveness bugs show up as stuck
   workload.
3. **Final checks**, if the workload finished.

If workload goroutines are still running when recovery ends, the runner
gives the fault-free system one more recovery phase to tell two cases
apart. Goroutines that finish then are reported as **slow**, with how
late they were: the phase is too short, or progress is too slow. Those
still blocked are **stuck**: a deadlock, a leak or lost liveness.

A workload with a fixed amount of work and long timeouts may not fit in
the run. Stop starting operations once the fault phase is over:

```go
for range 100 {
	if h.Phase() != dsttest.Faulty {
		return
	}
	doOneOperation(w)
}
```

## Turning an end-to-end test into a simulation test

An existing scenario (ingest data, run GC, check the result) makes a good
simulation test. Run it repeatedly while faults happen, once more after
they stop, then check the end state, with invariants checked throughout:

```go
func TestIngestAndGC(t *testing.T) {
	dsttest.Run(t, dsttest.Options{Faults: dsttest.AllFaults, FaultPhase: 10 * time.Second, RecoveryPhase: 30 * time.Second},
		func(h *dsttest.Harness) {
			store := fakestore.NewService() // a service you don't run: survives crashes
			model := newModel()             // what should exist: survives crashes
			quietRunDone := false
			h.Machine("client", func(w *dst.World) { // runs again after every restart
				db := Open(w, store.Bucket(w)) // recover from durable state
				for h.Phase() == dsttest.Faulty {
					RunIngestAndGC(w, db, model)
				}
				RunIngestAndGC(w, db, model) // faults have stopped: every check applies
				quietRunDone = true
			})
			h.Always("no live data lost, no dangling references", func() error { return checkSafety(store, model) })
			h.Final("the end state matches the model", func(w *dst.World) error {
				if !quietRunDone {
					return errors.New("the run after recovery never finished")
				}
				return checkEndState(w, store, model)
			})
			h.Expect("gc cycle completed", "gc cycle interrupted")
		})
}
```

**Let the runner turn faults on and off.** It draws fault rates per seed
(some seeds have one crash, some many, some none), times the phases and
makes every failure replayable. The scenario reads `h.Phase()`; it
doesn't inject faults itself.

**Put each piece where it lives in production.** Code that dies with the
process (the database library, its GC) runs in a machine's start
function, which runs again after each restart and must recover from
durable state; anything holding a handle to it must use the current
incarnation's. Services you don't run (fakes, with fault points) and the
model are created in the setup function, outside every machine, so they
survive crashes.

**Write checks that stay true under faults.** A step-by-step check like
"after GC, the deleted items are gone" holds only if the GC finished, and
under faults it may not have. Split checks in two:

- *Always true*, even mid-operation or right after a crash: no live data
  lost, no dangling reference, nothing deleted while still referenced.
  These go in `h.Always`, or run unconditionally in the scenario.
- *True once an operation completes*: reclaimed items are gone, packs are
  consolidated, counts match. Run these only when the **system's durable
  record** says the operation completed (a completed-cycle number in the
  GC's metadata, say), not when the call returned nil: a call can fail
  after completing (a lost reply, a crash after the commit) or return
  before its result is durable. Mind what the operation could see: a GC
  cycle reclaims what was garbage when it started, so check only the
  deletions made before that.

In the quiet run after recovery everything completes, so every check
applies; checking that the quiet run finished (as `quietRunDone` above)
makes sure it did.

**Compare against the model, not absolute numbers.** The quiet run
starts from whatever the faulty runs left (half-finished cycles, extra
packs). "Exactly N packs" breaks; "nothing live lost, nothing
reclaimable left, counts match the model" holds.

**Start with faults off.** `Faults` left zero runs the scenario with exact
checks across hundreds of interleavings: concurrency bugs first. Then
turn on crashes and disk faults, then fault points, adjusting checks as
each class goes on.

**Many seeds, a few runs each.**

- Loop the scenario for the whole fault phase: repeated runs reach states
  one run never does (GC over an earlier GC's output, a crash while
  recovering from a crash).
- Most coverage comes from seeds: hundreds in CI, tens of thousands
  nightly from fresh seeds. Keep each seed to seconds of simulated time,
  so failures replay and debug fast.
- Don't limit a seed to one failure: the swarm already gives some seeds a
  single fault, and many bugs need two.
- Draw sizes, orders and ratios from `w.Rand()`, so seeds vary the data,
  not just the timing.
- For a narrow window (between copying a pack and deleting its source),
  put a fault point or `w.Buggify` there: random crashes rarely land in
  it.
- `h.Expect` both outcomes (completed and interrupted) so a sweep where
  every cycle was interrupted, or none was, doesn't pass quietly.

**Phases are timed, not driven by the scenario.** Set `FaultPhase` to a
few runs' worth of simulated time, and `RecoveryPhase` to comfortably
more than one clean run.

## Recording client histories

For linearizability, record each operation's call and outcome with the
World's clock:

```go
call := hist.Invoke(client, check.KVInput{Op: check.KVPut, Key: k, Value: v})
switch err := put(w, k, v); {
case err == nil:
	call.Return(check.KVOutput{})
case errors.Is(err, errNotApplied): // the server said it definitely didn't happen
	call.Fail()
default: // timeout: it may or may not have happened
	call.Abandon()
}
```

Getting the third case right matters: a timed-out write may still take
effect later, and the checker allows exactly that.

## Designing the workload

The simulation explores timing: interleavings, crashes, lost replies. It
can only find bugs in what the workload does and what the fakes can say.
Most of a sweep's power comes from these choices.

- **Do what the real clients do.** Retry the same request (same request
  ID, same body) after an error or a timeout. Renew a credential the
  server reports expired. Resend after a lost reply and expect the same
  answer. Go away and come back later. A client that does less misses
  bugs; one that does things the real one never does reports bugs that
  aren't.
- **Several clients per identity.** One user on two devices at once
  sends concurrent requests that a per-user lock, a per-session cache or a
  "re-fetch before write" may not cover.
- **Every writer of shared state.** List what writes each record (an
  account, a balance, a document) and make the workload call all of them,
  often. Bugs between writers (lost updates, double counting) need two of
  them in the same window.
- **Every kind of answer.** Not only success: the outcomes the code
  branches on (not found, refused, blocked, quota exceeded), and inputs
  near limits (long prompts, many items, sizes near chunk or page
  boundaries).
- **Misuse a real client can send.** Replays, a spent token on a new
  request, a stale version. Check they're refused.
- **A model outside the machines.** Record what each client was told and
  what it sent before it sent it (a request whose reply is lost may still
  have taken effect), and check the system's final state against it.
- **Coverage.** `h.Sometimes` each kind of operation and outcome, and
  `h.Expect` the ones a sweep must reach.
- **Check only what is observable.** A goroutine records what it received
  after its receive returns, and the scheduler may run others in between:
  with two consumers, the order the model records is not the order of the
  handoffs. Check what each consumer saw (its own order), what all of them
  saw together (no duplicates, nothing missing), and keep exact ordering
  checks for a run with one consumer. Most false failures while writing a
  model come from checking an order nothing could observe.
- **Rare inputs need many seeds or a swarm over inputs.** A bug that
  needs a value in a narrow range (a word straddling a 9,000-byte chunk
  boundary) shows up in a fraction of a percent of seeds: run long sweeps,
  and draw sizes near the boundaries the code has.

### What a fake should answer

Model the service's documented failure modes, not only its happy path:

| Service | Model at least |
|---|---|
| A database (Cosmos, DynamoDB, Postgres) | throttling (429 or its equivalent: the request didn't run), timeouts that may have committed (`FailAfter`), not found, precondition failed on a conditional write, latency |
| An object store | not found, precondition failed, throttling (503 slow down), a write that committed but whose reply was lost |
| A key service (KMS, Key Vault) | throttling, unavailable, a key that is disabled |
| An HTTP API (LLM provider, payments) | error answers: 4xx for bad requests, 429 with retry-after, 500/503 overloaded; slow answers; answers that are errors in a 200 |
| A webhook sender | at-least-once delivery, duplicates, any order, retries after a non-2xx or a timeout |

## Faults in your own fakes

### Which tool for which fault

| What goes wrong | Use | You write |
|---|---|---|
| Your process dies (SIGKILL, power loss) | machine crashes (`Faults.Crash`) | nothing: the code must recover on restart |
| Your disk loses or tears unsynced writes | `w.FS()` (`Faults.Disk`) | nothing |
| The network between your own machines | `w.Net()` (`Faults.Partition`, `Network`) | nothing |
| Your process stalls (GC pause, VM freeze) | `Faults.Pause` | nothing |
| A service you don't run fails, times out or is slow | fault points in a wrapper around its fake (`Faults.Points`) | one `dst.Call` per method |
| That whole service goes down | outages (`Faults.Services`) | nothing beyond `service.op` names |
| A service answers, with an error (503 overloaded, 429, a 5xx) | `dst.Inject` in the fake, returning that response | the error response |
| Your own code should sometimes take a rare legal path | `w.Buggify` | the other path |

`dst.Call` models the request failing or its reply being lost; it can't
make the service *answer* with an error. Real services do both, and code
often handles them differently (an error response is easy to mistake for
an answer and save), so model the answers too.

Put fault points at the boundary, one per remote round trip. Don't use
them to fake crashes or network faults in your own system: the runner
does those, more realistically.

### A fake, and a wrapper that adds faults

Keep the fake plain: a correct, in-memory implementation of the client
interface, usable in any test. Add faults in a wrapper with the same
interface, one `dst.Call` per method:

```go
// Faulty wraps any Store with fault points.
func Faulty(w *dst.World, s Store) Store { return faulty{w, s} }

var (
	readErrs  = dst.Errors{Before: unavailable, After: unavailable} // a lost read reply changes nothing
	writeErrs = dst.Errors{Before: unavailable, After: timedOut}    // a lost write reply is ambiguous
)

func (f faulty) Put(ctx context.Context, key string, data []byte, ifMatch string) (string, error) {
	return dst.Call(ctx, f.w, "objstore.put", writeErrs, func() (string, error) {
		return f.s.Put(ctx, key, data, ifMatch)
	})
}
```

`dst.Call` decides, per call: **None** runs the operation and returns its
result; **Fail** returns `Errors.Before()` without running it (refused:
nothing happened); **FailAfter** runs it and returns `Errors.After()`
instead of its result (it happened, the reply was lost). Injected
latency waits on the caller's context: if that ends first, Call returns
the context's error and the operation still happens unless the decision
was Fail. So the fake must act like a server that has the request: it
shouldn't give up because the caller's context is done. A context
already done when the call starts isn't sent at all. In the real World,
Call just calls through. [`examples/objstore`](../examples/objstore) is
this pattern in full.

If you write wrappers with pre- and post-call error hooks today, Fail is
the pre-call error and FailAfter the post-call one; the difference is
that the runner chooses them from the seed, so a failure replays, rates
vary per seed, whole services go down, and each firing is counted.

### Choosing the errors

Return what your real client returns in that situation. If the real
service distinguishes "definitely not done" (a 503 before processing, a
failed precondition) from "don't know" (a timeout), map Fail to the
first and FailAfter to the second. If the real client can't tell them
apart, use the same error for both: that forces your code to treat every
error as possibly applied, which is what production demands. Then the
code under test should treat only errors that are certainly refusals as
"didn't happen"; everything else, including a context deadline, is
ambiguous.

### Kinds of operation

- **A read** has no side effects, so a lost reply looks exactly like a
  refused request: map both faults to the same error.
- **A write** is where FailAfter matters: the write is applied and the
  caller sees an error (the lost acknowledgement).
- **A read with side effects** (a queue receive that consumes a message,
  a read that renews a lease) is like a write: FailAfter means the effect
  happened but the caller never heard.
- **A multi-step operation** (a multipart upload, a paginated list) gets
  one point per step: each is its own round trip.

### Names, rates and coverage

- **Names** are `service.operation`. With `Faults.Services` on, the runner
  takes a whole service down for a while (every point of it fails), as a
  real outage would.
- **Rates** are drawn per seed from a stream named after the point: quiet
  in a third of seeds, else failing 0.1% to 20% of calls (a random share
  of them after acting), sometimes slowed by 1ms to 1s. Adding a point
  doesn't change the other points' draws.
- **Overrides**: `Options.Points` sets points by name prefix, the longest
  prefix winning: `{"table.": {}}` silences the table,
  `{"table.insert": {FailAfter: 0.1}}` loses 10% of insert replies.
- **Coverage**: each firing counts as `point NAME: fail`, `: fail-after`,
  `: slow` or `: outage`, so `h.Expect("point table.insert: fail-after")`
  makes sure the ambiguous case was reached.
- Points fire only in the fault phase.

`dst.Inject(w, name)` is the lower-level form, returning the decision
(after sleeping its latency, without watching a context) for a fake that
handles faults itself; `dst.Do` is Call for an operation that returns
only an error.

### Buggify

`w.Buggify(name)` (after FoundationDB's BUGGIFY) is a point in your own
code: it returns true now and then in a simulation, so code can take a
legal but rare path (a tiny buffer, an early flush, a spurious retry) to
shake out bugs that path hides. Each point is active in about a quarter
of seeds and then fires on about a quarter of calls; it is always false
in the real World.

## Running

```sh
go test -run TestKV                          # the sweep (10 seeds with -short)
go test -run TestKV -dst.seeds=20000 -dst.first=random   # a long sweep from fresh seeds (nightly)
go test -run TestKV -dst.seed=2891 -dst.v    # replay one seed with its event log
```

The first 5 seeds, and every failing seed, run twice to check that the
system is deterministic (`Options.DeterminismChecks`). `-dst.parallel`
sets how many seeds run at once (default GOMAXPROCS).

## Measuring exploration: dstbench

Each bug a sweep finds and you fix is ground truth: a known way the code
was wrong. Keep them re-insertable, and you can measure whether a change
to the sweep, the workload or the library finds bugs more often.

1. Re-insert each fixed bug behind a switch that only a build tag can
   turn on, so normal builds compile the buggy paths out:

   ```go
   //go:build !dstbugs
   package mypkg
   func bug(name string) bool { return false }
   ```

   ```go
   //go:build dstbugs
   package mypkg
   var activeBug = os.Getenv("DSTBUG")
   func bug(name string) bool { return name != "" && name == activeBug }
   ```

   ```go
   if !bug("close-without-fsync") {
       f.Sync()
   }
   ```

   Re-insert the bug as it was, not a stronger version: an easier bug
   flatters the sweep. A check: with the bug re-inserted, the sweep should
   first fail at the seed that originally found it.
2. List the bugs in `dstbench.json`, each with the sweeps that should
   catch it and, optionally, a `Sometimes` marker for its precondition:

   ```json
   {"package": ".", "tags": "dstbugs", "env": "DSTBUG",
    "baseline": ["TestDSTCrashes"],
    "bugs": [{"name": "close-without-fsync", "tests": ["TestDSTCrashes"], "reached": "clean close and reopen"}]}
   ```
3. Run `go run github.com/prince776/go-dst-kit/cmd/dstbench -seeds 300`.
   It reports, per bug and sweep, the failure rate per seed (with a 95%
   interval), the seeds and CPU time to the first failure, and how often
   the precondition was reached; and runs the baseline sweeps with no bug,
   which must pass.

A bug whose precondition is reached often but which fails rarely needs
better timing or inputs, not more of the same; one never reached needs
the workload to do something new.

Use it as a floor, not a target. Mark a few bugs `"held_out": true`
(skipped unless `-held-out`) and look at them only to check a change at
the end; the measure that matters is new bugs found in code not yet
explored.

## When a seed fails

The report says which check failed and the seed's fault mix. Replay it
with `-dst.v` to get every scheduling decision, message, crash and
partition, with simulated timestamps; add `w.Logf` calls to your code to
see your own state in the same log. The seed fails the same way every
time, so a debugger or extra logging doesn't make it go away.

A seed is entropy, not an input: once the code or the workload changes,
the same seed explores a different run. So a pinned seed (`dsttest.RunSeed`)
only reproduces its bug until the next change, the fix included. To keep
a fixed bug fixed, write a small ordinary test of the exact scenario the
trace showed (it should fail when the fix is reverted), and keep the sweep
that found it.

## Sweeps that pass for the wrong reason

A passing sweep only means something if the faults could reach the code
that matters. Each of these made a real sweep pass while bugs were there.

- **Crashes with nothing slow.** If your fakes answer instantly, a request
  takes no simulated time: crashes can only land between requests, never
  between a request's steps. Pair `Faults.Crash` with latency on your
  fault points (`Options.Points` with `Slow`), so crashes land mid-request.
  One service passed 1,000 crash-only seeds and failed most seeds once its
  calls were slow.
- **Clients that don't behave like the real ones.** A test client that
  gives up, changes its request on a retry, or ignores a response flag the
  real client acts on makes failures that aren't bugs, and hides the ones
  that are. Write it to do what the real client does: retry the same
  request, renew credentials it's told have expired, resend after a lost
  reply.
- **A workload that skips writers.** When a sweep finds a bug in how some
  state is written (a lost update, say), find every other writer of that
  state and make the workload call them too. Fixing two writers and not
  the third passes the sweep.
- **Coverage you didn't check.** `h.Sometimes` the things that must happen
  (a payment credited, a token redeemed, a crash mid-request) and
  `h.Expect` them, so a workload that quietly stops doing them fails.
- **Inputs that change per process.** Anything the simulation uses that
  varies from one process to the next breaks replay across processes, and
  the determinism check can't see it (it reruns a seed in the same
  process). Keys generated at test start are the usual culprit: crypto
  that rejection-samples from `dst.CryptoReader` takes a key-dependent
  number of bytes from the World's stream, shifting every later draw. Load
  fixed test keys from testdata.
- **Dates.** Every simulation starts at 2030-01-01 00:00 UTC, with machine
  clocks up to `Clock.MaxOffset` apart, so code that buckets by day, month
  or year straddles a boundary at the start of every seed. That finds
  boundary bugs; it also means nothing in your sweep starts mid-month.

## Reading many failures

With `Options.KeepGoing`, a sweep reports every failing seed. Group them
by which guarantee failed, and by direction where there is one (credited
more or less than paid, charged more or less than delivered): each group
is usually one bug, and its first seed is the one to trace. Replay it with
`-dst.v` and enough `w.Logf` in your fakes (the writes to the state the
check is about) to see the interleaving.
