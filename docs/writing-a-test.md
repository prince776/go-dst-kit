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
  locks, but it must not use the World. Keep it cheap.
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

## Faults in your own fakes

A service you don't simulate (cloud storage, a managed database, a queue)
gets a fake in tests. Put fault points in the fake and the runner makes it
fail for you, the same way it crashes machines:

```go
func (t *FakeTable) Insert(ctx context.Context, e Entity) error {
	switch dst.Inject(t.w, "table.insert") {
	case dst.Fail: // refused: nothing happened
		return &ResponseError{StatusCode: 503}
	case dst.FailAfter: // it happened, but the caller never hears
		t.insert(e)
		return context.DeadlineExceeded
	}
	return t.insert(e)
}
```

`dst.Do(w, name, op)` is the short form: it returns `dst.ErrInjected`
without running op on Fail, runs op and returns `dst.ErrInjected` on
FailAfter. Inject also sleeps any latency it injects. In the real World a
point is always quiet.

- **Names** are `service.operation`. With `Faults.Services` on, the
  runner takes a whole service down for a while (every point of it
  fails), as a real outage would.
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

`w.Buggify(name)` (after FoundationDB's BUGGIFY) is a point in your own
code: it returns true now and then in a simulation, so code can take a
legal but rare path (a tiny buffer, an early flush, a spurious retry) to
shake out bugs that path hides. Each point is active in about a quarter
of seeds and then fires on about a quarter of calls; it is always false
in the real World. [`examples/objstore`](../examples/objstore) has a fake
store and a client that handles the ambiguous case.

## Running

```sh
go test -run TestKV                          # the sweep (10 seeds with -short)
go test -run TestKV -dst.seeds=20000 -dst.first=random   # a long sweep from fresh seeds (nightly)
go test -run TestKV -dst.seed=2891 -dst.v    # replay one seed with its event log
```

The first 5 seeds, and every failing seed, run twice to check that the
system is deterministic (`Options.DeterminismChecks`). `-dst.parallel`
sets how many seeds run at once (default GOMAXPROCS).

## When a seed fails

The report says which check failed and the seed's fault mix. Replay it
with `-dst.v` to get every scheduling decision, message, crash and
partition, with simulated timestamps; add `w.Logf` calls to your code to
see your own state in the same log. The seed fails the same way every
time, so a debugger or extra logging doesn't make it go away.

Once fixed, keep the seed in a regression test with `dsttest.RunSeed` if
the bug was subtle: sweeps start from seed 1, but a fix to the harness can
change which seeds reach the bug.
