# Migrating code to a World

Code becomes simulatable by taking a `*dst.World` and using it, instead
of the language and standard library, for everything that waits or is
nondeterministic. Nothing else changes: the structure, the goroutines and
the channels all stay.

## 1. Thread the World through

Pass the World the way you pass a logger: to constructors, kept in a
field.

```go
type Server struct {
	w  *dst.World
	mu dst.Mutex
	// ...
}

func NewServer(w *dst.World, cfg Config) *Server { return &Server{w: w} }
```

In `main`, the World is real:

```go
w := realworld.New(realworld.Options{Dir: *dataDir}) // w.FS() is rooted at dataDir
srv := NewServer(w, cfg)
```

In tests, `dsttest` gives each simulated machine its own World.

**Last resort: `dst.Current()`.** Leaf code that no World can reach (a
package-level helper called from everywhere, with no owner to pass it
one: an assertion helper's random choice, say) can ask for the calling
goroutine's World. A goroutine a simulated World started gets its
machine's World; any other goroutine gets the real World, silently, even
during a simulation, and only the determinism check notices what that
changes. So pass a World wherever one is reachable. `dstlint` reports
every use of `dst.Current`, in every package; excuse each with `//dst:ok`
and why no World reaches it.

## 2. Substitute

| Go | World | Notes |
|---|---|---|
| `go f()` | `w.Go(f)` | `w.GoNamed("name", f)` names it in reports |
| `make(chan T, n)` | `dst.MakeChan[T](w, n)` | the zero `Chan` is a nil channel |
| `c <- v`, `v, ok := <-c`, `close(c)` | `c.Send(v)`, `v, ok := c.Recv()`, `c.Close()` | same panics as Go (send on closed, close twice) |
| `for v := range c` | `for v := range c.All()` | |
| `select` | `dst.Select(w, cases...)` | see below |
| `<-ctx.Done()` | `dst.Done(ctx).Recv()` or `dst.OnDone(ctx, fn)` | |
| `time.Now`, `Since`, `Until` | `w.Now()`, `w.Since(t)`, `w.Until(t)` | `time.Time` and `time.Duration` are unchanged |
| `time.Sleep(d)` | `w.Sleep(d)` | |
| `time.NewTimer`, `After`, `AfterFunc`, `NewTicker` | `w.NewTimer(d)`, `w.After(d)`, `w.AfterFunc(d, f)`, `w.NewTicker(d)` | timers have `.C`-style channels as `dst.Chan` |
| `sync.Mutex`, `RWMutex`, `WaitGroup`, `Once` | `dst.Mutex`, `dst.RWMutex`, `dst.WaitGroup`, `dst.Once` | zero values work; `wg.Go(w, f)` too |
| `sync.NewCond(l)`, `sync.Cond` | `dst.NewCond(l)`, `dst.Cond{L: l}` | same semantics: a Signal with no waiter is lost |
| `context.WithCancel`, `WithTimeout`, `WithDeadline` | `dst.WithCancel(w, ctx)`, `dst.WithTimeout(w, ctx, d)`, `dst.WithDeadline(w, ctx, t)` | `Background`, `WithValue` are fine as they are |
| `context.AfterFunc` | `dst.AfterDone(ctx, f)` | |
| `runtime.Gosched` (a spin waiting for another goroutine) | `w.Yield()` | in simulation it returns once another goroutine has made progress or time has advanced; a spin no one can end is a deadlock |
| `runtime.Gosched` (a turn for others, in a loop that makes its own progress) | `w.Gosched()` | a plain scheduling point: the goroutine stays runnable. dstmigrate writes `w.Yield()` for every Gosched (the safe default for spins); switch these by hand. The wrong choice shows as simulated time jumping to the next timer, and as a liveness hang when that timer is the end of the run. Rule: if the loop's exit condition depends on another goroutine, it is a spin (Yield); if the loop ends by its own work, Gosched |
| `math/rand` | `w.Rand()` | seeded per machine in simulation |
| `crypto/rand.Read`, `crypto/rand.Reader` | `w.Rand().CryptoRead`, `dst.CryptoReader(w)` | crypto/rand in the real World; seeded in simulation (key generation in the crypto packages doesn't replay whatever reader it gets: give simulated machines fixed keys) |
| `os.OpenFile`, `Rename`, `Remove`, `Mkdir`, `Stat`, `ReadDir` | `w.FS().OpenFile`, ... | paths are relative to the World's root |
| `os.ReadFile`, `WriteFile`, `Create`, `Open` | `dst.ReadFile(w.FS(), ...)`, ... | `dst.SyncDir(fsys, dir)` to fsync a directory |
| `os.Link` (a hard link) | `dst.Link(w.FS(), old, new)` | like a create, the new name is durable once its directory is synced |
| `syscall.Flock(fd, LOCK_EX\|LOCK_NB)` on a lock file (gofrs/flock's `TryLock`) | `unlock, err := dst.LockFile(w.FS(), name)` | `dst.ErrLocked` when held; released by `unlock` or the process's death; unix only in the real World |
| `net.Listen`, `net.Dial`, `DialContext` | `w.Net().Listen`, `w.Net().Dial`, `w.Net().DialContext` | returns ordinary `net.Listener` and `net.Conn` |
| `http.Serve`, `http.DefaultClient` | `dsthttp.Serve(w, ln, h)`, `dsthttp.Client(w)` | handlers are unchanged |
| `grpc.NewServer`, `grpc.NewClient` | `dstgrpc.NewServer(w)`, `dstgrpc.NewClient(w, target)` | generated stubs are unchanged |

What doesn't wait needs no change: `sync/atomic`, `sync.Map`, encoding,
`strings`, `errors`, pure computation.

### select

Each case is a function that runs if that case is chosen. Use a variable
for what a `break` or `return` would have done:

```go
// before
select {
case v := <-in:
	handle(v)
case <-ctx.Done():
	return ctx.Err()
case <-time.After(time.Second):
	log.Print("idle")
}

// after
done := false
dst.Select(w,
	dst.OnRecv(in, func(v Msg, ok bool) { handle(v) }),
	dst.OnDone(ctx, func() { done = true }),
	dst.OnRecv(w.After(time.Second), func(time.Time, bool) { w.Logf("idle") }),
)
if done {
	return ctx.Err()
}
```

Like Go, `Select` picks at random among ready cases (in simulation, by the
seed), runs `dst.Default` if nothing is ready and there is one, and
otherwise waits.

In the real World `Select` uses reflection, about 4x a native `select`.
On hot paths with two or three cases and no default, use `dst.Select2` or
`dst.Select3`: the same cases, compiled to a native `select`, with no
allocation:

```go
dst.Select2(w,
	dst.OnRecv(in, func(v Msg, ok bool) { handle(v) }),
	dst.OnDone(ctx, func() { done = true }),
)
```

## 3. Things that still escape

- **Map iteration order** decides behaviour if you act on map entries in
  order (send to peers, write records, pick "the first"). Sort the keys.
  The runner's determinism check reports seeds whose two runs differ.
- **Libraries that wait on their own**: anything that starts goroutines,
  sleeps, or does I/O inside. Wrap them behind an interface and give the
  simulation a World-based implementation, or use an adapter (`dsthttp`,
  `dstgrpc`). For a client of a service you don't run (cloud storage, a
  managed database), the implementation is a fake: give it fault points
  (`dst.Inject`) so the runner can make the service fail. A receive-only channel a library hands you can be used in
  the real World with `dst.RecvOnly`, not in a simulated one.
- **Raw blocking** (a plain channel, `sync.Mutex` held while someone waits
  for it, `time.Sleep`, real I/O) stalls the simulator's single baton. It
  reports this after 10s of wall time (the watchdog) with the goroutine's
  stack, rather than hanging.
- **A leftover `go` statement** whose goroutine uses the World is reported
  as a misuse, with that goroutine's stack.

## dstmigrate

`go run github.com/prince776/go-dst-kit/dstlint/cmd/dstmigrate -world
'Server=$.w' ./pkg` does the substitutions above in place. `-world` names
the World expression for each receiver type (`$` is the receiver);
elsewhere it writes `__w`, which won't compile until a person passes a
World in. A project's own wrappers around the clock (or anything else
that needs a World) are rewritten with `-map`: each call becomes a call
of a function taking the World first, then the receiver for a method:

```
-map 'example.com/clock.Now=example.com/app/dstclock.Now,(example.com/clock.Mono).Elapsed=example.com/app/dstclock.Elapsed'
```

turns `clock.Now()` into `dstclock.Now(w)` and `m.Elapsed()` into
`dstclock.Elapsed(w, m)`, importing `dstclock`.

## 4. Do it a package at a time

1. Mark the package: `//dst:simulated` in any file.
2. Run `dstlint` on it; fix what it reports, or excuse a line with
   `//dst:ok reason`.
3. Run the package's existing tests against `realworld.New(...)`: behaviour
   in production must not change.
4. Write a `dsttest` test for it ([writing-a-test.md](writing-a-test.md)).

Code that isn't migrated yet can still be called from simulated code as
long as it doesn't block or do I/O. Everything stays correct in the real
World throughout, so the migration can be merged as it goes.
