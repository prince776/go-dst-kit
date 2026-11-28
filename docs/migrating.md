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
w := realworld.New(realworld.Options{}) // w.FS() is the os package: names as the code gives them
srv := NewServer(w, cfg)
```

In production leave `Dir` empty: the World's filesystem is then exactly
the os package (absolute names, names relative to the working directory,
`..`), so migrated code opens what it opened before. `Dir` roots the
filesystem for tests and tools, and then refuses absolute and escaping
names as a simulated disk does: a simulated machine's disk is its own,
so code meant for both Worlds uses names relative to a data directory it
is given.

In tests, `dsttest` gives each simulated machine its own World.

**A library** whose constructors have callers it can't change
(`yamux.Server(conn, cfg)`, `raft.NewRaft(conf, ...)`) adds a World field
to the options it already takes, and uses the real World when it's nil,
so every existing caller keeps working unchanged:

```go
type Config struct {
	// ...
	// World is what the library waits, sleeps, locks and spawns with.
	// Nil means the real World: plain goroutines, time and os.
	World *dst.World
}

func (c *Config) world() *dst.World {
	if c.World == nil {
		return realworld.Default()
	}
	return c.World
}
```

`realworld.Default()` is one real World for the whole process, made at
start; it is also the World `dst.Current` gives goroutines no simulation
started. Read the World once, in the constructor, into a field: a World
must not change under a running instance.

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
| `time.NewTimer`, `After`, `AfterFunc`, `NewTicker`, `Tick` | `w.NewTimer(d)`, `w.After(d)`, `w.AfterFunc(d, f)`, `w.NewTicker(d)`, `w.Tick(d)` | timers have `.C`-style channels as `dst.Chan` |
| `sync.Mutex`, `RWMutex`, `WaitGroup`, `Once`, `Pool` | `dst.Mutex`, `dst.RWMutex`, `dst.WaitGroup`, `dst.Once`, `dst.Pool` | zero values work; `wg.Go(w, f)`, `TryLock`, `TryRLock`, `RLocker` too |
| `sync.OnceFunc`, `OnceValue`, `OnceValues` | `dst.OnceFunc(f)`, `dst.OnceValue(f)`, `dst.OnceValues(f)` | work in a simulation even when made outside one (a package-level variable) |
| `sync.NewCond(l)`, `sync.Cond` | `dst.NewCond(l)`, `dst.Cond{L: l}` | same semantics: a Signal with no waiter is lost |
| `context.WithCancel`, `WithTimeout`, `WithDeadline` | `dst.WithCancel(w, ctx)`, `dst.WithTimeout(w, ctx, d)`, `dst.WithDeadline(w, ctx, t)` | `Background`, `WithValue` are fine as they are |
| `context.WithCancelCause`, `WithTimeoutCause`, `WithDeadlineCause` | `dst.WithCancelCause(w, ctx)`, ... | `context.Cause` works on them as on std's |
| `context.AfterFunc` | `dst.AfterDone(ctx, f)` | f runs in its own goroutine, as with context.AfterFunc |
| `runtime.Gosched` (a spin waiting for another goroutine) | `w.Yield()` | in simulation it returns once another goroutine has made progress or time has advanced; a spin no one can end is a deadlock |
| `runtime.Gosched` (a turn for others, in a loop that makes its own progress) | `w.Gosched()` | a plain scheduling point: the goroutine stays runnable. dstmigrate writes `w.Yield()` for every Gosched (the safe default for spins); switch these with `-gosched`. The wrong choice shows as simulated time jumping to the next timer, and as a liveness hang when that timer is the end of the run. Rule: if the loop's exit condition depends on another goroutine, it is a spin (Yield); if the loop ends by its own work, Gosched |
| `math/rand/v2` (`rand.N`) | `w.Rand()`, same names (`dst.N(w.Rand(), n)`) | seeded per machine in simulation |
| `math/rand` (v1) | `w.MathRand()`, same names | math/rand itself in the real World: `Seed` and the GODEBUGs keep their meaning |
| `crypto/rand.Read`, `Reader`, `Text` | `w.Rand().CryptoRead`, `dst.CryptoReader(w)`, `dst.CryptoText(w)` | crypto/rand in the real World; seeded in simulation (key generation in the crypto packages doesn't replay whatever reader it gets: give simulated machines fixed keys) |
| `os.OpenFile`, `Rename`, `Remove`, `Mkdir`, `Stat`, `ReadDir` | `w.FS().OpenFile`, ... | in production names reach os as they are; a simulated disk is rooted (names relative to it) |
| `os.ReadFile`, `WriteFile`, `Create`, `Open` | `dst.ReadFile(w.FS(), ...)`, ... | `dst.SyncDir(fsys, dir)` to fsync a directory |
| `os.Chmod`, `Chtimes`, `Truncate`, `MkdirTemp`, `CreateTemp`, `DirFS` | `dst.Chmod(w.FS(), ...)`, ... | os's own in production; a simulated disk names temporary files from a counter and puts `MkdirTemp("", ...)` in `tmp`; modes and times aren't crash-modelled |
| `filepath.Walk`, `WalkDir`, `Glob` | `dst.Walk(w.FS(), ...)`, ... | filepath's own in production (symbolic links as there) |
| `(*os.File).Chmod`, `Readdir`, `Readdirnames` | `dst.FileChmod(f, m)`, `dst.Readdir(f, n)`, `dst.Readdirnames(f, n)` | the `*os.File`'s own methods in production |
| `os.Link` (a hard link) | `dst.Link(w.FS(), old, new)` | like a create, the new name is durable once its directory is synced |
| `syscall.Flock(fd, LOCK_EX\|LOCK_NB)` on a lock file (gofrs/flock's `TryLock`) | `unlock, err := dst.LockFile(w.FS(), name)` | `dst.ErrLocked` when held; released by `unlock` or the process's death; unix only in the real World |
| `net.Listen`, `net.Dial`, `DialContext`, `DialTimeout` | `w.Net().Listen`, `w.Net().Dial`, `w.Net().DialContext`, `dst.DialTimeout(w, ...)` | returns ordinary `net.Listener` and `net.Conn` |
| `io.Pipe`, `net.Pipe` | `dst.Pipe(w)`, `dst.NetPipe(w)` | io's and net's own pipes in the real World |
| `http.Serve`, `ListenAndServe`, `http.Get` (`Post`, ...), `http.DefaultClient` | `dsthttp.Serve(w, ln, h)`, `dsthttp.ListenAndServe(w, addr, h)`, `dsthttp.DefaultClient(w).Get`, `dsthttp.DefaultClient(w)` | handlers are unchanged; `DefaultClient` is `http.DefaultClient` in the real World |
| `grpc.NewServer`, `grpc.NewClient` | `dstgrpc.NewServerExact(w, opts...)`, `dstgrpc.NewClientExact(w, target, opts...)` | grpc's own calls in the real World (no default credentials); interceptor options as `dstgrpc.UnaryInterceptorOption` and kin, which dstmigrate writes; generated stubs are unchanged |

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
- **Finalizers and cleanups** (`runtime.SetFinalizer`, `runtime.AddCleanup`)
  run on the runtime's own goroutine, outside any World, when the
  collector decides: they can't be simulated. Keep them out of anything
  a run's outcome depends on. A finalizer that asserts an object was
  closed (and exits the process if not) misfires in simulation, where a
  crash abandons a machine's objects open: don't register it for objects
  made in a simulated World (`w.Simulated()` at construction), and check
  for leaks with the runner instead (goroutines left at the end of a
  run, or a `h.Final` that counts what is still open).
- **Pooled objects holding a World's channels or timers.** A `dst.Chan`,
  `Timer` or `Ticker` belongs to the World it was made in. An object
  kept in a pool (`dst.Pool`, a free list) that holds one can be taken
  by code in another World, another seed's or the real one, and its
  channel would then wait outside that World. Keep the World in the
  object, and remake the channel when the object is taken by another:

  ```go
  t := pool.Get().(*task)
  if t.w != w { // made, or last used, in another World
  	t.w, t.done = w, dst.MakeChan[error](w, 1)
  }
  ```

  dstlint reports a pool whose New makes a value holding one.
- **Package-level primitives** (a global `dst.Mutex`, `Once` or `Cond`)
  are shared by every simulation running at once, a sweep's parallel
  seeds: one simulation can't wake another's goroutines, and the seeds
  would depend on each other. The World fails such a run as misuse,
  naming the primitive; dstlint reports them. Make them per instance.

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

The other flags:

- `-tests` rewrites the packages' `_test.go` files too, in the same
  pass. Tests usually run on the real World: `-test-world` gives their
  World expression (`'realworld.New(realworld.Options{})'`, or a
  helper of yours named by import path,
  `'example.com/app/internal/dstworld.Real()'`, which is then imported
  in each test file that uses it).
- `-real-fs 'peersjson.go,*_test.go'` leaves the os file I/O of the
  files it names (base-name patterns) as it is: tests' temporary
  directories, operator tools.
- `-world-param 'randomTimeout,Store.snapshotName'` gives the functions
  it names (`Func` or `Type.Method`) the World as a new first parameter,
  `w`, and passes the World at hand at every call: the caller's, its
  receiver's (`-world`), or the tests' (`-test-world`). Use it for
  helpers with no owner to hold a World.
- `-world-field 'Config.World'` threads a library's options: each
  function given a `*Config` (or `Config`) that needs a World starts
  with

  ```go
  w := conf.World
  if w == nil {
  	w = realworld.Default()
  }
  ```

  and uses `w`. The field is added to the type if it hasn't one.
- With `-world 'T=$.w'`, a literal of `T` that leaves `w` unset gets
  the World at hand (`&T{w: w, ...}`), through one embedded struct
  too; a type with a method taking a World (an `init(w)`) sets its own,
  and is left alone.
- `-gosched 'shard.evict,drain'` names the functions (`Type.Method` or
  `Func`) whose `runtime.Gosched` is a turn for others in a loop that
  makes its own progress: they keep `w.Gosched()`. Every other Gosched
  becomes `w.Yield()`, the safe choice for a spin that waits for another
  goroutine (see the table above). A name that matches no function is
  reported.
- `-n` prints the rewritten files instead of writing them.

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
