# The fault model

What the simulator can do to your code, and what it promises not to do.
A fault the model allows is one real systems can produce. A test that
fails under it is a real bug, not a simulator artifact. Each rule here is
a law in the package that implements it, with a test.

## Scheduling (`dst`, `internal/sched`)

- Every simulated goroutine is a real goroutine, but only one runs at a
  time. Each World operation (channel op, lock, sleep, I/O, `Select`) is a
  point where the scheduler may switch to another runnable goroutine,
  chosen by the seed. That includes `Lock` and `RLock`, so races between
  separate critical sections are explored. Plain memory and `sync/atomic`
  accesses between World operations are not interleaved (use `-race` for
  data races).
- Channels, `select`, locks, wait groups and contexts behave as Go
  specifies. The channel and select model is checked against a
  brute-force enumeration of Go's semantics, in both directions: every
  outcome the simulator produces is legal, and every legal outcome is
  produced by some seed.
- Time is virtual and shared by all machines. It advances only when no
  goroutine can run, straight to the next timer. Timers fire in deadline
  order.
- Reported as failures: every goroutine blocked (deadlock); workload
  goroutines, or goroutines they started, never finishing (deadlock, leak,
  or lost liveness); a panic in any goroutine (with your stack); a
  goroutine blocked outside the World for 10s of wall time (watchdog); a
  run exceeding its step or time limit; a World operation from a goroutine
  the World didn't start, or from an `Always` invariant (checked strictly
  on the determinism-checked seeds and replays, by sampling elsewhere, or
  always with `Options.StrictIdentity`).

## Machines (`internal/simworld`)

- A machine is a process: its goroutines, timers, disk and network host.
- **Crash** stops every goroutine on the machine at once, wherever it is.
  Deferred functions don't run (as with `kill -9` or power loss). Its
  timers and tickers never fire again, and its open connections go
  silent.
- **Restart** runs the machine's start function again on what its disk
  kept. It's a new incarnation: nothing in memory survives.
- A machine can also lose power inside an I/O call (below).

## Disk (`internal/simfs`)

Live operations behave exactly like `os` (the same contract suite runs
against both). What survives a crash is anything POSIX and real
journaling filesystems allow, chosen by the seed:

- File data is durable only after the file's `Sync`.
- Directory changes (create, mkdir, rename, remove) form one ordered
  journal: a crash keeps a prefix of the unsynced ones, and `Sync` on a
  directory (`dst.SyncDir`) makes it durable up to that directory's last
  change. A rename is atomic: the old file or the new, never neither.
- Per seed, one of two modes:
  - **harsh** (like ext4 `data=writeback`, XFS): each file's unsynced writes
    survive independently of other files', and a file's `Sync` covers only
    its own data;
  - **ordered** (like ext4's default `data=ordered`): writes reach the disk
    in issue order, and any `Sync` commits everything before it.
- The first lost write of a file may survive in part (torn).
- Power can fail inside a `Write`, `Sync` or directory operation.
- Files opened before a crash return `ESTALE`.
- `ReadDir` on an open directory returns entries in a seeded order, like
  real filesystems; `FS.ReadDir` sorts, like `os.ReadDir`.

Not modelled yet: a full disk, I/O errors, bit rot, misdirected writes.

## Network (`internal/simnet`)

Connections are TCP-like streams (`net.Conn`), between hosts named after
their machines.

- Bytes arrive in order, exactly once, after a latency of a floor plus an
  exponential tail (drawn per seed, occasionally spiking 20×).
- A **partition** (symmetric, or one way) stalls traffic: nothing is lost,
  and healing delivers what was held. Dials into a partition wait until
  healed or until their context expires.
- A crashed host is **silent**: dials and writes to it hang until their
  deadline. A restarted host answers old connections with a **reset**.
- A **reset** arrives in order and discards data the receiver hadn't read
  (like TCP RST). Closing a connection with unread input sends one, as
  Linux does. With network faults on, a seed may also reset connections at
  random.
- Deadlines (`SetDeadline`, `DialContext`) follow the virtual clock.

Not modelled yet: UDP, clock skew between machines, bandwidth limits.

## The runner (`dsttest`)

- Fault rates are drawn per seed (swarm testing): some seeds turn whole
  fault classes off, so bugs that need a quiet system, or one fault class
  alone, are reached too.
- During the recovery phase every machine is up, the network is whole,
  and no faults happen. A system that can't recover then fails the seed.
- A run is decided by its seed alone. The runner checks this by running
  seeds twice and comparing a fingerprint of every scheduling decision.
