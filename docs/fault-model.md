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
- Half the seeds choose uniformly among runnable goroutines; the other
  half use PCT (probabilistic concurrency testing): random priorities,
  the highest runnable goroutine runs, and at 0 to 2 random steps the
  running one drops to the bottom. PCT reaches orderings where one
  goroutine is starved for a long stretch, which uniform choice almost
  never does. A goroutine picked 100 times in a row while others wait is
  demoted, so a polling loop can't starve the one it waits for.
- Channels, `select`, locks, wait groups and contexts behave as Go
  specifies. The channel and select model is checked against a
  brute-force enumeration of Go's semantics, in both directions: every
  outcome the simulator produces is legal, and every legal outcome is
  produced by some seed.
- Time is virtual. One true timeline drives the simulation and advances
  only when no goroutine can run, straight to the next timer. Each machine
  reads its own clock: an offset from true time plus a drift in rate
  (`Options.Clock`, default up to 10ms and 100ppm, a quarter of the draws
  at the bounds), kept across restarts. Its `Now`, sleeps, timers,
  context deadlines and connection deadlines all follow that clock;
  network latency and disk speed are in true time. Invariants, and the
  test machine, use true time. Wall-clock jumps (NTP steps) aren't
  modelled: Go's monotonic clock hides them from durations anyway.
- Reported as failures: every goroutine blocked (deadlock); workload
  goroutines, or goroutines they started, never finishing (deadlock, leak,
  or lost liveness); a panic in any goroutine (with your stack); a
  goroutine blocked outside the World for 10s of wall time (watchdog); a
  run exceeding its step or time limit; a World operation from a goroutine
  the World didn't start, or from an `Always` invariant (checked on every
  World operation on amd64 and arm64, where reading the goroutine's
  identity costs a few nanoseconds; on every 256th elsewhere).

## Machines (`internal/simworld`)

- A machine is a process: its goroutines, timers, disk and network host.
- **Crash** stops every goroutine on the machine at once, wherever it is.
  Deferred functions don't run (as with `kill -9` or power loss). Its
  timers and tickers never fire again, and its open connections go
  silent.
- **Kill** (`Faults.Kill`, `Machine.Kill`) is a crash of the process
  alone (kill -9, a panic, the OOM killer): its goroutines stop the same
  way, but the host stays up, so the disk keeps everything written,
  synced or not, and a later power loss may still lose what wasn't
  synced. With `Faults.Crash` too, each crash is a kill or a power loss,
  by a share drawn per seed (a quarter of seeds all kills, a quarter
  none); turning kills on doesn't move any crash. On the network the host
  stays up and its kernel closes the process's sockets: peers read what
  it had sent, then EOF, at once; their next write is reset; dials to its
  ports are refused until it listens again. (A power loss stays silent.)
- **Restart** runs the machine's start function again on what its disk
  kept. It's a new incarnation: nothing in memory survives.
- A machine can also lose power inside an I/O call (below).
- **Pause** stops a machine without crashing it, like SIGSTOP, a long GC
  pause or a VM stall: its goroutines don't run, so its sleeps and timers
  return late, while its clock keeps going and data sent to it waits in
  its socket buffers. On resume it carries on as if nothing happened,
  except that time has jumped. Pause lengths are drawn per seed from 10ms
  to 2s.

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
- After a kill, nothing is lost: the page cache survives, so every
  completed write and directory change is still visible, and still
  unsynced.
- Advisory locks (`dst.LockFile`) are exclusive, even within one process,
  and a crash or a kill releases them, as the death of the process
  holding a `flock(2)` does.
- `ReadDir` on an open directory returns entries in a seeded order, like
  real filesystems; `FS.ReadDir` sorts, like `os.ReadDir`.

Disk errors (`Faults.DiskErrors`), as Linux reports them:

- **A full disk stays full.** At a time drawn per seed a machine's disk
  fills: from then until the faults stop it holds no more bytes than it
  did. A write that doesn't fit writes what does and fails with ENOSPC
  (as `os.File` reports a short write); creating a file or directory
  fails with ENOSPC while no byte is free; removing or truncating files
  frees space under the cap, so compaction can make room. Nothing already
  written is lost. When the faults stop the cap lifts (an operator added
  space), so a correct system can recover. Space is freed at `Remove`,
  even while the file is open.
- **A failed fsync loses the data for good.** A `Sync` of a file with
  unsynced writes may fail with EIO; those writes then never become
  durable (they stay readable until a crash, which loses them), and the
  next `Sync` succeeds: Linux marks the pages clean after a writeback
  error and reports it once (PostgreSQL's "fsyncgate", 2018). Retrying the
  fsync is the bug this catches; the safe responses are to rewrite the
  data or to crash. Directory syncs don't fail.
- **Writes and reads may fail with EIO**, doing nothing (no bytes, the
  offset unmoved).
- Rates are drawn per seed (none in a third of seeds per kind): a disk
  fills every 1 to 6s on average, fsyncs with data to write fail up to 5%
  of the time, writes and reads up to 0.5%. Machines marked `NeverCrash`
  are spared. Errors are `*fs.PathError`s wrapping `syscall.ENOSPC` or
  `syscall.EIO`, as `os` returns them.

What a correct system owes: an error may fail a request or stop a
process, but acknowledged data must stay durable and nothing
unacknowledged may appear half-written. Startup steps that hit the disk
should retry: the errors pass.

Not modelled yet: a failed fsync's pages being evicted (reads returning
the old data), bit rot, misdirected writes.

## Network (`internal/simnet`)

Connections are TCP-like streams (`net.Conn`), between hosts named after
their machines. UDP is below.

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

**UDP** (`w.Net().ListenPacket("udp", ...)`, or `Dial("udp", ...)` for a
connected socket that sends to and receives from one peer):

- Each datagram arrives whole or not at all, after its own latency, so
  datagrams reorder. With network faults on, a seed loses up to 20% of
  them and duplicates up to 5%.
- With gray failure on (`Faults.Gray`), datagrams take their link's
  behaviour: a slow link delays them, a lossy link loses them (UDP has no
  retransmission, so a loss is a loss), and in a quarter of the seeds up
  to 30% of links **block UDP** while TCP on them still flows (a firewall
  or security group). Losses and blocks stop in the recovery phase; slow
  links stay slow.
- About a third of the Gray seeds are **mild** (`Swarm.MildGray`): of the
  gray kinds that would be on (slow links, lossy links, resets at heal,
  slow machines, slow disks, UDP blocks), only one or two stay on. That
  makes seeds with survivable gray failure common, for checks that hold
  only inside an envelope of it. The choice comes from a stream of its
  own, so every other field of every seed is what it was before.
- A partition, a crashed host, or a port nobody listens on **drops**
  datagrams: UDP never holds them. A paused host's socket keeps up to 256
  for when it resumes, and drops the rest.
- A crash closes the machine's sockets; the restarted machine can bind
  the same ports.
- Differences from a real kernel: a datagram to an unbound port draws no
  ICMP error (a connected socket's next read doesn't fail with
  ECONNREFUSED), and addresses are `host:port` names, not `*net.UDPAddr`:
  compare them with `String()`. A machine named by an IP literal
  (`h.Machine("10.0.0.1", ...)`) has `*net.TCPAddr` and `*net.UDPAddr`
  addresses instead, for code that stores IPs or type-asserts addresses.

Not modelled yet: bandwidth limits.

## Fault points (`dst.Inject`, `dst.Call`, `w.Buggify`, `internal/points`)

A fault point stands for one call to a service the simulation doesn't run
(cloud storage, a managed database, a third-party API), placed in its fake
or in a wrapper around the fake; see [writing a
test](writing-a-test.md#faults-in-your-own-fakes). Each call gets one
decision, after an injected latency (often zero) during which other
goroutines run:

| Decision | The operation | The caller | Models |
|---|---|---|---|
| None | happens | gets the real result | normal |
| Fail | does not happen | gets an error (`Errors.Before`) | refused (503, throttled), connection refused, request lost on the way |
| FailAfter | happens; its effects stay | gets an error instead of the result (`Errors.After`) | timeout or reset after the service acted, lost reply |

With `dst.Call`, injected latency ends early when the caller's context
does: the caller gets the context's error, and the operation still
happens unless the decision was Fail (a slow request the service received
takes effect, though the caller gave up). A context already done when the
call starts means the request is never sent.

- Decisions are a pure function of the seed. Each point's rates are drawn
  per seed from a stream named after it: quiet in a third of seeds, else
  failing 0.1% to 20% of calls (a random share of them after acting), and
  in some seeds slowed by 1ms to 1s. Adding a point leaves the others'
  decisions as they were; a quiet point changes nothing in the schedule.
- A **service outage** fails every point of a service (`svc` in
  `svc.op`) for a window, lengths log-uniform from 10ms to 3s.
- Nothing fires outside the fault phase, or in the real World.
- `w.Buggify(name)` is a point in your own code: true now and then (about
  a quarter of seeds, then a quarter of calls), so the code takes a legal
  but rare path.

## The runner (`dsttest`)

- Fault rates are drawn per seed (swarm testing): some seeds turn whole
  fault classes off, so bugs that need a quiet system, or one fault class
  alone, are reached too.
- During the recovery phase every machine is up, the network is whole,
  and no faults happen. A system that can't recover then fails the seed.
- A run is decided by its seed alone. The runner checks this by running
  seeds twice and comparing a fingerprint of every scheduling decision.
