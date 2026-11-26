# go-dst-kit

Deterministic simulation testing for ordinary Go: code takes a
`*dst.World` (real in production, simulated in tests) for everything that
waits or is nondeterministic. The design and its approved decisions:
`PROPOSAL.md`. Status, decisions made while building, and findings:
`PLAN.md`. User docs: `README.md`, `docs/`.

## Map

| Path | What |
|---|---|
| `dst/` | the World API: goroutines, Chan, Select, time, sync, context, rand, FS, Net |
| `realworld/` | the production World |
| `internal/sched/` | the baton scheduler: one goroutine at a time, seeded choices, virtual time, machines |
| `internal/simfs/`, `internal/simnet/` | crash-prone disk; network with TCP-like streams and UDP |
| `internal/simworld/` | wires them into simulated Worlds and machines |
| `internal/worldtest/` | the contract suite run against both Worlds |
| `dsttest/` | the runner: seeds, swarm faults, phases, invariants, replay |
| `check/` | linearizability checker, KV model, World-timed History |
| `dsthttp/` | net/http over the World |
| `dstgrpc/` | gRPC over the World (own module: grpc v1.84.0, the last for Go 1.25) |
| `dstlint/` | analyzer for `//dst:simulated` packages (own module: x/tools) |
| `examples/` | own module: wal, dedupe, lease, heartbeat, gcounter, twophase, workerpool |

The library module (`go.mod`, Go 1.25) has **no dependencies**; keep it
that way. Public APIs must not expose `internal/` types.

## Rules

- **Simulated code** (the examples, and anything marked `//dst:simulated`)
  uses the World for goroutines, channels, select, time, locks that wait,
  contexts with timers, randomness, files and the network, and never lets
  map iteration order reach anything observable (sort keys). `dstlint`
  checks it.
- **Library internals** that the simulation drives (`internal/sched`,
  `simfs`, `simnet`) must be deterministic given the seed: no wall clock,
  no global rand, sorted iteration.
- **Laws (PAP).** Contracts live in doc comments. Each `Verified:` law has
  a test marked `// Verifies: [id]` with a `// Fails if:` line, and the
  test must be able to fail: a negative case in the same test, or an
  independent model checked in both directions. No mutants file. When
  in doubt, break the code by hand once and watch the test fail. `Laws:`
  and `Decisions:` are Prince's; never add, reword or move them without
  his OK.
- **Before committing:**
  ```sh
  GOTOOLCHAIN=go1.25.0 go vet ./... && go test ./...      # library
  (cd dstgrpc && go test ./...) ; (cd dstlint && go test ./...)
  (cd examples && go test ./...)
  go test -race ./...                                      # for scheduler or adapter changes
  ```
- **Examples** pair a correct system with `Bugs` switches for classic
  mistakes; the test asserts the correct one passes and each bug is caught
  for the right reason.
- **The System view** (`.sys/view.json`) is refreshed incrementally after
  substantial work. Never `--fresh`; `--plan` first and report the cost.
  Don't hand-edit it. sys is not part of CI.
- **Propose before building**: a new milestone's architecture and
  verification goals are agreed with Prince first.
- **Commits** are local; nothing is pushed until Prince asks.
