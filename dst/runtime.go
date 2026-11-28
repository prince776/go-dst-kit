package dst

import "runtime"

// GOMAXPROCS returns runtime.GOMAXPROCS(0): how many goroutines may run
// at once. A simulated World runs one at a time whatever it reports, and
// reports its seed's draw (1, 2, 4 or 8, the same on every machine), so
// code that sizes itself by it (shard counts, worker pools) is explored
// at several sizes and replays at one. To set it, call
// runtime.GOMAXPROCS(n) as before: a simulation ignores it.
//
// Verified:
//   - [procs-are-the-runtimes] In the real World GOMAXPROCS and NumCPU
//     are runtime.GOMAXPROCS(0) and runtime.NumCPU(), following a change
//     to GOMAXPROCS.
//   - [procs-drawn-per-seed] In a simulated World they are the seed's
//     draw (dsttest.Swarm.GOMAXPROCS and NumCPU): GOMAXPROCS from 1, 2,
//     4 and 8, NumCPU at least that, the same on every machine and in
//     every replay, whatever the process's own counts are.
func (w *World) GOMAXPROCS() int {
	if w.s == nil {
		return runtime.GOMAXPROCS(0)
	}
	return w.procs
}

// NumCPU returns runtime.NumCPU(); a simulated World reports its seed's
// draw, at least GOMAXPROCS (see GOMAXPROCS).
func (w *World) NumCPU() int {
	if w.s == nil {
		return runtime.NumCPU()
	}
	return w.cpus
}
