package sched

// goid identifies the calling goroutine among the live ones, for the
// scheduler's identity checks and the registry. It is not the goroutine's
// ID: on amd64 and arm64 it is the address of the runtime's g for the
// goroutine (a few nanoseconds, no lock); elsewhere the ID from
// runtime.Stack. A g is a heap object the collector never moves, and it
// is reused only after its goroutine exits, so two live goroutines never
// share a value and one goroutine's never changes.
//
// Verified:
//   - [goid-identifies] goid is the same for every call on one goroutine
//     and differs between goroutines alive at the same time.
func goid() uint64 { return getg() }
