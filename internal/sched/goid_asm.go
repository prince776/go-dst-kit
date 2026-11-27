//go:build amd64 || arm64

package sched

// getg returns the address of the calling goroutine's g (goid_*.s).
func getg() uint64

// goidCheap: goid is a few nanoseconds and takes no lock, so the identity
// check can run on every scheduling call.
const goidCheap = true
