//go:build !amd64 && !arm64

package sched

func getg() uint64 { return goidSlow() }

// goidCheap: goid is runtime.Stack, about 2µs under a process-wide lock,
// so the identity check samples (every 256th scheduling call).
const goidCheap = false
