//go:build amd64 || arm64

package sched

// getg returns the address of the calling goroutine's g (goid_*.s).
func getg() uint64
