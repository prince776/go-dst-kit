//go:build !amd64 && !arm64

package sched

func getg() uint64 { return goidSlow() }
