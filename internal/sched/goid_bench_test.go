package sched

import "testing"

func BenchmarkGoid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		goid()
	}
}
