package dst_test

import (
	"runtime"
	"testing"

	"github.com/prince776/go-dst-kit/realworld"
)

// Verifies: [procs-are-the-runtimes]
// Fails if: the real World's GOMAXPROCS or NumCPU isn't the runtime's,
// or doesn't follow a change to GOMAXPROCS.
func TestRealProcsAreTheRuntimes(t *testing.T) {
	w := realworld.Default()
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)
	for _, n := range []int{prev, 3, 1} {
		runtime.GOMAXPROCS(n)
		if got := w.GOMAXPROCS(); got != n {
			t.Errorf("after GOMAXPROCS(%d): w.GOMAXPROCS() = %d", n, got)
		}
	}
	if got := w.NumCPU(); got != runtime.NumCPU() {
		t.Errorf("w.NumCPU() = %d, runtime.NumCPU() = %d", got, runtime.NumCPU())
	}
}
