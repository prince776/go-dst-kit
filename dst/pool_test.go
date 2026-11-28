package dst_test

import (
	"testing"

	"github.com/prince776/go-dst-kit/dst"
)

// Verifies: [pools-dont-cross-seeds] (the real World's half)
// Fails if: in the real World a Pool never returns what was Put (it
// would not be a pool), or Get ignores New.
func TestPoolRealWorldPools(t *testing.T) {
	type obj struct{ n int }
	p := dst.Pool{New: func() any { return &obj{n: -1} }}
	if got := p.Get().(*obj); got.n != -1 {
		t.Fatalf("an empty pool's Get = %v, want New's value", got)
	}
	reused := false
	for i := 0; i < 100 && !reused; i++ {
		x := &obj{n: i}
		p.Put(x)
		reused = p.Get().(*obj) == x // sync.Pool may drop a Put; it keeps most
	}
	if !reused {
		t.Fatal("in the real World, Get never returned the object just Put")
	}
	var empty dst.Pool
	if x := empty.Get(); x != nil {
		t.Fatalf("a Pool without New: Get = %v, want nil", x)
	}
}
