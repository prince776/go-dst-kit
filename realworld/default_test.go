package realworld_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/realworld"
)

// Verifies: [default-is-one]
// Fails if: Default returns different Worlds (across calls or
// goroutines), a simulated one, or one whose filesystem is rooted;
// if dst.Current's real World is another World, or one without the os
// filesystem and network; or if Default allocates.
func TestDefaultIsOne(t *testing.T) {
	w := realworld.Default()
	if w == nil || w.Simulated() {
		t.Fatalf("Default() = %v, want a real World", w)
	}
	var wg sync.WaitGroup
	got := make([]*dst.World, 8)
	for i := range got {
		wg.Add(1)
		go func() { defer wg.Done(); got[i] = realworld.Default() }()
	}
	wg.Wait()
	for i, g := range got {
		if g != w {
			t.Errorf("goroutine %d's Default() is another World", i)
		}
	}
	// No root: an absolute name reaches os as given.
	name := filepath.Join(t.TempDir(), "f")
	if err := dst.WriteFile(w.FS(), name, []byte("x"), 0o644); err != nil {
		t.Fatalf("Default's FS refused an absolute name: %v", err)
	}
	if b, err := os.ReadFile(name); err != nil || string(b) != "x" {
		t.Fatalf("os sees %q, %v", b, err)
	}
	// dst.Current, from a goroutine no World started, is the same World,
	// filesystem and network included.
	var cur *dst.World
	done := make(chan struct{})
	go func() { defer close(done); cur = dst.Current() }()
	<-done
	if cur != w {
		t.Fatalf("dst.Current() in a plain goroutine is another World than Default()")
	}
	if _, err := cur.FS().Stat(name); err != nil {
		t.Fatalf("Current's FS: %v", err)
	}
	ln, err := cur.Net().Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Current's Net: %v", err)
	}
	ln.Close()
	if n := testing.AllocsPerRun(100, func() { _ = realworld.Default() }); n != 0 {
		t.Errorf("Default allocates %v times", n)
	}
}
