package unsetworld

import "github.com/prince776/go-dst-kit/dst"

// Test files are skipped: a test may build a bare value on purpose.
func bareInTest(w *dst.World) *Store { return &Store{name: "t"} }
