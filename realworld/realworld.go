// Package realworld provides the production World: real goroutines, the
// real clock, real randomness, the operating system's files and network.
// Every operation is the plain Go one, so code written against a
// *dst.World runs in production with negligible overhead.
//
// Verified:
//   - [real-is-plain-go] Every World operation behaves as the plain Go
//     operation it replaces (the contract suite passes).
//
// Depends on: dst
package realworld

import (
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/worldhook"
)

// Options configure the real World.
type Options struct {
	// Dir is the root of the World's filesystem: w.FS() paths are relative
	// to it. Empty means the current directory.
	Dir string
}

// New returns the real World.
func New(opts Options) *dst.World {
	return worldhook.NewWorld(worldhook.Spec{FS: newFS(opts.Dir), Net: osNet{}}).(*dst.World)
}
