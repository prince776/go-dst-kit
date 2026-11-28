// Package realworld provides the production World: real goroutines, the
// real clock, real randomness, the operating system's files and network.
// Every operation is the plain Go one, so code written against a
// *dst.World runs in production with negligible overhead.
//
// Verified:
//   - [real-is-plain-go] Every World operation behaves as the plain Go
//     operation it replaces (the contract suite passes).
//   - [create-like-os] Concurrent creates of one file or directory
//     (OpenFile with O_CREATE, LockFile, MkdirAll) succeed as the os
//     package's do. Two workarounds make it so, each removable once its
//     cause is gone: openInRoot works around macOS's kernel (openat(2),
//     which os.Root uses, fails concurrent creates spuriously with
//     ENOENT; seen on macOS 26.3, still with Go 1.26.11), and MkdirAll's
//     retry works around Go before 1.25.7 (os.Root.MkdirAll failed on a
//     directory created meanwhile; fixed upstream by 1.25.7).
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
