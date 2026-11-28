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
//     package's do: the FS is the os package's (os.Root's openat(2)
//     failed them on macOS, and its MkdirAll before Go 1.25.7).
//   - [fs-is-os] With no Dir, every name reaches the os package exactly as
//     the program gave it: absolute, relative to the working directory,
//     or climbing out of it with "..", with os's results and errors and
//     the *os.File itself. With a Dir, names are relative to it and
//     checked as a simulated disk checks them, so both reject the same
//     names.
//
// Depends on: dst
package realworld

import (
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/worldhook"
)

// Options configure the real World.
type Options struct {
	// Dir, if set, roots the World's filesystem there: w.FS() names are
	// relative to it, and absolute names and names that climb out of it
	// ("..") are rejected, as a simulated disk rejects them (tests and
	// examples use this). The check is lexical: a symbolic link inside
	// Dir can still lead outside it.
	//
	// Empty, the production default, means no root at all: w.FS() is the
	// os package, and every name reaches it as the program gave it.
	Dir string
}

// New returns the real World.
func New(opts Options) *dst.World {
	return worldhook.NewWorld(worldhook.Spec{FS: newFS(opts.Dir), Net: osNet{}}).(*dst.World)
}
