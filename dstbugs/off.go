//go:build !dstbugs

package dstbugs

// Tagged reports whether this build has the dstbugs tag, without which
// no switch can turn on.
const Tagged = false

// Bug reports whether the planted bug name is in: never, in this build.
func Bug(name string) bool { return false }

// Fix reports whether the candidate fix name is on: never, in this
// build.
func Fix(name string) bool { return false }

// Hits reports how often code has reached the switch name: in this
// build switches aren't counted, so 0.
func Hits(name string) int64 { return 0 }
