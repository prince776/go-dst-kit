//go:build dstbugs

package dstbugs

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

// Tagged reports whether this build has the dstbugs tag, without which
// no switch can turn on.
const Tagged = true

// Bug reports whether the planted bug name is in: listed in $DSTBUG.
func Bug(name string) bool { return on("DSTBUG", name) }

// Fix reports whether the candidate fix name is on: listed in $DSTFIX.
func Fix(name string) bool { return on("DSTFIX", name) }

// Hits reports how often code has reached the switch name (Bug or Fix,
// on or off) in this process so far: a switch a workload never reaches
// can't matter to it.
func Hits(name string) int64 {
	if c, ok := hits.Load(name); ok {
		return c.(*atomic.Int64).Load()
	}
	return 0
}

// hits counts each switch's reaches, process-wide.
var hits sync.Map // name -> *atomic.Int64

func on(env, name string) bool {
	c, ok := hits.Load(name)
	if !ok {
		c, _ = hits.LoadOrStore(name, new(atomic.Int64))
	}
	c.(*atomic.Int64).Add(1)
	if name == "" {
		return false
	}
	for n := range strings.SplitSeq(os.Getenv(env), ",") {
		if strings.TrimSpace(n) == name {
			return true
		}
	}
	return false
}
