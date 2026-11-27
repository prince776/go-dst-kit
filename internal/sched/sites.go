package sched

import (
	"hash/fnv"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// Code sites for interleaving pairs (Options.Pairs).

const (
	// siteFinished is where a task that returned handed off.
	siteFinished = 1
	// siteLibrary is a scheduling point with no frame outside go-dst-kit.
	siteLibrary = 2
)

// noteSite records where t hands off (Park, Yield).
func (s *Scheduler) noteSite(t *Task) {
	if !s.opts.Pairs {
		return
	}
	t.site = callerSite()
	s.lastSite = t.site
}

// siteCache maps a program counter to its site (0: only library frames
// there). PCs are fixed for the life of a process, and the site is a hash
// of function and line, so the cache is shared by every run.
var siteCache sync.Map

// libraryPrefixes are go-dst-kit's packages whose frames aren't sites: the
// World and what implements it. Its test packages (dsttest_test, ...) and
// the examples are programs under test, so their frames are.
var libraryPrefixes = []string{
	"runtime.",
	"github.com/prince776/go-dst-kit/dst.",
	"github.com/prince776/go-dst-kit/internal/",
	"github.com/prince776/go-dst-kit/realworld.",
	"github.com/prince776/go-dst-kit/check.",
	"github.com/prince776/go-dst-kit/dsthttp.",
	"github.com/prince776/go-dst-kit/dstgrpc.",
}

func isLibrary(fn string) bool {
	for _, p := range libraryPrefixes {
		if strings.HasPrefix(fn, p) {
			return true
		}
	}
	return false
}

// callerSite is the first frame of the current goroutine's stack outside
// go-dst-kit.
func callerSite() uint64 {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	for _, pc := range pcs[:n] {
		if v, ok := siteCache.Load(pc); ok {
			if site := v.(uint64); site != 0 {
				return site
			}
			continue
		}
		site := siteAt(pc)
		siteCache.Store(pc, site)
		if site != 0 {
			return site
		}
	}
	return siteLibrary
}

// siteAt is the site of pc: its first frame outside go-dst-kit, inlined
// frames included, or 0.
func siteAt(pc uintptr) uint64 {
	frames := runtime.CallersFrames([]uintptr{pc})
	for {
		f, more := frames.Next()
		if f.Function != "" && !isLibrary(f.Function) {
			return hashSite(f.Function + ":" + strconv.Itoa(f.Line))
		}
		if !more {
			return 0
		}
	}
}

// startSite is the site of a task that hasn't handed off yet: its start
// function, named as Spawn was told (machine prefix and instance number
// dropped, so every node's copy of a goroutine is one site).
func startSite(name string) uint64 {
	if i := strings.LastIndexByte(name, '#'); i >= 0 {
		name = name[:i]
	}
	if i := strings.IndexByte(name, '/'); i >= 0 && !strings.Contains(name[:i], ".") {
		name = name[i+1:]
	}
	return hashSite("go " + name)
}

func hashSite(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64() | 4 // never 0, 1 or 2
}

// pairKey combines two sites, in order.
func pairKey(a, b uint64) uint64 {
	k := a*0x9e3779b97f4a7c15 ^ (b<<31 | b>>33)
	if k == 0 {
		k = 1
	}
	return k
}
