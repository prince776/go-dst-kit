package sched

import (
	"bytes"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// The registry maps goroutine IDs to tasks, so zero-value primitives
// (dst.Mutex, dst.WaitGroup) can find their scheduler without being handed
// a World. Lookups cost about a microsecond, so callers use them only on
// slow paths (contention, waiting).
var (
	registry sync.Map // goid -> *Task
	active   atomic.Int64
)

func register(t *Task) {
	active.Add(1)
	registry.Store(t.goid, t)
}

func unregister(t *Task) {
	registry.Delete(t.goid)
	active.Add(-1)
}

// CurrentTask returns the task running on this goroutine, or nil if the
// goroutine isn't a simulated one.
func CurrentTask() *Task {
	if active.Load() == 0 {
		return nil
	}
	v, ok := registry.Load(goid())
	if !ok {
		return nil
	}
	return v.(*Task)
}

func goid() int64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseInt(string(b), 10, 64)
	return id
}

// userStack is the stack of a panicking task, starting at the frame that
// panicked, with the scheduler's and the World's own frames removed.
func userStack() string {
	lines := strings.Split(strings.TrimSpace(string(debug.Stack())), "\n")
	// Skip to just after the "panic(" frame.
	start := 1
	for i := 1; i+1 < len(lines); i += 2 {
		if strings.HasPrefix(lines[i], "panic(") {
			start = i + 2
			break
		}
	}
	var out []string
	for i := start; i+1 < len(lines); i += 2 {
		fn := lines[i]
		if len(out) == 0 && isLibraryFrame(fn) {
			continue // library frames above the user's code
		}
		if strings.HasPrefix(fn, "github.com/prince776/go-dst-kit/internal/sched.(*Task).main") {
			break
		}
		out = append(out, fn, lines[i+1])
	}
	return strings.Join(out, "\n")
}

// libraryPackages are the packages whose frames are hidden above the
// user's code in a panic report.
var libraryPackages = []string{
	"github.com/prince776/go-dst-kit/internal/sched.",
	"github.com/prince776/go-dst-kit/dst.",
	"github.com/prince776/go-dst-kit/internal/simworld.",
}

func isLibraryFrame(fn string) bool {
	for _, p := range libraryPackages {
		if strings.HasPrefix(fn, p) {
			return true
		}
	}
	return false
}

// callerStack is the current goroutine's stack from the first frame
// outside this library (for misuse reports).
func callerStack() string {
	lines := strings.Split(strings.TrimSpace(string(debug.Stack())), "\n")
	var out []string
	for i := 1; i+1 < len(lines); i += 2 {
		fn := lines[i]
		if len(out) == 0 && (isLibraryFrame(fn) || strings.HasPrefix(fn, "runtime/debug.") || strings.HasPrefix(fn, "runtime.")) {
			continue
		}
		out = append(out, fn, lines[i+1])
	}
	return strings.Join(out, "\n")
}

// Active reports whether any simulated goroutine exists in this process:
// zero-value primitives check it before paying for a goroutine lookup.
func Active() bool { return active.Load() > 0 }
