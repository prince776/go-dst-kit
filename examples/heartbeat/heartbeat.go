// Package heartbeat is a failure detector over UDP: workers send
// heartbeats to a monitor, which suspects a worker it hasn't heard from
// for Timeout. A worker can also leave for good, and says so in its
// heartbeats.
//
// UDP loses, duplicates and reorders datagrams, so each heartbeat carries
// a sequence number, and the monitor ignores any not newer than the last
// it accepted. The sequence number starts with the worker's incarnation,
// kept on disk and bumped at every boot, so a restarted worker's
// heartbeats are newer than its old ones. The monitor knows the members
// from its configuration, not from who it has heard: after it restarts,
// a worker that is down is still suspected.
//
// Verified:
//   - [no-zombies] Once the monitor has seen a worker leave, it never sees
//     it active again, under loss, duplication, reordering, crashes,
//     pauses and partitions.
//   - [complete] A worker that has been down for Timeout plus the network's
//     worst delay is suspected by a monitor that has been up that long.
//   - [eventually-accurate] Once faults stop, every live worker is
//     trusted, with its latest status.
//
// Depends on: dst
//
//dst:simulated
package heartbeat

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

const (
	// Interval is how often a worker sends a heartbeat.
	Interval = 50 * time.Millisecond
	// Timeout is how long the monitor waits before suspecting a worker.
	Timeout = 300 * time.Millisecond
)

// Bugs are classic mistakes the tests must catch.
type Bugs struct {
	// NoSeq makes the monitor apply every heartbeat, in whatever order
	// they arrive: a delayed one from before a worker left says it's
	// active again.
	NoSeq bool
	// NoIncarnation makes a worker's sequence numbers start over when it
	// restarts: the monitor ignores it until it passes its old count.
	NoIncarnation bool
	// LearnsMembers makes the monitor track only workers it has heard
	// from: after the monitor restarts, a worker that is down is never
	// suspected.
	LearnsMembers bool
}

// Status is what a worker says about itself.
type Status string

const (
	Active  Status = "active"
	Leaving Status = "leaving"
)

// ---- worker ----

// Worker sends heartbeats to the monitor at Monitor.
type Worker struct {
	w       *dst.World
	name    string
	monitor string
	bugs    Bugs
	inc     uint64
	n       uint64
	status  Status
	// OnSend, if set, is called with each heartbeat's sequence number and
	// status before it is sent (for tests).
	OnSend func(seq uint64, st Status)
}

// state is what a worker keeps on disk: its incarnation and whether it
// has left.
func loadState(fsys dst.FS) (inc uint64, left bool) {
	b, err := dst.ReadFile(fsys, "state")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		panic(err)
	}
	f := strings.Fields(string(b))
	if len(f) == 2 {
		inc, _ = strconv.ParseUint(f[0], 10, 64)
		left = f[1] == "left"
	}
	return inc, left
}

// saveState replaces the state file atomically and durably.
func saveState(fsys dst.FS, inc uint64, left bool) {
	f, err := fsys.OpenFile("state.tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	word := "member"
	if left {
		word = "left"
	}
	fmt.Fprintf(f, "%d %s\n", inc, word)
	if err := f.Sync(); err != nil {
		panic(err)
	}
	f.Close()
	if err := fsys.Rename("state.tmp", "state"); err != nil {
		panic(err)
	}
	if err := dst.SyncDir(fsys, "."); err != nil {
		panic(err)
	}
}

// NewWorker boots a worker: it bumps its incarnation on disk before
// sending anything.
func NewWorker(w *dst.World, name, monitor string, bugs Bugs) *Worker {
	inc, left := loadState(w.FS())
	inc++
	saveState(w.FS(), inc, left)
	wk := &Worker{w: w, name: name, monitor: monitor, bugs: bugs, inc: inc, status: Active}
	if left {
		wk.status = Leaving
	}
	return wk
}

// Leave makes the worker leave for good: it records that on disk, then
// announces it at once.
func (wk *Worker) Leave(pc net.PacketConn, to net.Addr) {
	if wk.status == Leaving {
		return
	}
	saveState(wk.w.FS(), wk.inc, true)
	wk.status = Leaving
	wk.send(pc, to)
}

func (wk *Worker) send(pc net.PacketConn, to net.Addr) {
	wk.n++
	seq := wk.inc<<32 | wk.n
	if wk.bugs.NoIncarnation {
		seq = wk.n
	}
	if wk.OnSend != nil {
		wk.OnSend(seq, wk.status)
	}
	pc.WriteTo(fmt.Appendf(nil, "hb %s %d %s", wk.name, seq, wk.status), to)
}

// Run sends a heartbeat every Interval; leaveAt, if positive, is how long
// after boot the worker leaves.
func (wk *Worker) Run(leaveAt time.Duration) error {
	pc, err := wk.w.Net().ListenPacket("udp", ":0")
	if err != nil {
		return err
	}
	defer pc.Close()
	to := addr(wk.monitor)
	start := wk.w.Now()
	for {
		if leaveAt > 0 && wk.w.Since(start) >= leaveAt {
			wk.Leave(pc, to)
		}
		wk.send(pc, to)
		wk.w.Sleep(Interval)
	}
}

// addr is a UDP address by name.
type addr string

func (a addr) Network() string { return "udp" }
func (a addr) String() string  { return string(a) }

// ---- monitor ----

type member struct {
	seq       uint64
	status    Status
	lastHeard time.Time // zero: never heard
}

// Monitor tracks workers' heartbeats.
type Monitor struct {
	w       *dst.World
	bugs    Bugs
	mu      dst.Mutex
	booted  time.Time
	members map[string]*member
	// OnStatus, if set, is called when the monitor's view of a worker's
	// status changes (for tests).
	OnStatus func(name string, from, to Status)
}

// NewMonitor returns a monitor for the given workers, just booted.
func NewMonitor(w *dst.World, workers []string, bugs Bugs) *Monitor {
	m := &Monitor{w: w, bugs: bugs, booted: w.Now(), members: map[string]*member{}}
	if !bugs.LearnsMembers {
		for _, name := range workers {
			m.members[name] = &member{}
		}
	}
	return m
}

func (m *Monitor) handle(msg string) {
	f := strings.Fields(msg)
	if len(f) != 4 || f[0] != "hb" {
		return
	}
	name, st := f[1], Status(f[3])
	seq, err := strconv.ParseUint(f[2], 10, 64)
	if err != nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	mb := m.members[name]
	if mb == nil {
		if !m.bugs.LearnsMembers {
			return // not a member
		}
		mb = &member{}
		m.members[name] = mb
	}
	if !m.bugs.NoSeq && seq <= mb.seq {
		return // a duplicate, or older than what we have
	}
	if mb.status != st && m.OnStatus != nil {
		m.OnStatus(name, mb.status, st)
	}
	mb.seq, mb.status, mb.lastHeard = seq, st, m.w.Now()
}

// Serve reads heartbeats from pc.
func (m *Monitor) Serve(pc net.PacketConn) error {
	buf := make([]byte, 512)
	for {
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			return err
		}
		m.handle(string(buf[:n]))
	}
}

// Suspected reports whether the monitor suspects the worker: it hasn't
// heard from it for Timeout (counting from its own boot if never).
// A worker the monitor doesn't know of isn't suspected.
func (m *Monitor) Suspected(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	mb := m.members[name]
	if mb == nil {
		return false
	}
	last := mb.lastHeard
	if last.IsZero() {
		last = m.booted
	}
	return m.w.Since(last) > Timeout
}

// Status is the monitor's view of a worker's status ("" if unknown).
func (m *Monitor) Status(name string) Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mb := m.members[name]; mb != nil {
		return mb.status
	}
	return ""
}

// Status is the worker's own status.
func (wk *Worker) Status() Status { return wk.status }
