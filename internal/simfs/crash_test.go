package simfs_test

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// ---- operation sequences over files a, b, d/c (d always exists) ----

type kind int

const (
	kCreate kind = iota
	kWrite
	kTrunc
	kSync
	kSyncDir
	kRename
	kRemove
)

type fop struct {
	k        kind
	name, to string
	data     string
	size     int
}

func (o fop) String() string {
	switch o.k {
	case kCreate:
		return "create " + o.name
	case kWrite:
		return fmt.Sprintf("write %s %q", o.name, o.data)
	case kTrunc:
		return fmt.Sprintf("truncate %s %d", o.name, o.size)
	case kSync:
		return "sync " + o.name
	case kSyncDir:
		return "syncdir " + o.name
	case kRename:
		return "rename " + o.name + " " + o.to
	default:
		return "remove " + o.name
	}
}

var files = []string{"a", "b", "d/c"}

func gen(r *rand.Rand, n int) []fop {
	live := map[string]bool{}
	var ops []fop
	for len(ops) < n {
		name := files[r.IntN(len(files))]
		var o fop
		switch k := r.IntN(8); {
		case !live[name] || k == 0:
			if live[name] {
				continue
			}
			o = fop{k: kCreate, name: name}
		case k <= 2:
			o = fop{k: kWrite, name: name, data: []string{"x", "yz", "pqr"}[r.IntN(3)]}
		case k == 3:
			o = fop{k: kTrunc, name: name, size: r.IntN(3)}
		case k == 4:
			o = fop{k: kSync, name: name}
		case k == 5:
			o = fop{k: kSyncDir, name: []string{".", "d"}[r.IntN(2)]}
		case k == 6:
			to := files[r.IntN(len(files))]
			if to == name {
				continue
			}
			o = fop{k: kRename, name: name, to: to}
			delete(live, name)
			live[to] = true
		default:
			o = fop{k: kRemove, name: name}
			delete(live, name)
		}
		if o.k == kCreate {
			live[name] = true
		}
		ops = append(ops, o)
	}
	return ops
}

// ---- the reference model, written from the rules in the package doc ----

type dop struct {
	write bool
	off   int
	data  string
	size  int
}

func (d dop) apply(s string) string {
	if !d.write {
		if d.size <= len(s) {
			return s[:d.size]
		}
		return s + strings.Repeat("\x00", d.size-len(s))
	}
	for len(s) < d.off+len(d.data) {
		s += "\x00"
	}
	return s[:d.off] + d.data + s[d.off+len(d.data):]
}

type obj struct {
	durable string
	pending []dop // harsh mode only
}

type mop struct {
	k        kind // kCreate, kRemove, kRename
	name, to string
	o        *obj
}

// entry is one unsynced operation in issue order (ordered mode).
type entry struct {
	data bool
	d    dop
	o    *obj
	m    mop
}

type model struct {
	ordered  bool
	live     map[string]*obj
	liveData map[*obj]string
	durNS    map[string]*obj
	meta     []mop   // harsh mode
	all      []entry // ordered mode
}

func newModel(ordered bool) *model {
	return &model{ordered: ordered, live: map[string]*obj{}, liveData: map[*obj]string{}, durNS: map[string]*obj{}}
}

func applyMeta(ns map[string]*obj, m mop) {
	switch m.k {
	case kCreate:
		ns[m.name] = m.o
	case kRemove:
		delete(ns, m.name)
	case kRename:
		if ns[m.name] == m.o {
			delete(ns, m.name)
		}
		ns[m.to] = m.o
	}
}

func (m *model) data(o *obj, d dop) {
	m.liveData[o] = d.apply(m.liveData[o])
	if m.ordered {
		m.all = append(m.all, entry{data: true, d: d, o: o})
	} else {
		o.pending = append(o.pending, d)
	}
}

func (m *model) metaOp(x mop) {
	if m.ordered {
		m.all = append(m.all, entry{m: x})
	} else {
		m.meta = append(m.meta, x)
	}
}

func (m *model) commitAll() {
	for _, e := range m.all {
		if e.data {
			e.o.durable = e.d.apply(e.o.durable)
		} else {
			applyMeta(m.durNS, e.m)
		}
	}
	m.all = nil
}

func dirOf(name string) string { return path.Dir(name) }

func (m *model) do(o fop) {
	switch o.k {
	case kCreate:
		x := &obj{}
		m.live[o.name] = x
		m.liveData[x] = ""
		m.metaOp(mop{k: kCreate, name: o.name, o: x})
	case kWrite:
		x := m.live[o.name]
		m.data(x, dop{write: true, off: len(m.liveData[x]), data: o.data})
	case kTrunc:
		x := m.live[o.name]
		m.data(x, dop{size: o.size})
	case kSync:
		if m.ordered {
			m.commitAll()
			return
		}
		x := m.live[o.name]
		for _, d := range x.pending {
			x.durable = d.apply(x.durable)
		}
		x.pending = nil
	case kSyncDir:
		if m.ordered {
			m.commitAll()
			return
		}
		last := -1
		for i, x := range m.meta {
			if dirOf(x.name) == o.name || (x.k == kRename && dirOf(x.to) == o.name) {
				last = i
			}
		}
		for _, x := range m.meta[:last+1] {
			applyMeta(m.durNS, x)
		}
		m.meta = m.meta[last+1:]
	case kRename:
		x := m.live[o.name]
		delete(m.live, o.name)
		m.live[o.to] = x
		m.metaOp(mop{k: kRename, name: o.name, to: o.to, o: x})
	case kRemove:
		delete(m.live, o.name)
		m.metaOp(mop{k: kRemove, name: o.name})
	}
}

func render(ns map[string]*obj, content func(*obj) string) string {
	var parts []string
	for _, n := range slices.Sorted(maps.Keys(ns)) {
		parts = append(parts, n+"="+content(ns[n]))
	}
	return strings.Join(parts, ";")
}

// tornVariants lists what a write d may leave if it is torn: a strict,
// non-empty prefix of its data.
func tornVariants(s string, d dop) []string {
	var out []string
	if !d.write || len(d.data) < 2 {
		return nil
	}
	for n := 1; n < len(d.data); n++ {
		out = append(out, dop{write: true, off: d.off, data: d.data[:n]}.apply(s))
	}
	return out
}

// legal lists every state a crash may leave.
func (m *model) legal() map[string]bool {
	out := map[string]bool{}
	if m.ordered {
		for k := 0; k <= len(m.all); k++ {
			ns := maps.Clone(m.durNS)
			cont := map[*obj]string{}
			get := func(o *obj) string {
				if c, ok := cont[o]; ok {
					return c
				}
				return o.durable
			}
			for _, e := range m.all[:k] {
				if e.data {
					cont[e.o] = e.d.apply(get(e.o))
				} else {
					applyMeta(ns, e.m)
				}
			}
			out[render(ns, get)] = true
			if k < len(m.all) && m.all[k].data {
				e := m.all[k]
				for _, v := range tornVariants(get(e.o), e.d) {
					out[render(ns, func(o *obj) string {
						if o == e.o {
							return v
						}
						return get(o)
					})] = true
				}
			}
		}
		return out
	}
	for k := 0; k <= len(m.meta); k++ {
		ns := maps.Clone(m.durNS)
		for _, x := range m.meta[:k] {
			applyMeta(ns, x)
		}
		names := slices.Sorted(maps.Keys(ns))
		var rec func(i int, acc map[*obj]string)
		rec = func(i int, acc map[*obj]string) {
			if i == len(names) {
				out[render(ns, func(o *obj) string { return acc[o] })] = true
				return
			}
			o := ns[names[i]]
			if _, done := acc[o]; done {
				rec(i+1, acc)
				return
			}
			s := o.durable
			for j := 0; j <= len(o.pending); j++ {
				opts := []string{s}
				if j < len(o.pending) {
					opts = append(opts, tornVariants(s, o.pending[j])...)
				}
				for _, v := range opts {
					next := maps.Clone(acc)
					next[o] = v
					rec(i+1, next)
				}
				if j < len(o.pending) {
					s = o.pending[j].apply(s)
				}
			}
		}
		rec(0, map[*obj]string{})
	}
	return out
}

// ---- the same sequence on the simulated disk ----

func play(t *testing.T, ops []fop, seed uint64, ordered bool) (got string, legal map[string]bool) {
	t.Helper()
	m := newModel(ordered)
	var failure error
	sim := simworld.New(simworld.Options{Seed: seed})
	disk := sim.AddMachine("disk", simworld.MachineOptions{Disk: simfs.Options{Ordered: ordered, TornWriteProb: 0.5}})
	res := sim.Run(func(*dst.World) {
		fsys := disk.World.FS()
		check := func(err error) {
			if err != nil && failure == nil {
				failure = err
			}
		}
		check(fsys.Mkdir("d", 0o755))
		check(dst.SyncDir(fsys, "."))
		for _, o := range ops {
			switch o.k {
			case kCreate:
				f, err := fsys.OpenFile(o.name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				check(err)
				if err == nil {
					f.Close()
				}
			case kWrite, kTrunc, kSync:
				f, err := fsys.OpenFile(o.name, os.O_WRONLY|os.O_APPEND, 0)
				check(err)
				switch o.k {
				case kWrite:
					_, err = f.Write([]byte(o.data))
				case kTrunc:
					err = f.Truncate(int64(o.size))
				default:
					err = f.Sync()
				}
				check(err)
				f.Close()
			case kSyncDir:
				check(dst.SyncDir(fsys, o.name))
			case kRename:
				check(fsys.Rename(o.name, o.to))
			case kRemove:
				check(fsys.Remove(o.name))
			}
			m.do(o)
			// [read-your-writes]: the live view always matches the model.
			for _, n := range files {
				b, err := dst.ReadFile(fsys, n)
				x, want := m.live[n]
				if want != (err == nil) || (want && string(b) != m.liveData[x]) {
					check(fmt.Errorf("after %v, %s reads %q (err %v); want exists=%v %q", o, n, b, err, want, m.liveData[x]))
				}
			}
		}
		legal = m.legal()
		disk.Crash()
		ns := map[string]*obj{}
		cont := map[*obj]string{}
		for _, n := range files {
			b, err := dst.ReadFile(fsys, n)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			check(err)
			o := &obj{}
			ns[n], cont[o] = o, string(b)
		}
		got = render(ns, func(o *obj) string { return cont[o] })
	})
	if res.Err != nil {
		t.Fatalf("%v: %v", ops, res.Err)
	}
	if failure != nil {
		t.Fatalf("%v: %v", ops, failure)
	}
	return got, legal
}

func modeName(ordered bool) string {
	if ordered {
		return "ordered"
	}
	return "harsh"
}

// Verifies: [crash-is-legal], [sync-is-durable], [behaves-like-os]
// Fails if: a crash leaves a state the rules don't allow (including losing
// synced data or names), in either mode.
func TestCrashIsLegal(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		r := rand.New(rand.NewPCG(42, 1))
		for i := range 2000 {
			ops := gen(r, 1+r.IntN(10))
			for s := range uint64(3) {
				got, legal := play(t, ops, uint64(i)*8+s, ordered)
				if !legal[got] {
					t.Fatalf("%s: after %v and a crash, %q isn't legal; legal:\n  %s", modeName(ordered), ops, got, strings.Join(slices.Sorted(maps.Keys(legal)), "\n  "))
				}
			}
		}
	}
}

// Verifies: [crash-explores]
// Fails if: some legal crash state never happens: a fake kinder than a
// real disk passes the test above and fails this one.
func TestCrashExplores(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		r := rand.New(rand.NewPCG(7, 1))
		checked := 0
		for range 300 {
			ops := gen(r, 1+r.IntN(5))
			_, legal := play(t, ops, 0, ordered)
			if len(legal) > 16 {
				continue
			}
			checked++
			seen := map[string]bool{}
			for s := uint64(1); s <= 3000 && len(seen) < len(legal); s++ {
				got, _ := play(t, ops, s, ordered)
				seen[got] = true
			}
			for o := range legal {
				if !seen[o] {
					t.Fatalf("%s: after %v, legal state %q never happened in 3000 seeds (saw %v)", modeName(ordered), ops, o, slices.Sorted(maps.Keys(seen)))
				}
			}
		}
		if checked < 100 {
			t.Fatalf("%s: only %d sequences small enough to explore", modeName(ordered), checked)
		}
	}
}

// The model must be able to tell modes apart: ordered outcomes are a
// subset of harsh ones, and strictly smaller for some sequence.
func TestModesDiffer(t *testing.T) {
	ops := []fop{{k: kCreate, name: "a"}, {k: kCreate, name: "b"}, {k: kWrite, name: "a", data: "x"}, {k: kWrite, name: "b", data: "y"}}
	harsh, ordered := newModel(false), newModel(true)
	for _, o := range ops {
		harsh.do(o)
		ordered.do(o)
	}
	h, o := harsh.legal(), ordered.legal()
	for s := range o {
		if !h[s] {
			t.Fatalf("ordered state %q isn't a harsh one", s)
		}
	}
	if len(o) >= len(h) {
		t.Fatalf("ordered %d states, harsh %d: want fewer", len(o), len(h))
	}
}

// playKill plays ops, then kills the disk's machine (or, with power,
// crashes it), reads the disk, restarts the machine and crashes it again
// (a power loss): what the disk held live before, after the kill, and
// after the later power loss; and the states a power loss after ops may
// leave. A handle opened before the kill must be stale after it.
func playKill(t *testing.T, ops []fop, seed uint64, ordered, power bool) (live, afterKill, afterPower string, legal map[string]bool) {
	t.Helper()
	m := newModel(ordered)
	var failure error
	sim := simworld.New(simworld.Options{Seed: seed})
	disk := sim.AddMachine("disk", simworld.MachineOptions{Disk: simfs.Options{Ordered: ordered, TornWriteProb: 0.5}})
	read := func(fsys dst.FS, check func(error)) string {
		ns := map[string]*obj{}
		cont := map[*obj]string{}
		for _, n := range files {
			b, err := dst.ReadFile(fsys, n)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			check(err)
			o := &obj{}
			ns[n], cont[o] = o, string(b)
		}
		return render(ns, func(o *obj) string { return cont[o] })
	}
	res := sim.Run(func(*dst.World) {
		fsys := disk.World.FS()
		check := func(err error) {
			if err != nil && failure == nil {
				failure = err
			}
		}
		check(fsys.Mkdir("d", 0o755))
		check(dst.SyncDir(fsys, "."))
		for _, o := range ops {
			switch o.k {
			case kCreate:
				f, err := fsys.OpenFile(o.name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				check(err)
				if err == nil {
					f.Close()
				}
			case kWrite, kTrunc, kSync:
				f, err := fsys.OpenFile(o.name, os.O_WRONLY|os.O_APPEND, 0)
				check(err)
				switch o.k {
				case kWrite:
					_, err = f.Write([]byte(o.data))
				case kTrunc:
					err = f.Truncate(int64(o.size))
				default:
					err = f.Sync()
				}
				check(err)
				f.Close()
			case kSyncDir:
				check(dst.SyncDir(fsys, o.name))
			case kRename:
				check(fsys.Rename(o.name, o.to))
			case kRemove:
				check(fsys.Remove(o.name))
			}
			m.do(o)
		}
		legal = m.legal()
		live = render(m.live, func(o *obj) string { return m.liveData[o] })
		held, _ := fsys.OpenFile(".", os.O_RDONLY, 0)
		if power {
			disk.Crash()
		} else {
			disk.Kill()
		}
		if _, err := held.ReadDir(-1); !errors.Is(err, simfs.ErrStale) {
			check(fmt.Errorf("a handle opened before the crash: %v, want ESTALE", err))
		}
		afterKill = read(fsys, check)
		sim.S.Restart(disk.M)
		disk.Crash()
		afterPower = read(fsys, check)
	})
	if res.Err != nil {
		t.Fatalf("%v: %v", ops, res.Err)
	}
	if failure != nil {
		t.Fatalf("%v: %v", ops, failure)
	}
	return live, afterKill, afterPower, legal
}

// Verifies: [kill-keeps-cache]
// Fails if: after a kill the disk doesn't read exactly as it did live
// (something written was lost, or a handle survived); or a power loss
// after the kill leaves an illegal state, or never loses anything (the
// kill made everything durable). The negative case: a power-loss crash in
// the kill's place differs from the live state on some seed, so a kill
// that behaved like one would fail.
func TestKillKeepsCache(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		r := rand.New(rand.NewPCG(11, 1))
		lostLater, powerDiffers := 0, 0
		for i := range 500 {
			ops := gen(r, 1+r.IntN(10))
			seed := uint64(i)
			live, afterKill, afterPower, legal := playKill(t, ops, seed, ordered, false)
			if afterKill != live {
				t.Fatalf("%s: after %v and a kill, the disk reads %q; live it was %q", modeName(ordered), ops, afterKill, live)
			}
			if !legal[afterPower] {
				t.Fatalf("%s: after %v, a kill, then a power loss: %q isn't a legal crash state", modeName(ordered), ops, afterPower)
			}
			if afterPower != live {
				lostLater++
			}
			if pl, afterCrash, _, _ := playKill(t, ops, seed, ordered, true); afterCrash != pl {
				powerDiffers++
			}
		}
		if lostLater == 0 {
			t.Errorf("%s: a power loss after a kill never lost anything in 500 sequences: the kill made writes durable", modeName(ordered))
		}
		if powerDiffers == 0 {
			t.Errorf("%s: negative case: a power loss in the kill's place never differed from the live state", modeName(ordered))
		}
		t.Logf("%s: 500 sequences; a later power loss lost something in %d, a power loss instead of the kill in %d", modeName(ordered), lostLater, powerDiffers)
	}
}

// Verifies: [lock-dies-with-process]
// Fails if: a lock survives a crash or a kill of its holder's machine; or
// something else releases it (the negative: while the machine lives, the
// lock stays held, and a dead holder's unlock doesn't release the lock
// the next incarnation took).
func TestLockDiesWithProcess(t *testing.T) {
	for _, kill := range []bool{false, true} {
		sim := simworld.New(simworld.Options{Seed: 1})
		disk := sim.AddMachine("disk", simworld.MachineOptions{Disk: simfs.Options{}})
		var failure error
		check := func(cond bool, format string, args ...any) {
			if !cond && failure == nil {
				failure = fmt.Errorf(format, args...)
			}
		}
		res := sim.Run(func(*dst.World) {
			fsys := disk.World.FS()
			oldUnlock, err := dst.LockFile(fsys, "LOCK")
			check(err == nil, "lock: %v", err)
			// The lock file survives the crash too, so both incarnations
			// lock the same file.
			check(dst.SyncDir(fsys, ".") == nil, "sync dir")
			_, err = dst.LockFile(fsys, "LOCK")
			check(errors.Is(err, dst.ErrLocked), "while held, before any crash: %v, want ErrLocked", err)
			if kill {
				disk.Kill()
			} else {
				disk.Crash()
			}
			sim.S.Restart(disk.M)
			newUnlock, err := dst.LockFile(fsys, "LOCK")
			check(err == nil, "after the holder's machine died, lock: %v", err)
			check(oldUnlock() == nil, "the dead holder's unlock failed")
			_, err = dst.LockFile(fsys, "LOCK")
			check(errors.Is(err, dst.ErrLocked), "the dead holder's unlock released the new holder's lock: %v", err)
			check(newUnlock() == nil, "unlock failed")
			_, err = dst.LockFile(fsys, "LOCK")
			check(err == nil, "after unlock: %v", err)
		})
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		if failure != nil {
			t.Fatalf("kill %v: %v", kill, failure)
		}
	}
}
