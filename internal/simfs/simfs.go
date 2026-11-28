// Package simfs is a simulated disk for one machine: an in-memory
// filesystem with os semantics, behind a page cache that a crash empties
// the way real kernels and disks may.
//
// It is adversarial but legal. Live operations behave exactly like os (the
// contract suite runs against both). What survives a crash is anything
// POSIX and real journaling filesystems allow, chosen by the seed:
//
//   - Data written to a file is durable only after the file's Sync.
//   - Directory changes (create, mkdir, link, rename, remove) form one ordered
//     journal: a crash keeps a prefix of the unsynced ones, and a Sync of
//     a directory makes the journal durable up to that directory's last
//     change. A rename is atomic.
//   - By default each file's unsynced writes survive independently of other
//     files' (as on ext4 data=writeback or XFS), and a file's Sync covers its
//     own data only. With Ordered, everything is one queue in issue order
//     and any Sync commits everything before it, like a journal commit on
//     ext4's default data=ordered.
//   - The first lost write of a file may survive in part (torn).
//   - With HoleProb, in harsh mode, a file's unsynced writes may instead
//     reach the disk out of order, page by page: after a crash each page
//     they touched holds either its last written content or what was
//     durable, independently of the other pages, and the file's size is
//     either the durable one or the last written one (so a size past the
//     data written reads as zeros, and a page not written back shows the
//     file's older content, as in a reused file). Files with an unsynced
//     truncate keep the prefix rule.
//   - Power can fail inside a Write, Sync or directory operation.
//   - Open files go stale after a crash.
//   - A kill (the process dies, the host stays up) keeps the page cache:
//     every completed write and directory change is still there after
//     restart, and still unsynced.
//
// Disk errors, as Linux reports them (Options' error rates, and Fill):
//
//   - A full disk (Fill) stays full: from then on, until StopFaults, the
//     disk holds no more bytes than it did. A write that doesn't fit
//     writes what does and fails with ENOSPC (as os.File does after a
//     short write); creating a file or directory fails with ENOSPC while
//     no byte is free; removing or truncating files frees space under the
//     cap. Nothing
//     already written is lost. (Space is freed at Remove, even while the
//     file is open.)
//   - A Sync of a file with unsynced writes may fail with EIO, and then
//     those writes will never be durable: they stay readable until a
//     crash, which loses them, and the next Sync succeeds (Linux marks
//     the pages clean after a writeback error and reports the error
//     once: PostgreSQL's "fsyncgate"). Retrying the Sync doesn't help;
//     only rewriting the data does. Directory Syncs don't fail.
//   - A Write or Read may fail with EIO, doing nothing.
//
// Modes and times (Chmod, Chtimes) are durable at once. Not modelled: a
// lost chmod, a failed fsync's pages being evicted (so reads return the
// old data), bit rot, misdirected writes.
//
// Verified:
//   - [behaves-like-os] Without crashes, every operation matches the os
//     package (the FS contract suite passes against both).
//   - [crash-is-legal] After a crash, the disk is in a state the rules
//     above allow, in either mode.
//   - [crash-explores] Every state the rules allow happens for some seed.
//   - [sync-is-durable] Data covered by a completed Sync, and names covered
//     by a completed directory Sync, survive every crash.
//   - [holes-own-stream] Hole decisions draw only from a stream of their
//     own: a disk whose hole draws all come out "no" crashes exactly as a
//     disk without holes, so turning holes on leaves every other draw as
//     it was.
//   - [kill-keeps-cache] After a kill, every completed write and directory
//     change is visible, and none became durable: a later power loss may
//     still lose any of it.
//   - [full-stays-full] After Fill, nothing grows the disk past what it
//     held: a write writes exactly the bytes that fit and fails with
//     ENOSPC, creates fail with ENOSPC while no byte is free, removing or
//     truncating frees space that can be written again, nothing written before is lost, and
//     StopFaults lifts the cap.
//   - [fsync-error-forgets] A Sync that fails with EIO makes exactly that
//     file's unsynced writes non-durable for good (still readable until a
//     crash, gone after it, though a later Sync succeeds); other files'
//     writes are unaffected.
//   - [io-error-does-nothing] A Write or Read that fails with EIO
//     transfers no bytes and moves no offset.
//
// Depends on: internal/sched
package simfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
)

// Options configure a disk's fault behaviour.
type Options struct {
	// Ordered makes the disk write back in issue order (see package doc).
	Ordered bool
	// TornWriteProb is the chance, per crash and file, that the first lost
	// write survives in part.
	TornWriteProb float64
	// IOCrashProb is the chance that power fails inside each Write, Sync or
	// directory operation, crashing the machine.
	IOCrashProb float64
	// SyncLatency is the mean time a Sync takes (exponential, capped at 10x).
	// Zero means syncs are instantaneous (but still scheduling points).
	SyncLatency time.Duration
	// NoDurability makes every operation durable at once: crashes lose
	// nothing. For tests that aren't about the disk.
	NoDurability bool
	// SyncErrProb, WriteErrProb and ReadErrProb are the chances that a
	// Sync of a file with unsynced writes, a Write, or a Read fails with
	// EIO (see the package doc).
	SyncErrProb, WriteErrProb, ReadErrProb float64
	// HoleProb is the chance, per crash and file with unsynced writes (in
	// harsh mode), that the file is written back out of order, page by
	// page (see the package doc).
	HoleProb float64
	// PageSize is the page size for holes (0: 4096). Tests make it small.
	PageSize int
}

// ErrStale is returned by a file handle opened before a crash.
var ErrStale = syscall.ESTALE

// FS is one machine's disk.
type FS struct {
	opts    Options
	s       *sched.Scheduler
	m       *sched.Machine
	rng     *rand.Rand
	holes   *rand.Rand // hole decisions only ([holes-own-stream])
	root    *inode
	gen     int // incremented by each crash; older handles are stale
	nextIno int
	seq     uint64
	pending []*op          // unsynced operations in issue order
	locks   map[*inode]int // advisory locks (LockFile) held, by holder
	lockN   int
	// OnCrashLoss, if set, is told after each crash how many unsynced
	// operations were lost and whether a write was torn (for coverage).
	OnCrashLoss func(lost int, torn bool)
	// OnHoles, if set, is told after a crash that left a file with holes
	// (for coverage).
	OnHoles func()
	// OnError, if set, is told of each injected error: "full" (an ENOSPC),
	// "sync", "write" or "read" (an EIO). For coverage.
	OnError func(kind string)
	// full: Fill capped the disk at capacity bytes (until StopFaults).
	full     bool
	capacity int64
	temps    int // names MkdirTemp and CreateTemp have tried
}

type inode struct {
	id      int
	dir     bool
	mode    fs.FileMode
	modTime time.Time
	// live is what reads see; durable is what survives a crash.
	live, durable           []byte
	liveDents, durableDents map[string]*inode
}

type opKind int

const (
	opWrite opKind = iota
	opTruncate
	opCreate // name -> ino in dir (files and directories)
	opRemove
	opRename
)

type op struct {
	seq  uint64
	kind opKind
	ino  *inode // data ops: the file; create/rename: the entry's inode
	// data ops
	off  int64
	data []byte
	size int64
	// directory ops
	dir, dir2   *inode
	name, name2 string
}

func (o *op) isData() bool { return o.kind == opWrite || o.kind == opTruncate }

// touches reports whether o concerns x (a file's data, or a directory's
// entries).
func (o *op) touches(x *inode) bool {
	if o.isData() {
		return o.ino == x
	}
	return o.dir == x || o.dir2 == x
}

// New returns an empty disk for machine m. Its root directory exists and
// is durable.
func New(s *sched.Scheduler, m *sched.Machine, opts Options) *FS {
	f := &FS{opts: opts, s: s, m: m, rng: s.Rand("machine/" + m.Name + "/disk"), holes: s.Rand("machine/" + m.Name + "/disk/holes")}
	f.root = f.newInode(true, fs.ModeDir|0o755)
	m.OnCrash(f.crash)
	return f
}

func (f *FS) newInode(dir bool, mode fs.FileMode) *inode {
	f.nextIno++
	ino := &inode{id: f.nextIno, dir: dir, mode: mode, modTime: f.now()}
	if dir {
		ino.liveDents, ino.durableDents = map[string]*inode{}, map[string]*inode{}
	}
	return ino
}

// now is the machine's local time (file times come from its clock).
func (f *FS) now() time.Time {
	return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).Add(f.m.LocalNow())
}

// ---- live operations ----

// issue records an operation as pending (or durable at once with
// NoDurability). Its live effect is applied by the caller.
func (f *FS) issue(o *op) {
	f.seq++
	o.seq = f.seq
	if f.opts.NoDurability {
		f.makeDurable(o)
		return
	}
	f.pending = append(f.pending, o)
}

// powerLoss may crash the machine (never returning) inside an I/O call.
func (f *FS) powerLoss() {
	if f.opts.IOCrashProb > 0 && f.rng.Float64() < f.opts.IOCrashProb {
		f.s.Logf("power loss on %s inside an I/O call", f.m.Name)
		f.s.Crash(f.m)
	}
}

// ioError decides an injected EIO with probability prob (drawing nothing
// when prob is 0, so seeds without errors keep their schedule).
func (f *FS) ioError(prob float64, kind string) bool {
	if prob <= 0 || f.rng.Float64() >= prob {
		return false
	}
	f.s.Logf("disk of %s: %s failed with EIO", f.m.Name, kind)
	if f.OnError != nil {
		f.OnError(kind)
	}
	return true
}

// Fill makes the disk full from now on: it holds no more bytes than it
// does now, until StopFaults (see the package doc).
func (f *FS) Fill() {
	f.full, f.capacity = true, f.used()
	f.s.Logf("disk of %s is full (%d bytes)", f.m.Name, f.capacity)
}

// room is how many more bytes the disk takes (-1: unlimited).
func (f *FS) room() int64 {
	if !f.full {
		return -1
	}
	return max(0, f.capacity-f.used())
}

// noSpace reports an ENOSPC for coverage.
func (f *FS) noSpace() {
	if f.OnError != nil {
		f.OnError("full")
	}
}

// used is the bytes the live tree's files hold.
func (f *FS) used() int64 {
	var n int64
	seen := map[*inode]bool{}
	var walk func(d *inode)
	walk = func(d *inode) {
		for _, c := range d.liveDents {
			if seen[c] {
				continue
			}
			seen[c] = true
			if c.dir {
				walk(c)
			} else {
				n += int64(len(c.live))
			}
		}
	}
	walk(f.root)
	return n
}

// enter is the start of every operation: a scheduling point.
func (f *FS) enter() { f.s.Yield() }

// clean turns a name relative to the root into an absolute path inside
// it. Like os.Root, it rejects absolute names and names that escape.
func clean(name string) (string, error) {
	if name == "" {
		return "", fs.ErrNotExist
	}
	c := path.Clean(name)
	if path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return "", errors.New("path escapes from parent")
	}
	if c == "." {
		return "/", nil
	}
	return "/" + c, nil
}

// lookup resolves a cleaned path to its inode in the live tree.
func (f *FS) lookup(p string) (*inode, error) {
	ino := f.root
	if p == "/" {
		return ino, nil
	}
	for _, part := range strings.Split(p[1:], "/") {
		if !ino.dir {
			return nil, syscall.ENOTDIR
		}
		next, ok := ino.liveDents[part]
		if !ok {
			return nil, fs.ErrNotExist
		}
		ino = next
	}
	return ino, nil
}

// parent resolves the directory containing p and p's last element.
func (f *FS) parent(p string) (*inode, string, error) {
	if p == "/" {
		return nil, "", syscall.EINVAL
	}
	dir, base := path.Split(p)
	d, err := f.lookup(path.Clean(dir))
	if err != nil {
		return nil, "", err
	}
	if !d.dir {
		return nil, "", syscall.ENOTDIR
	}
	return d, base, nil
}

func pathErr(op, name string, err error) error { return &fs.PathError{Op: op, Path: name, Err: err} }

// OpenFile is os.OpenFile.
func (f *FS) OpenFile(name string, flag int, perm fs.FileMode) (dst.File, error) {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return nil, pathErr("open", name, err)
	}
	ino, err := f.lookup(p)
	switch {
	case errors.Is(err, fs.ErrNotExist) && flag&os.O_CREATE != 0:
		d, base, perr := f.parent(p)
		if perr != nil {
			return nil, pathErr("open", name, perr)
		}
		if f.room() == 0 {
			f.noSpace()
			return nil, pathErr("open", name, syscall.ENOSPC)
		}
		ino = f.newInode(false, perm&fs.ModePerm)
		d.liveDents[base] = ino
		d.modTime = f.now()
		f.issue(&op{kind: opCreate, dir: d, name: base, ino: ino})
		f.powerLoss()
	case err != nil:
		return nil, pathErr("open", name, err)
	case flag&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL:
		return nil, pathErr("open", name, fs.ErrExist)
	case ino.dir && flag&(os.O_WRONLY|os.O_RDWR) != 0:
		return nil, pathErr("open", name, syscall.EISDIR)
	}
	h := &file{fs: f, ino: ino, name: name, flag: flag, gen: f.gen}
	if flag&os.O_TRUNC != 0 && !ino.dir && len(ino.live) > 0 {
		ino.live = ino.live[:0:0]
		ino.modTime = f.now()
		f.issue(&op{kind: opTruncate, ino: ino, size: 0})
	}
	return h, nil
}

// Mkdir is os.Mkdir.
func (f *FS) Mkdir(name string, perm fs.FileMode) error {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return pathErr("mkdir", name, err)
	}
	if _, err := f.lookup(p); err == nil {
		return pathErr("mkdir", name, fs.ErrExist)
	}
	d, base, err := f.parent(p)
	if err != nil {
		return pathErr("mkdir", name, err)
	}
	if f.room() == 0 {
		f.noSpace()
		return pathErr("mkdir", name, syscall.ENOSPC)
	}
	ino := f.newInode(true, fs.ModeDir|perm&fs.ModePerm)
	d.liveDents[base] = ino
	f.issue(&op{kind: opCreate, dir: d, name: base, ino: ino})
	f.powerLoss()
	return nil
}

// MkdirAll is os.MkdirAll.
func (f *FS) MkdirAll(name string, perm fs.FileMode) error {
	p, err := clean(name)
	if err != nil {
		return pathErr("mkdir", name, err)
	}
	cur := ""
	for _, part := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if part == "" {
			continue
		}
		cur += "/" + part
		ino, err := f.lookup(cur)
		if err == nil {
			if !ino.dir {
				return pathErr("mkdir", cur[1:], syscall.ENOTDIR)
			}
			continue
		}
		if err := f.Mkdir(cur[1:], perm); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return nil
}

// Remove is os.Remove.
func (f *FS) Remove(name string) error {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return pathErr("remove", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return pathErr("remove", name, err)
	}
	if ino.dir && len(ino.liveDents) > 0 {
		return pathErr("remove", name, syscall.ENOTEMPTY)
	}
	d, base, err := f.parent(p)
	if err != nil {
		return pathErr("remove", name, err)
	}
	delete(d.liveDents, base)
	f.issue(&op{kind: opRemove, dir: d, name: base})
	f.powerLoss()
	return nil
}

// RemoveAll is os.RemoveAll.
func (f *FS) RemoveAll(name string) error {
	p, err := clean(name)
	if err != nil {
		return pathErr("removeall", name, err)
	}
	ino, err := f.lookup(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return pathErr("removeall", name, err)
	}
	if ino.dir {
		for _, child := range slices.Sorted(maps.Keys(ino.liveDents)) {
			if err := f.RemoveAll(strings.TrimPrefix(p+"/"+child, "/")); err != nil {
				return err
			}
		}
	}
	return f.Remove(name)
}

// Rename is os.Rename: atomic, replacing a file at newpath.
func (f *FS) Rename(oldpath, newpath string) error {
	f.enter()
	linkErr := func(err error) error { return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: err} }
	op1, err := clean(oldpath)
	if err != nil {
		return linkErr(err)
	}
	op2, err := clean(newpath)
	if err != nil {
		return linkErr(err)
	}
	ino, err := f.lookup(op1)
	if err != nil {
		return linkErr(err)
	}
	d1, b1, err := f.parent(op1)
	if err != nil {
		return linkErr(err)
	}
	d2, b2, err := f.parent(op2)
	if err != nil {
		return linkErr(err)
	}
	if target, ok := d2.liveDents[b2]; ok {
		if target == ino {
			return nil
		}
		if target.dir != ino.dir {
			if target.dir {
				return linkErr(syscall.EISDIR)
			}
			return linkErr(syscall.ENOTDIR)
		}
		if target.dir && len(target.liveDents) > 0 {
			return linkErr(syscall.ENOTEMPTY)
		}
	}
	if ino.dir && strings.HasPrefix(op2+"/", op1+"/") {
		return linkErr(syscall.EINVAL)
	}
	delete(d1.liveDents, b1)
	d2.liveDents[b2] = ino
	f.issue(&op{kind: opRename, dir: d1, name: b1, dir2: d2, name2: b2, ino: ino})
	f.powerLoss()
	return nil
}

// Link is os.Link: newname becomes a second name for oldname's file (a
// hard link). Like a create, the new name is a directory change: durable
// once its directory is synced. The file's data is the same through
// either name, and lasts while any name does.
func (f *FS) Link(oldname, newname string) error {
	f.enter()
	linkErr := func(err error) error { return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: err} }
	p1, err := clean(oldname)
	if err != nil {
		return linkErr(err)
	}
	p2, err := clean(newname)
	if err != nil {
		return linkErr(err)
	}
	ino, err := f.lookup(p1)
	if err != nil {
		return linkErr(err)
	}
	if ino.dir {
		return linkErr(syscall.EPERM) // Linux refuses hard links to directories
	}
	d, base, err := f.parent(p2)
	if err != nil {
		return linkErr(err)
	}
	if _, ok := d.liveDents[base]; ok {
		return linkErr(fs.ErrExist)
	}
	if f.room() == 0 {
		f.noSpace()
		return linkErr(syscall.ENOSPC)
	}
	d.liveDents[base] = ino
	d.modTime = f.now()
	f.issue(&op{kind: opCreate, dir: d, name: base, ino: ino})
	f.powerLoss()
	return nil
}

// Chmod is os.Chmod (dst.Chmoder): the permission bits and the setuid,
// setgid and sticky bits. Modes and times aren't crash-modelled: a change
// is durable at once.
func (f *FS) Chmod(name string, mode fs.FileMode) error {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return pathErr("chmod", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return pathErr("chmod", name, err)
	}
	chmod(ino, mode)
	return nil
}

// chmodBits are the mode bits Chmod changes, as os's syscallMode keeps.
const chmodBits = fs.ModePerm | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky

func chmod(ino *inode, mode fs.FileMode) { ino.mode = ino.mode&^chmodBits | mode&chmodBits }

// Chtimes is os.Chtimes (dst.Chtimeser): a zero time leaves the
// modification time as it was; access times aren't kept.
func (f *FS) Chtimes(name string, atime, mtime time.Time) error {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return pathErr("chtimes", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return pathErr("chtimes", name, err)
	}
	if !mtime.IsZero() {
		ino.modTime = mtime
	}
	return nil
}

// Truncate is os.Truncate (dst.Truncater): a truncate of the file's data,
// durable like one made through an open file.
func (f *FS) Truncate(name string, size int64) error {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return pathErr("truncate", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return pathErr("truncate", name, err)
	}
	if ino.dir {
		return pathErr("truncate", name, syscall.EISDIR)
	}
	if size < 0 {
		return pathErr("truncate", name, syscall.EINVAL)
	}
	if room := f.room(); room >= 0 && size-int64(len(ino.live)) > room {
		f.noSpace()
		return pathErr("truncate", name, syscall.ENOSPC)
	}
	ino.live = truncate(ino.live, size)
	ino.modTime = f.now()
	f.issue(&op{kind: opTruncate, ino: ino, size: size})
	f.powerLoss()
	return nil
}

// tempPrefix is os's prefixAndSuffix and joinPath: the parts of a
// temporary name around its unique part, under dir ("" is "tmp", made if
// missing).
func (f *FS) tempPrefix(op, dir, pattern string) (prefix, suffix string, err error) {
	if strings.ContainsRune(pattern, '/') {
		return "", "", pathErr(op, pattern, errors.New("pattern contains path separator"))
	}
	if dir == "" {
		dir = "tmp"
		if err := f.MkdirAll(dir, 0o777); err != nil {
			return "", "", err
		}
	}
	prefix = pattern
	if i := strings.LastIndexByte(pattern, '*'); i >= 0 {
		prefix, suffix = pattern[:i], pattern[i+1:]
	}
	if strings.HasSuffix(dir, "/") {
		return dir + prefix, suffix, nil
	}
	return dir + "/" + prefix, suffix, nil
}

// nextTemp is the next unique part of a temporary name: a counter, so the
// same seed makes the same names.
func (f *FS) nextTemp() string {
	f.temps++
	return fmt.Sprint(f.temps)
}

// MkdirTemp is os.MkdirTemp (dst.TempMaker).
func (f *FS) MkdirTemp(dir, pattern string) (string, error) {
	prefix, suffix, err := f.tempPrefix("mkdirtemp", dir, pattern)
	if err != nil {
		return "", err
	}
	for try := 0; ; try++ {
		name := prefix + f.nextTemp() + suffix
		err := f.Mkdir(name, 0o700)
		if err == nil {
			return name, nil
		}
		if errors.Is(err, fs.ErrExist) && try < 10000 {
			continue
		}
		if errors.Is(err, fs.ErrNotExist) {
			if _, serr := f.Stat(dir); errors.Is(serr, fs.ErrNotExist) {
				return "", serr
			}
		}
		return "", err
	}
}

// CreateTemp is os.CreateTemp (dst.TempMaker).
func (f *FS) CreateTemp(dir, pattern string) (dst.File, error) {
	prefix, suffix, err := f.tempPrefix("createtemp", dir, pattern)
	if err != nil {
		return nil, err
	}
	for try := 0; ; try++ {
		file, err := f.OpenFile(prefix+f.nextTemp()+suffix, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, fs.ErrExist) {
			if try < 10000 {
				continue
			}
			return nil, pathErr("createtemp", prefix+"*"+suffix, fs.ErrExist)
		}
		return file, err
	}
}

// Stat is os.Stat.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return nil, pathErr("stat", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return nil, pathErr("stat", name, err)
	}
	return info{name: path.Base(p), ino: ino}, nil
}

// ReadDir is os.ReadDir: entries sorted by name.
func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	f.enter()
	p, err := clean(name)
	if err != nil {
		return nil, pathErr("open", name, err)
	}
	ino, err := f.lookup(p)
	if err != nil {
		return nil, pathErr("open", name, err)
	}
	if !ino.dir {
		return nil, pathErr("readdirent", name, syscall.ENOTDIR)
	}
	return dirEntries(ino), nil
}

func dirEntries(d *inode) []fs.DirEntry {
	var out []fs.DirEntry
	for _, n := range slices.Sorted(maps.Keys(d.liveDents)) {
		out = append(out, fs.FileInfoToDirEntry(info{name: n, ino: d.liveDents[n]}))
	}
	return out
}

type info struct {
	name string
	ino  *inode
}

func (i info) Name() string { return i.name }
func (i info) Size() int64 {
	if i.ino.dir {
		return 0
	}
	return int64(len(i.ino.live))
}
func (i info) Mode() fs.FileMode  { return i.ino.mode }
func (i info) ModTime() time.Time { return i.ino.modTime }
func (i info) IsDir() bool        { return i.ino.dir }
func (i info) Sys() any           { return nil }

// ---- durability ----

// sync makes durable every pending operation that a Sync of x covers.
func (f *FS) sync(x *inode) error {
	f.powerLoss()
	if f.opts.SyncLatency > 0 {
		mean := float64(f.opts.SyncLatency)
		d := time.Duration(min(f.rng.ExpFloat64()*mean, 10*mean))
		f.s.Sleep(d)
	}
	if !x.dir && f.opts.SyncErrProb > 0 && slices.ContainsFunc(f.pending, func(o *op) bool { return o.isData() && o.ino == x }) &&
		f.ioError(f.opts.SyncErrProb, "sync") {
		// A writeback error: the file's unsynced writes are dropped from
		// the page cache's dirty set, never to be durable.
		f.pending = slices.DeleteFunc(f.pending, func(o *op) bool { return o.isData() && o.ino == x })
		return syscall.EIO
	}
	last := -1
	for i, o := range f.pending {
		if o.touches(x) {
			last = i
		}
	}
	var keep []*op
	for i, o := range f.pending {
		durable := false
		switch {
		case f.opts.Ordered:
			durable = true // a journal commit: everything issued so far
		case x.dir:
			durable = !o.isData() && i <= last // the journal up to x's last change
		default:
			durable = o.isData() && o.ino == x
		}
		if durable {
			f.makeDurable(o)
		} else {
			keep = append(keep, o)
		}
	}
	f.pending = keep
	return nil
}

func (f *FS) makeDurable(o *op) {
	switch o.kind {
	case opWrite:
		o.ino.durable = writeAt(o.ino.durable, o.off, o.data)
	case opTruncate:
		o.ino.durable = truncate(o.ino.durable, o.size)
	case opCreate:
		o.dir.durableDents[o.name] = o.ino
	case opRemove:
		delete(o.dir.durableDents, o.name)
	case opRename:
		if o.dir.durableDents[o.name] == o.ino {
			delete(o.dir.durableDents, o.name)
		}
		o.dir2.durableDents[o.name2] = o.ino
	}
}

// describeLoss names what a power loss dropped, for the event log: per
// file, its lost writes and truncates and whether one tore; then the lost
// directory operations. Names are paths in the live tree (a file no longer
// linked anywhere shows as its inode). It draws nothing from the seed.
func (f *FS) describeLoss(lostOps []*op, tornOp *op) string {
	paths := map[*inode]string{f.root: "."}
	var walk func(d *inode, prefix string)
	walk = func(d *inode, prefix string) {
		for _, name := range slices.Sorted(maps.Keys(d.liveDents)) {
			c := d.liveDents[name]
			if _, ok := paths[c]; ok {
				continue
			}
			p := name
			if prefix != "" {
				p = prefix + "/" + name
			}
			paths[c] = p
			if c.dir {
				walk(c, p)
			}
		}
	}
	walk(f.root, "")
	path := func(x *inode) string {
		if p, ok := paths[x]; ok {
			return p
		}
		return fmt.Sprintf("(unlinked inode %d)", x.id)
	}
	in := func(d *inode, name string) string {
		if d == f.root {
			return name
		}
		return path(d) + "/" + name
	}
	type fileLoss struct {
		writes, truncates int
		torn              bool
	}
	byFile := map[*inode]*fileLoss{}
	var order []*inode
	var dirOps []string
	note := func(ino *inode) *fileLoss {
		if byFile[ino] == nil {
			byFile[ino] = &fileLoss{}
			order = append(order, ino)
		}
		return byFile[ino]
	}
	for _, o := range lostOps {
		switch o.kind {
		case opWrite:
			note(o.ino).writes++
		case opTruncate:
			note(o.ino).truncates++
		case opCreate:
			dirOps = append(dirOps, "create "+in(o.dir, o.name))
		case opRemove:
			dirOps = append(dirOps, "remove "+in(o.dir, o.name))
		case opRename:
			dirOps = append(dirOps, "rename "+in(o.dir, o.name)+" -> "+in(o.dir2, o.name2))
		}
	}
	if tornOp != nil {
		note(tornOp.ino).torn = true
	}
	var parts []string
	for _, ino := range order {
		l := byFile[ino]
		var what []string
		if l.writes > 0 {
			what = append(what, plural(l.writes, "write"))
		}
		if l.truncates > 0 {
			what = append(what, plural(l.truncates, "truncate"))
		}
		if l.torn {
			what = append(what, "one torn")
		}
		parts = append(parts, path(ino)+" ("+strings.Join(what, ", ")+")")
	}
	if len(dirOps) > 0 {
		parts = append(parts, "directory: "+strings.Join(dirOps, "; "))
	}
	return strings.Join(parts, "; ")
}

func plural(n int, what string) string {
	if n == 1 {
		return "1 " + what
	}
	return fmt.Sprintf("%d %ss", n, what)
}

// crash empties the page cache: a seeded, legal subset of the pending
// operations survives, and every open handle goes stale. After a kill
// (the process died, the host didn't) the page cache survives: handles go
// stale, and everything written stays visible and stays pending, so a
// later power loss may still lose it.
func (f *FS) crash() {
	f.gen++
	f.locks = nil // the process holding them is gone
	if f.m.Killed() {
		f.s.Logf("disk of %s kept its page cache (a kill): %d unsynced operations still pending", f.m.Name, len(f.pending))
		return
	}
	lost, torn, holed := 0, false, 0
	survive := func(o *op) { f.makeDurable(o) }
	var lostOps []*op // for the log: what was lost
	var tornOp *op
	tear := func(o *op) {
		if o.kind == opWrite && len(o.data) > 1 && f.rng.Float64() < f.opts.TornWriteProb {
			n := 1 + f.rng.IntN(len(o.data)-1)
			o.ino.durable = writeAt(o.ino.durable, o.off, o.data[:n])
			torn, tornOp = true, o
		}
	}
	if f.opts.Ordered {
		k := f.rng.IntN(len(f.pending) + 1)
		for _, o := range f.pending[:k] {
			survive(o)
		}
		if k < len(f.pending) {
			tear(f.pending[k])
		}
		lost = len(f.pending) - k
		lostOps = f.pending[k:]
	} else {
		// Directory journal: a prefix.
		var meta []*op
		data := map[*inode][]*op{}
		var files []*inode
		for _, o := range f.pending {
			if o.isData() {
				if data[o.ino] == nil {
					files = append(files, o.ino)
				}
				data[o.ino] = append(data[o.ino], o)
			} else {
				meta = append(meta, o)
			}
		}
		k := f.rng.IntN(len(meta) + 1)
		for _, o := range meta[:k] {
			survive(o)
		}
		lost += len(meta) - k
		lostOps = append(lostOps, meta[k:]...)
		// Each file's writes: an independent prefix, maybe torn; or, with
		// holes, page by page. The prefix and tear are drawn either way, so
		// holes change no other draw.
		for _, ino := range files {
			ops := data[ino]
			var base []byte // the durable content before this crash, for holes
			if f.opts.HoleProb > 0 {
				base = slices.Clone(ino.durable)
			}
			k := f.rng.IntN(len(ops) + 1)
			for _, o := range ops[:k] {
				survive(o)
			}
			if k < len(ops) {
				tear(ops[k])
			}
			if f.opts.HoleProb > 0 && !slices.ContainsFunc(ops, func(o *op) bool { return o.kind == opTruncate }) &&
				f.holes.Float64() < f.opts.HoleProb {
				// The content the pending writes would leave: not the live
				// content, which also holds writes a failed fsync dropped
				// ([fsync-error-forgets]).
				full := slices.Clone(base)
				for _, o := range ops {
					full = writeAt(full, o.off, o.data)
				}
				ino.durable = f.punch(base, full)
				holed++
				lost, lostOps = lost+len(ops), append(lostOps, ops...)
				continue
			}
			lost += len(ops) - k
			lostOps = append(lostOps, ops[k:]...)
		}
	}
	var lossLine string
	if f.s.Verbose() && (lost > 0 || torn) {
		lossLine = f.describeLoss(lostOps, tornOp) // names from the live tree, before it resets
	}
	f.pending = nil
	f.resetLive(f.root, map[*inode]bool{})
	f.s.Logf("disk of %s crashed: %d unsynced operations lost, torn=%v, files with holes=%d", f.m.Name, lost, torn, holed)
	if lossLine != "" {
		f.s.Logf("disk of %s lost: %s", f.m.Name, lossLine)
	}
	if f.OnCrashLoss != nil && (lost > 0 || torn) {
		f.OnCrashLoss(lost, torn)
	}
	if f.OnHoles != nil && holed > 0 {
		f.OnHoles()
	}
}

// punch is a file written back out of order: its size is the durable
// one or the written one, and each page holds the written content or the
// durable content (zeros past either's end), drawn independently from
// the holes stream.
func (f *FS) punch(durable, written []byte) []byte {
	ps := f.opts.PageSize
	if ps <= 0 {
		ps = 4096
	}
	size := len(durable)
	if f.holes.IntN(2) == 0 {
		size = len(written)
	}
	out := make([]byte, size)
	for off := 0; off < size; off += ps {
		src := durable
		if f.holes.IntN(2) == 0 {
			src = written
		}
		if off < len(src) {
			copy(out[off:min(off+ps, size)], src[off:min(off+ps, len(src))])
		}
	}
	return out
}

// resetLive makes the live tree the durable one.
func (f *FS) resetLive(d *inode, seen map[*inode]bool) {
	if seen[d] {
		return
	}
	seen[d] = true
	if d.dir {
		d.liveDents = maps.Clone(d.durableDents)
		for _, n := range slices.Sorted(maps.Keys(d.liveDents)) {
			f.resetLive(d.liveDents[n], seen)
		}
	} else {
		d.live = slices.Clone(d.durable)
	}
}

func writeAt(b []byte, off int64, p []byte) []byte {
	if end := off + int64(len(p)); end > int64(len(b)) {
		b = append(b, make([]byte, end-int64(len(b)))...)
	}
	copy(b[off:], p)
	return b
}

func truncate(b []byte, size int64) []byte {
	if size <= int64(len(b)) {
		return b[:size:size]
	}
	return append(b, make([]byte, size-int64(len(b)))...)
}

// ---- file handles ----

type file struct {
	fs       *FS
	ino      *inode
	name     string
	flag     int
	gen      int
	off      int64
	closed   bool
	dirPos   int
	dirOrder []fs.DirEntry
}

func (h *file) check(opName string) error {
	if h.closed {
		return pathErr(opName, h.name, fs.ErrClosed)
	}
	if h.gen != h.fs.gen {
		return pathErr(opName, h.name, ErrStale)
	}
	return nil
}

func (h *file) Name() string { return h.name }

func (h *file) Read(p []byte) (int, error) {
	h.fs.enter()
	if err := h.check("read"); err != nil {
		return 0, err
	}
	if h.ino.dir {
		return 0, pathErr("read", h.name, syscall.EISDIR)
	}
	if h.flag&(os.O_WRONLY) != 0 {
		return 0, pathErr("read", h.name, syscall.EBADF)
	}
	if len(p) > 0 && h.fs.ioError(h.fs.opts.ReadErrProb, "read") {
		return 0, pathErr("read", h.name, syscall.EIO)
	}
	if h.off >= int64(len(h.ino.live)) {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	n := copy(p, h.ino.live[h.off:])
	h.off += int64(n)
	return n, nil
}

func (h *file) ReadAt(p []byte, off int64) (int, error) {
	h.fs.enter()
	if err := h.check("read"); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, pathErr("readat", h.name, errors.New("negative offset"))
	}
	if len(p) > 0 && h.fs.ioError(h.fs.opts.ReadErrProb, "read") {
		return 0, pathErr("read", h.name, syscall.EIO)
	}
	if off >= int64(len(h.ino.live)) {
		return 0, io.EOF
	}
	n := copy(p, h.ino.live[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (h *file) Write(p []byte) (int, error) {
	off := h.off
	if h.flag&os.O_APPEND != 0 {
		off = int64(len(h.ino.live))
	}
	n, err := h.writeAt(p, off, "write")
	h.off = off + int64(n) // a short write moves the offset by what it wrote, as os does
	return n, err
}

func (h *file) WriteAt(p []byte, off int64) (int, error) {
	if h.flag&os.O_APPEND != 0 {
		return 0, errors.New("os: invalid use of WriteAt on file opened with O_APPEND")
	}
	return h.writeAt(p, off, "write")
}

func (h *file) writeAt(p []byte, off int64, opName string) (int, error) {
	h.fs.enter()
	if err := h.check(opName); err != nil {
		return 0, err
	}
	if h.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, pathErr(opName, h.name, syscall.EBADF)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if h.fs.ioError(h.fs.opts.WriteErrProb, "write") {
		return 0, pathErr(opName, h.name, syscall.EIO)
	}
	n := len(p)
	if room := h.fs.room(); room >= 0 {
		// Bytes past the end (and any hole before them) need space.
		if grow := off + int64(len(p)) - int64(len(h.ino.live)); grow > room {
			n = max(0, len(p)-int(grow-room))
		}
	}
	if n > 0 {
		data := slices.Clone(p[:n])
		h.ino.live = writeAt(h.ino.live, off, data)
		h.ino.modTime = h.fs.now()
		h.fs.issue(&op{kind: opWrite, ino: h.ino, off: off, data: data})
		h.fs.powerLoss()
	}
	if n < len(p) {
		h.fs.noSpace()
		return n, pathErr(opName, h.name, syscall.ENOSPC)
	}
	return n, nil
}

func (h *file) Seek(offset int64, whence int) (int64, error) {
	if err := h.check("seek"); err != nil {
		return 0, err
	}
	var base int64
	switch whence {
	case io.SeekCurrent:
		base = h.off
	case io.SeekEnd:
		base = int64(len(h.ino.live))
	}
	if base+offset < 0 {
		return 0, pathErr("seek", h.name, syscall.EINVAL)
	}
	h.off = base + offset
	return h.off, nil
}

func (h *file) Truncate(size int64) error {
	h.fs.enter()
	if err := h.check("truncate"); err != nil {
		return err
	}
	if h.flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		return pathErr("truncate", h.name, syscall.EINVAL)
	}
	if room := h.fs.room(); room >= 0 && size-int64(len(h.ino.live)) > room {
		h.fs.noSpace()
		return pathErr("truncate", h.name, syscall.ENOSPC)
	}
	h.ino.live = truncate(h.ino.live, size)
	h.ino.modTime = h.fs.now()
	h.fs.issue(&op{kind: opTruncate, ino: h.ino, size: size})
	h.fs.powerLoss()
	return nil
}

// Chmod is (*os.File).Chmod.
func (h *file) Chmod(mode fs.FileMode) error {
	h.fs.enter()
	if err := h.check("chmod"); err != nil {
		return err
	}
	chmod(h.ino, mode)
	return nil
}

func (h *file) Sync() error {
	h.fs.enter()
	if err := h.check("sync"); err != nil {
		return err
	}
	if err := h.fs.sync(h.ino); err != nil {
		return pathErr("sync", h.name, err)
	}
	return nil
}

func (h *file) Stat() (fs.FileInfo, error) {
	if err := h.check("stat"); err != nil {
		return nil, err
	}
	return info{name: path.Base(h.name), ino: h.ino}, nil
}

func (h *file) ReadDir(n int) ([]fs.DirEntry, error) {
	h.fs.enter()
	if err := h.check("readdirent"); err != nil {
		return nil, err
	}
	if !h.ino.dir {
		return nil, pathErr("readdirent", h.name, syscall.ENOTDIR)
	}
	if h.dirOrder == nil {
		// Directory order is unspecified; make it seeded-random so code
		// that relies on it fails in simulation (os.ReadDir sorts).
		h.dirOrder = dirEntries(h.ino)
		h.fs.rng.Shuffle(len(h.dirOrder), func(i, j int) {
			h.dirOrder[i], h.dirOrder[j] = h.dirOrder[j], h.dirOrder[i]
		})
	}
	all := h.dirOrder
	rest := all[min(h.dirPos, len(all)):]
	if n <= 0 {
		h.dirPos = len(all)
		return rest, nil
	}
	if len(rest) == 0 {
		return nil, io.EOF
	}
	rest = rest[:min(n, len(rest))]
	h.dirPos += len(rest)
	return rest, nil
}

func (h *file) Close() error {
	if h.closed {
		return pathErr("close", h.name, fs.ErrClosed)
	}
	h.closed = true
	return nil
}

// StopFaults turns off power loss inside I/O calls and injected errors,
// and lifts a full disk's cap (the runner's recovery phase). Crashes the
// runner still causes keep their usual effect.
func (f *FS) StopFaults() {
	f.opts.IOCrashProb, f.opts.SyncErrProb, f.opts.WriteErrProb, f.opts.ReadErrProb = 0, 0, 0, 0
	f.full = false
}

// LockFile is dst.LockFile on the simulated disk: an exclusive lock per
// file (inode), held until unlock, a crash or a kill.
func (f *FS) LockFile(name string) (func() error, error) {
	h, err := f.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	ino := h.(*file).ino
	h.Close()
	if f.locks == nil {
		f.locks = map[*inode]int{}
	}
	if _, held := f.locks[ino]; held {
		return nil, &fs.PathError{Op: "flock", Path: name, Err: dst.ErrLocked}
	}
	f.lockN++
	id, gen := f.lockN, f.gen
	f.locks[ino] = id
	return func() error {
		if gen == f.gen && f.locks[ino] == id {
			delete(f.locks, ino)
		}
		return nil
	}, nil
}
