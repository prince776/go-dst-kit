package worldtest

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing/fstest"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// FSChecks run against w.FS() of a fresh World with an empty filesystem.
var FSChecks = []Check{
	{"fs/create-write-read", fsCreateWriteRead},
	{"fs/open-flags", fsOpenFlags},
	{"fs/append", fsAppend},
	{"fs/seek-readat-writeat", fsSeekAt},
	{"fs/truncate", fsTruncate},
	{"fs/dirs", fsDirs},
	{"fs/rename", fsRename},
	{"fs/remove", fsRemove},
	{"fs/stat", fsStat},
	{"fs/closed-handle", fsClosed},
	{"fs/unlinked-open-file", fsUnlinked},
	{"fs/no-escape", fsNoEscape},
	{"fs/sync", fsSync},
	{"fs/name-as-opened", fsName},
	{"fs/lock", fsLock},
	{"fs/link", fsLink},
	{"fs/chmod-chtimes", fsChmodChtimes},
	{"fs/truncate-by-name", fsTruncateName},
	{"fs/temp", fsTemp},
	{"fs/walk-glob-dirfs", fsWalkGlob},
}

func must(err error) {
	if err != nil {
		failf("unexpected error: %v", err)
	}
}

func read(fsys dst.FS, name string) string {
	b, err := dst.ReadFile(fsys, name)
	must(err)
	return string(b)
}

func fsCreateWriteRead(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("hello"), 0o644))
	if got := read(fsys, "f"); got != "hello" {
		failf("read %q", got)
	}
	must(dst.WriteFile(fsys, "f", []byte("hi"), 0o644))
	if got := read(fsys, "f"); got != "hi" {
		failf("WriteFile didn't truncate: %q", got)
	}
}

func fsOpenFlags(w *dst.World) {
	fsys := w.FS()
	if _, err := dst.Open(fsys, "missing"); !errors.Is(err, fs.ErrNotExist) {
		failf("open missing: %v", err)
	}
	must(dst.WriteFile(fsys, "f", []byte("x"), 0o644))
	if _, err := fsys.OpenFile("f", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, fs.ErrExist) {
		failf("O_EXCL on existing: %v", err)
	}
	ro, err := dst.Open(fsys, "f")
	must(err)
	if _, err := ro.Write([]byte("y")); err == nil {
		failf("write to a read-only handle succeeded")
	}
	ro.Close()
	must(fsys.Mkdir("d", 0o755))
	if _, err := fsys.OpenFile("d", os.O_WRONLY, 0); err == nil {
		failf("opening a directory for writing succeeded")
	}
}

func fsAppend(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("ab"), 0o644))
	f, err := fsys.OpenFile("f", os.O_WRONLY|os.O_APPEND, 0)
	must(err)
	_, err = f.Write([]byte("cd"))
	must(err)
	f.Close()
	if got := read(fsys, "f"); got != "abcd" {
		failf("after append: %q", got)
	}
}

func fsSeekAt(w *dst.World) {
	fsys := w.FS()
	f, err := dst.Create(fsys, "f")
	must(err)
	defer f.Close()
	_, err = f.WriteAt([]byte("xy"), 4)
	must(err)
	b := make([]byte, 6)
	n, err := f.ReadAt(b, 0)
	if n != 6 || (err != nil && err != io.EOF) || string(b) != "\x00\x00\x00\x00xy" {
		failf("sparse ReadAt: %d %v %q", n, err, b)
	}
	if _, err := f.ReadAt(b, 3); err != io.EOF {
		failf("ReadAt past the end: %v", err)
	}
	off, err := f.Seek(-2, io.SeekEnd)
	must(err)
	if off != 4 {
		failf("Seek to %d", off)
	}
	n, _ = f.Read(b)
	if string(b[:n]) != "xy" {
		failf("read after seek %q", b[:n])
	}
	if _, err := f.Read(b); err != io.EOF {
		failf("read at end: %v", err)
	}
}

func fsTruncate(w *dst.World) {
	fsys := w.FS()
	f, err := dst.Create(fsys, "f")
	must(err)
	f.Write([]byte("hello"))
	must(f.Truncate(2))
	must(f.Truncate(4))
	f.Close()
	if got := read(fsys, "f"); got != "he\x00\x00" {
		failf("after truncates %q", got)
	}
}

func names(es []fs.DirEntry) []string {
	var out []string
	for _, e := range es {
		n := e.Name()
		if e.IsDir() {
			n += "/"
		}
		out = append(out, n)
	}
	return out
}

func fsDirs(w *dst.World) {
	fsys := w.FS()
	must(fsys.MkdirAll("a/b/c", 0o755))
	must(fsys.MkdirAll("a/b", 0o755))
	must(dst.WriteFile(fsys, "a/z", nil, 0o644))
	must(dst.WriteFile(fsys, "a/b/y", nil, 0o644))
	es, err := fsys.ReadDir("a")
	must(err)
	if got := names(es); !slices.Equal(got, []string{"b/", "z"}) {
		failf("ReadDir(a) = %v", got)
	}
	if err := fsys.Mkdir("a", 0o755); !errors.Is(err, fs.ErrExist) {
		failf("Mkdir existing: %v", err)
	}
	if err := fsys.Mkdir("nope/x", 0o755); !errors.Is(err, fs.ErrNotExist) {
		failf("Mkdir without parent: %v", err)
	}
	d, err := dst.Open(fsys, "a")
	must(err)
	first, err := d.ReadDir(1)
	must(err)
	rest, err := d.ReadDir(-1)
	must(err)
	d.Close()
	// File.ReadDir returns entries in directory order, which is unspecified.
	all := append(names(first), names(rest)...)
	slices.Sort(all)
	if len(first) != 1 || !slices.Equal(all, []string{"b/", "z"}) {
		failf("ReadDir(1), ReadDir(-1) = %v %v", names(first), names(rest))
	}
}

func fsRename(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "old", []byte("new contents"), 0o644))
	must(dst.WriteFile(fsys, "target", []byte("old contents"), 0o644))
	must(fsys.Rename("old", "target"))
	if got := read(fsys, "target"); got != "new contents" {
		failf("after rename over: %q", got)
	}
	if _, err := fsys.Stat("old"); !errors.Is(err, fs.ErrNotExist) {
		failf("old name still exists: %v", err)
	}
	must(fsys.Mkdir("d", 0o755))
	must(fsys.Rename("target", "d/moved"))
	if got := read(fsys, "d/moved"); got != "new contents" {
		failf("after rename into a dir: %q", got)
	}
	if err := fsys.Rename("missing", "x"); !errors.Is(err, fs.ErrNotExist) {
		failf("rename missing: %v", err)
	}
}

func fsRemove(w *dst.World) {
	fsys := w.FS()
	must(fsys.MkdirAll("d/e", 0o755))
	must(dst.WriteFile(fsys, "d/e/f", nil, 0o644))
	if err := fsys.Remove("d"); err == nil {
		failf("removed a non-empty directory")
	}
	if err := fsys.Remove("missing"); !errors.Is(err, fs.ErrNotExist) {
		failf("remove missing: %v", err)
	}
	must(fsys.RemoveAll("d"))
	must(fsys.RemoveAll("d")) // missing is fine
	if _, err := fsys.Stat("d"); !errors.Is(err, fs.ErrNotExist) {
		failf("d survived RemoveAll: %v", err)
	}
}

func fsStat(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("12345"), 0o644))
	must(fsys.Mkdir("d", 0o755))
	fi, err := fsys.Stat("f")
	must(err)
	if fi.Size() != 5 || fi.IsDir() || fi.Name() != "f" {
		failf("stat f: %d %v %q", fi.Size(), fi.IsDir(), fi.Name())
	}
	fi, err = fsys.Stat("d")
	must(err)
	if !fi.IsDir() {
		failf("stat d: not a dir")
	}
	f, err := dst.Open(fsys, "f")
	must(err)
	defer f.Close()
	fi, err = f.Stat()
	must(err)
	if fi.Size() != 5 {
		failf("File.Stat size %d", fi.Size())
	}
}

func fsClosed(w *dst.World) {
	fsys := w.FS()
	f, err := dst.Create(fsys, "f")
	must(err)
	must(f.Close())
	if _, err := f.Write([]byte("x")); !errors.Is(err, fs.ErrClosed) {
		failf("write after close: %v", err)
	}
	if err := f.Close(); err == nil {
		failf("second close succeeded")
	}
}

func fsUnlinked(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("still here"), 0o644))
	f, err := dst.Open(fsys, "f")
	must(err)
	defer f.Close()
	must(fsys.Remove("f"))
	b, err := io.ReadAll(f)
	must(err)
	if string(b) != "still here" {
		failf("unlinked open file reads %q", b)
	}
}

func fsNoEscape(w *dst.World) {
	fsys := w.FS()
	if _, err := dst.Create(fsys, "../escape"); err == nil {
		failf("created a file outside the root")
	}
}

func fsSync(w *dst.World) {
	fsys := w.FS()
	must(fsys.Mkdir("d", 0o755))
	f, err := dst.Create(fsys, "d/f")
	must(err)
	f.Write([]byte("x"))
	must(f.Sync())
	f.Close()
	must(dst.SyncDir(fsys, "d"))
	must(dst.SyncDir(fsys, "."))
}

// fsName: a file's Name is the name it was opened with, as os.File's is,
// so code can pass it back to the same FS (Remove(f.Name())).
func fsName(w *dst.World) {
	fsys := w.FS()
	must(fsys.MkdirAll("d/e", 0o755))
	f, err := fsys.OpenFile("d/e/f", os.O_CREATE|os.O_RDWR, 0o644)
	must(err)
	if f.Name() != "d/e/f" {
		failf("Name() = %q, want the name as opened, %q", f.Name(), "d/e/f")
	}
	must(f.Close())
	must(fsys.Remove(f.Name()))
}

// fsLock: an advisory lock is exclusive, also within one process, until
// it is released ([lock-exclusive]).
func fsLock(w *dst.World) {
	fsys := w.FS()
	unlock, err := dst.LockFile(fsys, "LOCK")
	must(err)
	if _, err := fsys.Stat("LOCK"); err != nil {
		failf("LockFile didn't create its file: %v", err)
	}
	if _, err := dst.LockFile(fsys, "LOCK"); !errors.Is(err, dst.ErrLocked) {
		failf("a second LockFile while held: %v, want ErrLocked", err)
	}
	unlock2, err := dst.LockFile(fsys, "OTHER")
	must(err) // another file's lock is independent
	must(unlock2())
	must(unlock())
	unlock, err = dst.LockFile(fsys, "LOCK")
	if err != nil {
		failf("LockFile after unlock: %v", err)
	}
	must(unlock())
}

// fsLink: a hard link is a second name for the same data: writes through
// one read through the other, removing one keeps the data, and linking
// onto an existing name or a directory fails ([link-shares-data]).
func fsLink(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "a", []byte("one"), 0o644))
	must(fsys.Mkdir("d", 0o755))
	must(dst.Link(fsys, "a", "d/b"))
	f, err := fsys.OpenFile("d/b", os.O_WRONLY|os.O_APPEND, 0)
	must(err)
	_, err = f.Write([]byte("two"))
	must(err)
	must(f.Close())
	if b, err := dst.ReadFile(fsys, "a"); err != nil || string(b) != "onetwo" {
		failf("after writing through the link, a reads %q (%v), want %q", b, err, "onetwo")
	}
	if err := dst.Link(fsys, "a", "d/b"); !errors.Is(err, fs.ErrExist) {
		failf("Link onto an existing name: %v, want ErrExist", err)
	}
	if err := dst.Link(fsys, "d", "e"); err == nil {
		failf("Link of a directory succeeded")
	}
	if err := dst.Link(fsys, "missing", "c"); !errors.Is(err, fs.ErrNotExist) {
		failf("Link of a missing file: %v, want ErrNotExist", err)
	}
	must(fsys.Remove("a"))
	if b, err := dst.ReadFile(fsys, "d/b"); err != nil || string(b) != "onetwo" {
		failf("after removing a, its link reads %q (%v), want %q", b, err, "onetwo")
	}
	if _, err := fsys.Stat("a"); !errors.Is(err, fs.ErrNotExist) {
		failf("a still exists after Remove: %v", err)
	}
}

// fsChmodChtimes: Chmod sets a file's permission bits (by name and on an
// open file), Chtimes its modification time (a zero time keeps it), and
// both fail as os's do on a missing name or a closed file.
func fsChmodChtimes(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("x"), 0o644))
	must(dst.Chmod(fsys, "f", 0o600))
	if fi, err := fsys.Stat("f"); err != nil || fi.Mode().Perm() != 0o600 {
		failf("mode after Chmod 0600: %v %v", fi.Mode(), err)
	}
	if err := dst.Chmod(fsys, "missing", 0o600); !errors.Is(err, fs.ErrNotExist) {
		failf("Chmod of a missing file: %v", err)
	}
	f, err := dst.Open(fsys, "f")
	must(err)
	must(dst.FileChmod(f, 0o640))
	if fi, _ := fsys.Stat("f"); fi.Mode().Perm() != 0o640 {
		failf("mode after File.Chmod 0640: %v", fi.Mode())
	}
	f.Close()
	if err := dst.FileChmod(f, 0o600); !errors.Is(err, fs.ErrClosed) {
		failf("Chmod of a closed file: %v", err)
	}
	mt := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	must(dst.Chtimes(fsys, "f", mt, mt))
	if fi, _ := fsys.Stat("f"); !fi.ModTime().Equal(mt) {
		failf("ModTime after Chtimes: %v, want %v", fi.ModTime(), mt)
	}
	must(dst.Chtimes(fsys, "f", time.Time{}, time.Time{}))
	if fi, _ := fsys.Stat("f"); !fi.ModTime().Equal(mt) {
		failf("a zero Chtimes moved ModTime to %v", fi.ModTime())
	}
	if err := dst.Chtimes(fsys, "missing", mt, mt); !errors.Is(err, fs.ErrNotExist) {
		failf("Chtimes of a missing file: %v", err)
	}
}

// fsTruncateName: Truncate by name shortens and zero-extends a file, and
// fails on a directory and a missing name.
func fsTruncateName(w *dst.World) {
	fsys := w.FS()
	must(dst.WriteFile(fsys, "f", []byte("hello world"), 0o644))
	must(dst.Truncate(fsys, "f", 5))
	if got := read(fsys, "f"); got != "hello" {
		failf("after Truncate 5: %q", got)
	}
	must(dst.Truncate(fsys, "f", 7))
	if got := read(fsys, "f"); got != "hello\x00\x00" {
		failf("after Truncate 7: %q", got)
	}
	must(fsys.Mkdir("d", 0o755))
	if err := dst.Truncate(fsys, "d", 0); err == nil {
		failf("Truncate of a directory succeeded")
	}
	if err := dst.Truncate(fsys, "missing", 0); !errors.Is(err, fs.ErrNotExist) {
		failf("Truncate of a missing file: %v", err)
	}
}

// fsTemp: MkdirTemp and CreateTemp make distinct new names around the
// pattern's last "*", in dir (a default one for ""), the file opened
// for reading and writing; a pattern with a separator, or a missing
// dir, fails.
func fsTemp(w *dst.World) {
	fsys := w.FS()
	d1, err := dst.MkdirTemp(fsys, "", "x-*-y")
	must(err)
	d2, err := dst.MkdirTemp(fsys, "", "x-*-y")
	must(err)
	b := path.Base(filepath.ToSlash(d1))
	if d1 == d2 || !strings.HasPrefix(b, "x-") || !strings.HasSuffix(b, "-y") || len(b) <= len("x--y") {
		failf("MkdirTemp names %q and %q for x-*-y", d1, d2)
	}
	if fi, err := fsys.Stat(d1); err != nil || !fi.IsDir() {
		failf("MkdirTemp's %q: %v %v", d1, fi, err)
	}
	f, err := dst.CreateTemp(fsys, d1, "f")
	must(err)
	if !strings.HasPrefix(f.Name(), filepath.Join(d1, "f")) {
		failf("CreateTemp in %q named %q", d1, f.Name())
	}
	_, err = f.Write([]byte("data"))
	must(err)
	f.Close()
	if got := read(fsys, f.Name()); got != "data" {
		failf("the temporary file holds %q", got)
	}
	if _, err := dst.CreateTemp(fsys, d1, "a/b*"); err == nil {
		failf("CreateTemp with a separator in the pattern succeeded")
	}
	if _, err := dst.MkdirTemp(fsys, "missing", "x"); !errors.Is(err, fs.ErrNotExist) {
		failf("MkdirTemp in a missing dir: %v", err)
	}
}

// fsWalkGlob: WalkDir and Walk visit a tree in lexical order (SkipDir
// prunes), Glob matches as filepath's does, Readdirnames and Readdir list
// a directory, and DirFS is a valid fs.FS of a subtree (testing/fstest).
func fsWalkGlob(w *dst.World) {
	fsys := w.FS()
	must(fsys.MkdirAll("t/a/b", 0o755))
	for _, n := range []string{"t/a/b/c.txt", "t/a/d.txt", "t/e.txt", "t/a/b/z.md"} {
		must(dst.WriteFile(fsys, n, []byte(n), 0o644))
	}
	var walked, walked2 []string
	must(dst.WalkDir(fsys, "t", func(p string, d fs.DirEntry, err error) error {
		must(err)
		walked = append(walked, filepath.ToSlash(p))
		if d.Name() == "b" {
			return filepath.SkipDir
		}
		return nil
	}))
	if want := []string{"t", "t/a", "t/a/b", "t/a/d.txt", "t/e.txt"}; !slices.Equal(walked, want) {
		failf("WalkDir visited %q, want %q", walked, want)
	}
	must(dst.Walk(fsys, "t", func(p string, info fs.FileInfo, err error) error {
		must(err)
		walked2 = append(walked2, filepath.ToSlash(p))
		return nil
	}))
	if want := []string{"t", "t/a", "t/a/b", "t/a/b/c.txt", "t/a/b/z.md", "t/a/d.txt", "t/e.txt"}; !slices.Equal(walked2, want) {
		failf("Walk visited %q, want %q", walked2, want)
	}
	var missing error
	dst.WalkDir(fsys, "nope", func(_ string, _ fs.DirEntry, err error) error { missing = err; return nil })
	if !errors.Is(missing, fs.ErrNotExist) {
		failf("WalkDir of a missing root reported %v", missing)
	}
	for pattern, want := range map[string][]string{
		"t/a/*.txt": {"t/a/d.txt"},
		"t/*/*":     {"t/a/b", "t/a/d.txt"},
		"t/a/b/*.?": nil,
		"t/a/b/*":   {"t/a/b/c.txt", "t/a/b/z.md"},
		"t/e.txt":   {"t/e.txt"},
		"t/none":    nil,
	} {
		got, err := dst.Glob(fsys, pattern)
		for i := range got {
			got[i] = filepath.ToSlash(got[i])
		}
		if err != nil || !slices.Equal(got, want) {
			failf("Glob(%q) = %q %v, want %q", pattern, got, err, want)
		}
	}
	if _, err := dst.Glob(fsys, "t/["); err != filepath.ErrBadPattern {
		failf("Glob of a bad pattern: %v", err)
	}
	d, err := dst.Open(fsys, "t/a")
	must(err)
	names, err := dst.Readdirnames(d, -1)
	must(err)
	d.Close()
	slices.Sort(names)
	if !slices.Equal(names, []string{"b", "d.txt"}) {
		failf("Readdirnames: %q", names)
	}
	d, err = dst.Open(fsys, "t/a")
	must(err)
	infos, err := dst.Readdir(d, -1)
	must(err)
	d.Close()
	var inames []string
	for _, fi := range infos {
		inames = append(inames, fi.Name())
		if fi.Name() == "d.txt" && fi.Size() != int64(len("t/a/d.txt")) {
			failf("Readdir's d.txt has size %d", fi.Size())
		}
	}
	slices.Sort(inames)
	if !slices.Equal(inames, []string{"b", "d.txt"}) {
		failf("Readdir: %q", inames)
	}
	sub := dst.DirFS(fsys, "t/a")
	if err := fstest.TestFS(sub, "d.txt", "b/c.txt", "b/z.md"); err != nil {
		failf("DirFS: %v", err)
	}
	if _, err := sub.Open("../e.txt"); !errors.Is(err, fs.ErrInvalid) {
		failf("DirFS opened a name outside its tree: %v", err)
	}
	if b, err := fs.ReadFile(sub, "b/c.txt"); err != nil || string(b) != "t/a/b/c.txt" {
		failf("DirFS ReadFile: %q %v", b, err)
	}
}
