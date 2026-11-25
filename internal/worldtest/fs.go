package worldtest

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"slices"

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
