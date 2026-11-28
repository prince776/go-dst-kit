package realworld_test

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

// errText is err's message, "" for nil, for comparing with os's.
func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// The production World (no Dir) hands every name to the os package as
// the program gave it: an absolute name, one relative to the working
// directory, and one climbing out of it all reach the same file os
// reaches, failures give os's own errors, and a file is the *os.File
// with the name it was opened by.
// Verifies: [fs-is-os]
// Fails if: an absolute or ".." name is rejected or lands elsewhere (the
// os.Root real World rejected both as escaping), an error differs from
// os's, or the file isn't os's.
func TestFSIsOS(t *testing.T) {
	top := t.TempDir()
	work := filepath.Join(top, "work")
	if err := os.Mkdir(work, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Chdir(work)
	if err := os.Mkdir("dot", 0o755); err != nil { // the kernel resolves dot before ".."
		t.Fatal(err)
	}
	fsys := realworld.New(realworld.Options{}).FS()
	for _, name := range []string{filepath.Join(top, "abs.txt"), "rel.txt", "../up.txt", "./dot/../rel2.txt"} {
		if err := dst.WriteFile(fsys, name, []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", name, err)
		}
		if b, err := os.ReadFile(name); err != nil || string(b) != name {
			t.Fatalf("os.ReadFile(%q) after the World's WriteFile: %q %v", name, b, err)
		}
		f, err := fsys.OpenFile(name, os.O_RDONLY, 0)
		if err != nil {
			t.Fatalf("OpenFile(%q): %v", name, err)
		}
		if _, ok := f.(*os.File); !ok || f.Name() != name {
			t.Errorf("OpenFile(%q) gave a %T named %q, want the *os.File named as opened", name, f, f.Name())
		}
		f.Close()
		fi, err := fsys.Stat(name)
		ofi, oerr := os.Stat(name)
		if err != nil || oerr != nil || fi.Size() != ofi.Size() || fi.Name() != ofi.Name() {
			t.Errorf("Stat(%q): %v %v, os: %v %v", name, fi, err, ofi, oerr)
		}
	}
	// Failures are os's own, word for word.
	missing := filepath.Join(top, "missing", "x")
	_, err := fsys.Stat(missing)
	_, oerr := os.Stat(missing)
	if errText(err) != errText(oerr) {
		t.Errorf("Stat of a missing name: %q, os: %q", errText(err), errText(oerr))
	}
	_, err = fsys.OpenFile("../nope/x", os.O_RDONLY, 0)
	_, oerr = os.OpenFile("../nope/x", os.O_RDONLY, 0)
	if errText(err) != errText(oerr) || !errors.Is(err, os.ErrNotExist) {
		t.Errorf("OpenFile of a missing name: %q, os: %q", errText(err), errText(oerr))
	}
	if err, oerr := fsys.Mkdir("rel.txt", 0o755), os.Mkdir("rel.txt", 0o755); errText(err) != errText(oerr) {
		t.Errorf("Mkdir over a file: %q, os: %q", errText(err), errText(oerr))
	}
	if err, oerr := fsys.Rename("nope", "x"), os.Rename("nope", "x"); errText(err) != errText(oerr) {
		t.Errorf("Rename of a missing name: %q, os: %q", errText(err), errText(oerr))
	}
	es, err := fsys.ReadDir("..")
	oes, oerr := os.ReadDir("..")
	if err != nil || oerr != nil || len(es) != len(oes) {
		t.Fatalf("ReadDir(..): %d %v, os: %d %v", len(es), err, len(oes), oerr)
	}
	for i := range es {
		if es[i].Name() != oes[i].Name() {
			t.Errorf("ReadDir(..)[%d] = %q, os: %q", i, es[i].Name(), oes[i].Name())
		}
	}
}

// The rest of os's and filepath's filesystem API on the production World
// is theirs: WalkDir, Walk and Glob don't follow a symbolic link to a
// directory (a walk over the FS's Stat, which follows it, would), DirFS
// is os.DirFS, MkdirTemp and CreateTemp default to os.TempDir, and
// Chmod, Chtimes, Truncate and the File methods act on the file os
// names, with os's errors.
// Verifies: [fs-is-os]
// Fails if: one of them is the generic algorithm instead of os's (a
// walk entering the linked directory), or differs from os's in result or
// error.
func TestFSRestIsOS(t *testing.T) {
	top := t.TempDir()
	fsys := realworld.New(realworld.Options{}).FS()
	for _, d := range []string{"tree/a", "elsewhere"} {
		if err := os.MkdirAll(filepath.Join(top, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{"tree/a/x.txt", "elsewhere/hidden.txt"} {
		if err := os.WriteFile(filepath.Join(top, f), []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(top, "elsewhere"), filepath.Join(top, "tree", "link")); err != nil {
		t.Skipf("no symbolic links here: %v", err)
	}
	tree := filepath.Join(top, "tree")
	collect := func(walk func(func(string))) []string {
		var got []string
		walk(func(p string) { got = append(got, p) })
		return got
	}
	got := collect(func(add func(string)) {
		dst.WalkDir(fsys, tree, func(p string, _ fs.DirEntry, _ error) error { add(p); return nil })
	})
	want := collect(func(add func(string)) {
		filepath.WalkDir(tree, func(p string, _ fs.DirEntry, _ error) error { add(p); return nil })
	})
	if !slices.Equal(got, want) || slices.Contains(got, filepath.Join(tree, "link", "hidden.txt")) {
		t.Errorf("WalkDir visited %q, filepath.WalkDir %q", got, want)
	}
	link := filepath.Join(tree, "link")
	got = collect(func(add func(string)) {
		dst.WalkDir(fsys, link, func(p string, _ fs.DirEntry, _ error) error { add(p); return nil })
	})
	if !slices.Equal(got, []string{link}) { // filepath.WalkDir lstats the root
		t.Errorf("WalkDir from a link to a directory visited %q, filepath.WalkDir only the link", got)
	}
	got = collect(func(add func(string)) {
		dst.Walk(fsys, tree, func(p string, _ fs.FileInfo, _ error) error { add(p); return nil })
	})
	want = collect(func(add func(string)) {
		filepath.Walk(tree, func(p string, _ fs.FileInfo, _ error) error { add(p); return nil })
	})
	if !slices.Equal(got, want) || slices.Contains(got, filepath.Join(tree, "link", "hidden.txt")) {
		t.Errorf("Walk visited %q, filepath.Walk %q", got, want)
	}
	pattern := filepath.Join(tree, "*", "*.txt")
	gm, gerr := dst.Glob(fsys, pattern)
	om, oerr := filepath.Glob(pattern)
	if !slices.Equal(gm, om) || gerr != oerr {
		t.Errorf("Glob: %q %v, filepath.Glob: %q %v", gm, gerr, om, oerr)
	}
	dangling := filepath.Join(top, "dangling")
	if err := os.Symlink(filepath.Join(top, "nowhere"), dangling); err != nil {
		t.Fatal(err)
	}
	if gm, _ := dst.Glob(fsys, dangling); !slices.Equal(gm, []string{dangling}) { // filepath lstats a literal pattern
		t.Errorf("Glob of a dangling link: %q, filepath.Glob finds it", gm)
	}
	if got, want := fmt.Sprintf("%T", dst.DirFS(fsys, tree)), fmt.Sprintf("%T", os.DirFS(tree)); got != want {
		t.Errorf("DirFS is a %s, os.DirFS a %s", got, want)
	}
	d, err := dst.MkdirTemp(fsys, "", "dstkit-*")
	if err != nil || filepath.Dir(d) != filepath.Clean(os.TempDir()) {
		t.Errorf("MkdirTemp(\"\") made %q (%v), want it in %q", d, err, os.TempDir())
	}
	os.RemoveAll(d)
	tf, err := dst.CreateTemp(fsys, top, "f*")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := tf.(*os.File); !ok {
		t.Errorf("CreateTemp gave a %T, want the *os.File", tf)
	}
	tf.Close()
	name := filepath.Join(top, "tree", "a", "x.txt")
	if err := dst.Chmod(fsys, name, 0o600); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(name); fi.Mode().Perm() != 0o600 {
		t.Errorf("after Chmod, os sees %v", fi.Mode())
	}
	if err := dst.Truncate(fsys, name, 2); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(name); string(b) != "tr" {
		t.Errorf("after Truncate, os reads %q", b)
	}
	mt := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	if err := dst.Chtimes(fsys, name, mt, mt); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(name); !fi.ModTime().Equal(mt) {
		t.Errorf("after Chtimes, os sees %v", fi.ModTime())
	}
	missing := filepath.Join(top, "missing")
	for _, c := range []struct {
		op       string
		dst, std error
	}{
		{"Chmod", dst.Chmod(fsys, missing, 0o600), os.Chmod(missing, 0o600)},
		{"Chtimes", dst.Chtimes(fsys, missing, mt, mt), os.Chtimes(missing, mt, mt)},
		{"Truncate", dst.Truncate(fsys, missing, 0), os.Truncate(missing, 0)},
	} {
		if errText(c.dst) != errText(c.std) {
			t.Errorf("%s of a missing name: %q, os: %q", c.op, errText(c.dst), errText(c.std))
		}
	}
	dir, err := dst.Open(fsys, tree)
	if err != nil {
		t.Fatal(err)
	}
	names, _ := dst.Readdirnames(dir, -1)
	dir.Close()
	slices.Sort(names)
	if !slices.Equal(names, []string{"a", "link"}) {
		t.Errorf("Readdirnames: %q", names)
	}
	dir, _ = dst.Open(fsys, tree)
	infos, _ := dst.Readdir(dir, -1)
	dir.Close()
	for _, fi := range infos {
		if fi.Name() == "link" && fi.Mode()&fs.ModeSymlink == 0 {
			t.Errorf("Readdir followed the link (mode %v): it is os.File's, which lstats", fi.Mode())
		}
	}
}

// A rooted real World (a Dir) and a simulated disk accept and reject
// the same names: inside the root both work, and absolute names and
// names that climb out are refused by both, as escaping.
// Verifies: [fs-is-os]
// Fails if: a name one accepts the other refuses (a rooted World that
// let ".." or an absolute name through, or refused one inside).
func TestRootedFSRejectsAsSimulated(t *testing.T) {
	names := []string{"x", "./x", "a/../x", "a/./y", "a/b/..", ".", "..", "../x", "a/../../x", "/x", "/"}
	try := func(fsys dst.FS) []string {
		if err := fsys.Mkdir("a", 0o755); err != nil {
			panic(err)
		}
		var out []string
		for _, n := range names {
			_, err := fsys.Stat(n)
			if err == nil {
				f, cerr := dst.Create(fsys, strings.TrimSuffix(n, "/")+"-file")
				if cerr == nil {
					f.Close()
				}
			}
			switch {
			case err == nil:
				out = append(out, n+": ok")
			case strings.Contains(err.Error(), "escapes"):
				out = append(out, n+": escapes")
			case errors.Is(err, os.ErrNotExist):
				out = append(out, n+": not exist")
			default:
				out = append(out, n+": "+errText(err))
			}
		}
		return out
	}
	root := t.TempDir()
	rw := realworld.New(realworld.Options{Dir: root})
	if err := dst.WriteFile(rw.FS(), "x", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	real := try(rw.FS())
	var sim []string
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		if err := dst.WriteFile(w.FS(), "x", nil, 0o644); err != nil {
			panic(err)
		}
		sim = try(w.FS())
	})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	escapes := 0
	for i := range names {
		if real[i] != sim[i] {
			t.Errorf("rooted real World %q, simulated %q", real[i], sim[i])
		}
		if strings.HasSuffix(real[i], "escapes") {
			escapes++
		}
	}
	if escapes < 4 {
		t.Errorf("only %d names refused as escaping: %v", escapes, real)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "x")); err == nil {
		t.Error("a rooted World wrote outside its root")
	}
}
