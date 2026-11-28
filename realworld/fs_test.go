package realworld_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
