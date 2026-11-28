package realworld

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// The rest of os's filesystem API (dst.Chmoder, Chtimeser, Truncater,
// TempMaker, and the methods dst.WalkDir, Walk, Glob and DirFS look
// for): os's and filepath's own functions, on the names as given when
// unrooted; under the root, as the other operations, when rooted.

// Chmod is os.Chmod.
func (f *osFS) Chmod(name string, mode fs.FileMode) error {
	p, err := f.path("chmod", name)
	if err != nil {
		return err
	}
	return os.Chmod(p, mode)
}

// Chtimes is os.Chtimes.
func (f *osFS) Chtimes(name string, atime, mtime time.Time) error {
	p, err := f.path("chtimes", name)
	if err != nil {
		return err
	}
	return os.Chtimes(p, atime, mtime)
}

// Truncate is os.Truncate.
func (f *osFS) Truncate(name string, size int64) error {
	p, err := f.path("truncate", name)
	if err != nil {
		return err
	}
	return os.Truncate(p, size)
}

// tempDir is the directory MkdirTemp and CreateTemp use: dir itself
// unrooted (os.TempDir for ""); under the root rooted, where "" is
// "tmp", made if missing, as on a simulated disk.
func (f *osFS) tempDir(op, dir string) (string, error) {
	if f.dir == "" {
		return dir, nil
	}
	if dir == "" {
		dir = "tmp"
		if err := f.MkdirAll(dir, 0o777); err != nil {
			return "", err
		}
	}
	return f.path(op, dir)
}

// MkdirTemp is os.MkdirTemp. Rooted, it returns the name under the root.
func (f *osFS) MkdirTemp(dir, pattern string) (string, error) {
	d, err := f.tempDir("mkdirtemp", dir)
	if err != nil {
		return "", err
	}
	name, err := os.MkdirTemp(d, pattern)
	if err != nil || f.dir == "" {
		return name, err
	}
	return f.unroot(dir, d, name), nil
}

// CreateTemp is os.CreateTemp. Rooted, the file's Name is its name under
// the root.
func (f *osFS) CreateTemp(dir, pattern string) (dst.File, error) {
	d, err := f.tempDir("createtemp", dir)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(d, pattern)
	if err != nil {
		return nil, err // not a typed-nil *os.File
	}
	if f.dir == "" {
		return file, nil
	}
	return &rootedFile{file, f.unroot(dir, d, file.Name())}, nil
}

// unroot turns p, a path under the root's directory d (which the
// program named dir), back into the program's name for it.
func (f *osFS) unroot(dir, d, p string) string {
	if dir == "" {
		dir = "tmp"
	}
	return filepath.Join(dir, strings.TrimPrefix(strings.TrimPrefix(p, d), string(filepath.Separator)))
}

// WalkDir is filepath.WalkDir. Rooted, fn sees the names under the root.
func (f *osFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	if f.dir == "" {
		return filepath.WalkDir(root, fn)
	}
	p, err := f.path("lstat", root)
	if err != nil {
		return fn(root, nil, err)
	}
	return filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
		return fn(f.rename(root, p, path), d, err)
	})
}

// Walk is filepath.Walk. Rooted, fn sees the names under the root.
func (f *osFS) Walk(root string, fn filepath.WalkFunc) error {
	if f.dir == "" {
		return filepath.Walk(root, fn)
	}
	p, err := f.path("lstat", root)
	if err != nil {
		return fn(root, nil, err)
	}
	return filepath.Walk(p, func(path string, info fs.FileInfo, err error) error {
		return fn(f.rename(root, p, path), info, err)
	})
}

// rename turns path, a path filepath produced under p (the root's path
// for the program's root), into the program's name: root joined with
// the rest, as filepath joins them.
func (f *osFS) rename(root, p, path string) string {
	if path == p {
		return root
	}
	return filepath.Join(root, path[len(p)+1:])
}

// Glob is filepath.Glob. Rooted, the pattern is matched under the root
// (absolute and escaping patterns match nothing, as on a simulated disk)
// and the matches are names under it.
func (f *osFS) Glob(pattern string) ([]string, error) {
	if f.dir == "" {
		return filepath.Glob(pattern)
	}
	if _, err := filepath.Match(pattern, ""); err != nil {
		return nil, err
	}
	if _, err := f.path("glob", pattern); err != nil {
		return nil, nil
	}
	root := f.dir
	if runtime.GOOS != "windows" {
		root = escapeMeta(root)
	}
	ms, err := filepath.Glob(filepath.Join(root, pattern))
	for i, m := range ms {
		ms[i] = m[len(f.dir)+1:]
	}
	return ms, err
}

// escapeMeta quotes the pattern characters in a literal path.
func escapeMeta(p string) string {
	var b strings.Builder
	for _, r := range p {
		if strings.ContainsRune(`*?[\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// DirFS is os.DirFS. Rooted, dir is under the root.
func (f *osFS) DirFS(dir string) fs.FS {
	if f.dir == "" {
		return os.DirFS(dir)
	}
	p, err := f.path("open", dir)
	if err != nil {
		return errFS{err}
	}
	return os.DirFS(p)
}

// errFS is an fs.FS whose every Open fails with err.
type errFS struct{ err error }

func (e errFS) Open(string) (fs.File, error) { return nil, e.err }

var (
	_ dst.Chmoder   = (*osFS)(nil)
	_ dst.Chtimeser = (*osFS)(nil)
	_ dst.Truncater = (*osFS)(nil)
	_ dst.TempMaker = (*osFS)(nil)
)
