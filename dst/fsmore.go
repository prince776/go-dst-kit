package dst

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

// The rest of package os's (and path/filepath's) filesystem API, over a
// World's FS. Each is os's (or filepath's) own function when fsys is the
// real World's production filesystem, so names, errors and symbolic
// links behave exactly as there; on a simulated disk it is that disk's
// operation or os's algorithm over the FS.

// Chmoder is a filesystem whose files' modes can change. The real
// World's filesystem and the simulated disk are Chmoders.
type Chmoder interface {
	Chmod(name string, mode fs.FileMode) error
}

// Chtimeser is a filesystem whose files' times can be set. The real
// World's filesystem and the simulated disk are Chtimesers.
type Chtimeser interface {
	Chtimes(name string, atime, mtime time.Time) error
}

// Truncater is a filesystem that truncates a file by name. The real
// World's filesystem and the simulated disk are Truncaters.
type Truncater interface {
	Truncate(name string, size int64) error
}

// TempMaker is a filesystem that makes uniquely named temporary files and
// directories. The real World's filesystem and the simulated disk are
// TempMakers. A simulated disk names them from a counter of its own
// (unique, and the same for the same seed) and, for dir "", puts them in
// the directory "tmp", made if missing.
type TempMaker interface {
	MkdirTemp(dir, pattern string) (string, error)
	CreateTemp(dir, pattern string) (File, error)
}

// Chmod is os.Chmod on fsys. A filesystem that isn't a Chmoder returns
// errors.ErrUnsupported.
func Chmod(fsys FS, name string, mode fs.FileMode) error {
	if c, ok := fsys.(Chmoder); ok {
		return c.Chmod(name, mode)
	}
	return &fs.PathError{Op: "chmod", Path: name, Err: errors.ErrUnsupported}
}

// Chtimes is os.Chtimes on fsys. A filesystem that isn't a Chtimeser
// returns errors.ErrUnsupported.
func Chtimes(fsys FS, name string, atime, mtime time.Time) error {
	if c, ok := fsys.(Chtimeser); ok {
		return c.Chtimes(name, atime, mtime)
	}
	return &fs.PathError{Op: "chtimes", Path: name, Err: errors.ErrUnsupported}
}

// Truncate is os.Truncate on fsys. A filesystem that isn't a Truncater
// returns errors.ErrUnsupported.
func Truncate(fsys FS, name string, size int64) error {
	if t, ok := fsys.(Truncater); ok {
		return t.Truncate(name, size)
	}
	return &fs.PathError{Op: "truncate", Path: name, Err: errors.ErrUnsupported}
}

// MkdirTemp is os.MkdirTemp on fsys. A filesystem that isn't a TempMaker
// returns errors.ErrUnsupported.
func MkdirTemp(fsys FS, dir, pattern string) (string, error) {
	if t, ok := fsys.(TempMaker); ok {
		return t.MkdirTemp(dir, pattern)
	}
	return "", &fs.PathError{Op: "mkdirtemp", Path: pattern, Err: errors.ErrUnsupported}
}

// CreateTemp is os.CreateTemp on fsys. A filesystem that isn't a
// TempMaker returns errors.ErrUnsupported.
func CreateTemp(fsys FS, dir, pattern string) (File, error) {
	if t, ok := fsys.(TempMaker); ok {
		return t.CreateTemp(dir, pattern)
	}
	return nil, &fs.PathError{Op: "createtemp", Path: pattern, Err: errors.ErrUnsupported}
}

// FileChmod is (*os.File).Chmod on f. A File without a Chmod method
// returns errors.ErrUnsupported.
func FileChmod(f File, mode fs.FileMode) error {
	if c, ok := f.(interface{ Chmod(fs.FileMode) error }); ok {
		return c.Chmod(mode)
	}
	return &fs.PathError{Op: "chmod", Path: f.Name(), Err: errors.ErrUnsupported}
}

// Readdirnames is (*os.File).Readdirnames on f: f's own when it has one,
// else the names of f.ReadDir(n).
func Readdirnames(f File, n int) ([]string, error) {
	if r, ok := f.(interface{ Readdirnames(int) ([]string, error) }); ok {
		return r.Readdirnames(n)
	}
	ents, err := f.ReadDir(n)
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	return names, err
}

// Readdir is (*os.File).Readdir on f: f's own when it has one, else the
// FileInfos of f.ReadDir(n) (an entry removed in between is skipped, as
// os does).
func Readdir(f File, n int) ([]fs.FileInfo, error) {
	if r, ok := f.(interface {
		Readdir(int) ([]fs.FileInfo, error)
	}); ok {
		return r.Readdir(n)
	}
	ents, err := f.ReadDir(n)
	infos := make([]fs.FileInfo, 0, len(ents))
	for _, e := range ents {
		info, ierr := e.Info()
		if errors.Is(ierr, fs.ErrNotExist) {
			continue
		}
		if ierr != nil && err == nil {
			err = ierr
		}
		if info != nil {
			infos = append(infos, info)
		}
	}
	return infos, err
}

// WalkDir is filepath.WalkDir on fsys: filepath's own on a filesystem
// with a WalkDir method (the real World's), else its algorithm over
// fsys's Stat and ReadDir.
func WalkDir(fsys FS, root string, fn fs.WalkDirFunc) error {
	if w, ok := fsys.(interface {
		WalkDir(string, fs.WalkDirFunc) error
	}); ok {
		return w.WalkDir(root, fn)
	}
	info, err := fsys.Stat(root)
	if err != nil {
		err = fn(root, nil, err)
	} else {
		err = walkDir(fsys, root, fs.FileInfoToDirEntry(info), fn)
	}
	if err == filepath.SkipDir || err == filepath.SkipAll {
		return nil
	}
	return err
}

func walkDir(fsys FS, path string, d fs.DirEntry, fn fs.WalkDirFunc) error {
	if err := fn(path, d, nil); err != nil || !d.IsDir() {
		if err == filepath.SkipDir && d.IsDir() {
			err = nil // skipped the directory
		}
		return err
	}
	dirs, err := fsys.ReadDir(path)
	if err != nil {
		// A second call, reporting the ReadDir error.
		if err = fn(path, d, err); err != nil {
			if err == filepath.SkipDir && d.IsDir() {
				err = nil
			}
			return err
		}
	}
	for _, d1 := range dirs {
		if err := walkDir(fsys, filepath.Join(path, d1.Name()), d1, fn); err != nil {
			if err == filepath.SkipDir {
				break
			}
			return err
		}
	}
	return nil
}

// Walk is filepath.Walk on fsys: filepath's own on a filesystem with a
// Walk method (the real World's), else its algorithm over fsys's Stat
// and ReadDir.
func Walk(fsys FS, root string, fn filepath.WalkFunc) error {
	if w, ok := fsys.(interface {
		Walk(string, filepath.WalkFunc) error
	}); ok {
		return w.Walk(root, fn)
	}
	info, err := fsys.Stat(root)
	if err != nil {
		err = fn(root, nil, err)
	} else {
		err = walk(fsys, root, info, fn)
	}
	if err == filepath.SkipDir || err == filepath.SkipAll {
		return nil
	}
	return err
}

func walk(fsys FS, path string, info fs.FileInfo, fn filepath.WalkFunc) error {
	if !info.IsDir() {
		return fn(path, info, nil)
	}
	ents, err := fsys.ReadDir(path)
	err1 := fn(path, info, err)
	// A ReadDir error goes to fn; if fn doesn't stop the walk, the
	// directory is skipped (as filepath does).
	if err != nil || err1 != nil {
		return err1
	}
	for _, e := range ents {
		name := filepath.Join(path, e.Name())
		fileInfo, err := fsys.Stat(name)
		if err != nil {
			if err := fn(name, fileInfo, err); err != nil && err != filepath.SkipDir {
				return err
			}
			continue
		}
		if err = walk(fsys, name, fileInfo, fn); err != nil {
			if !fileInfo.IsDir() || err != filepath.SkipDir {
				return err
			}
		}
	}
	return nil
}

// Glob is filepath.Glob on fsys: filepath's own on a filesystem with a
// Glob method (the real World's), else its algorithm over fsys's Stat
// and ReadDir.
func Glob(fsys FS, pattern string) ([]string, error) {
	if g, ok := fsys.(interface {
		Glob(string) ([]string, error)
	}); ok {
		return g.Glob(pattern)
	}
	return globWithLimit(fsys, pattern, 0)
}

func globWithLimit(fsys FS, pattern string, depth int) (matches []string, err error) {
	const pathSeparatorsLimit = 10000
	if depth == pathSeparatorsLimit {
		return nil, filepath.ErrBadPattern
	}
	if _, err := filepath.Match(pattern, ""); err != nil {
		return nil, err
	}
	if !hasMeta(pattern) {
		if _, err = fsys.Stat(pattern); err != nil {
			return nil, nil
		}
		return []string{pattern}, nil
	}
	dir, file := filepath.Split(pattern)
	switch dir {
	case "":
		dir = "."
	case string(filepath.Separator):
	default:
		dir = dir[:len(dir)-1] // the trailing separator
	}
	if !hasMeta(dir) {
		return glob(fsys, dir, file, nil)
	}
	if dir == pattern {
		return nil, filepath.ErrBadPattern
	}
	m, err := globWithLimit(fsys, dir, depth+1)
	if err != nil {
		return nil, err
	}
	for _, d := range m {
		if matches, err = glob(fsys, d, file, matches); err != nil {
			return matches, err
		}
	}
	return matches, nil
}

func glob(fsys FS, dir, pattern string, matches []string) ([]string, error) {
	if fi, err := fsys.Stat(dir); err != nil || !fi.IsDir() {
		return matches, nil // I/O errors are ignored
	}
	ents, _ := fsys.ReadDir(dir)
	names := make([]string, len(ents))
	for i, e := range ents {
		names[i] = e.Name()
	}
	slices.Sort(names)
	for _, n := range names {
		matched, err := filepath.Match(pattern, n)
		if err != nil {
			return matches, err
		}
		if matched {
			matches = append(matches, filepath.Join(dir, n))
		}
	}
	return matches, nil
}

func hasMeta(path string) bool {
	magic := `*?[`
	if runtime.GOOS != "windows" {
		magic = `*?[\`
	}
	return strings.ContainsAny(path, magic)
}

// DirFS is os.DirFS on fsys: an fs.FS of the tree at dir. On a
// filesystem with a DirFS method (the real World's) it is that, os.DirFS
// itself in production; otherwise names are checked as os.DirFS checks
// them (fs.ValidPath) and opened under dir.
func DirFS(fsys FS, dir string) fs.FS {
	if d, ok := fsys.(interface{ DirFS(string) fs.FS }); ok {
		return d.DirFS(dir)
	}
	return dirFS{fsys, dir}
}

type dirFS struct {
	fsys FS
	dir  string
}

// join is os's dirFS.join.
func (d dirFS) join(name string) (string, error) {
	if d.dir == "" {
		return "", errors.New("os: DirFS with empty root")
	}
	if !fs.ValidPath(name) {
		return "", os.ErrInvalid
	}
	if strings.HasSuffix(d.dir, "/") {
		return d.dir + name, nil
	}
	return d.dir + "/" + name, nil
}

func (d dirFS) Open(name string) (fs.File, error) {
	full, err := d.join(name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	f, err := Open(d.fsys, full)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Path = name // os.DirFS reports the name it was given
		}
		return nil, err
	}
	return f, nil
}

func (d dirFS) ReadFile(name string) ([]byte, error) {
	full, err := d.join(name)
	if err != nil {
		return nil, &fs.PathError{Op: "readfile", Path: name, Err: err}
	}
	b, err := ReadFile(d.fsys, full)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Path = name
		}
		return nil, err
	}
	return b, nil
}

func (d dirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	full, err := d.join(name)
	if err != nil {
		return nil, &fs.PathError{Op: "readdir", Path: name, Err: err}
	}
	ents, err := d.fsys.ReadDir(full)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Path = name
		}
		return nil, err
	}
	return ents, nil
}

func (d dirFS) Stat(name string) (fs.FileInfo, error) {
	full, err := d.join(name)
	if err != nil {
		return nil, &fs.PathError{Op: "stat", Path: name, Err: err}
	}
	info, err := d.fsys.Stat(full)
	if err != nil {
		var pe *fs.PathError
		if errors.As(err, &pe) {
			pe.Path = name
		}
		return nil, err
	}
	return info, nil
}
