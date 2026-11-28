package realworld

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/prince776/go-dst-kit/dst"
)

// osFS is the operating system's filesystem, through the os package.
// Unrooted (dir empty, the production World) every name reaches os
// exactly as the program gave it. Rooted, names are relative to dir and
// checked as a simulated disk checks them; the check is lexical only, so
// a symbolic link inside dir can still lead outside it.
type osFS struct {
	dir string // "" for unrooted; else absolute
}

func newFS(dir string) *osFS {
	if dir == "" {
		return &osFS{}
	}
	if abs, err := filepath.Abs(dir); err == nil {
		dir = abs // a later Chdir doesn't move the root
	}
	return &osFS{dir: dir}
}

// errEscapes is what a rooted FS returns for an absolute name or one
// that leaves the root, as a simulated disk does.
var errEscapes = errors.New("path escapes from parent")

// path is the operating system's name for name: name itself when
// unrooted; under the root, after the simulated disk's check, when
// rooted.
func (f *osFS) path(op, name string) (string, error) {
	if f.dir == "" {
		return name, nil
	}
	if name == "" {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	c := path.Clean(name)
	if path.IsAbs(c) || c == ".." || strings.HasPrefix(c, "../") {
		return "", &fs.PathError{Op: op, Path: name, Err: errEscapes}
	}
	return filepath.Join(f.dir, filepath.FromSlash(c)), nil
}

func (f *osFS) OpenFile(name string, flag int, perm fs.FileMode) (dst.File, error) {
	p, err := f.path("open", name)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(p, flag, perm)
	if err != nil {
		return nil, err // not a typed-nil *os.File
	}
	if f.dir == "" {
		return file, nil // the *os.File itself: its Name, its methods
	}
	return &rootedFile{file, name}, nil
}

// rootedFile is a file a rooted FS opened. Its Name is the name it was
// opened with, as os.File's is, not the path under the root.
type rootedFile struct {
	*os.File
	name string
}

func (f *rootedFile) Name() string { return f.name }

func (f *osFS) Mkdir(name string, perm fs.FileMode) error {
	p, err := f.path("mkdir", name)
	if err != nil {
		return err
	}
	return os.Mkdir(p, perm)
}

func (f *osFS) MkdirAll(name string, perm fs.FileMode) error {
	p, err := f.path("mkdir", name)
	if err != nil {
		return err
	}
	return os.MkdirAll(p, perm)
}

func (f *osFS) Remove(name string) error {
	p, err := f.path("remove", name)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (f *osFS) RemoveAll(name string) error {
	p, err := f.path("removeall", name)
	if err != nil {
		return err
	}
	return os.RemoveAll(p)
}

func (f *osFS) Rename(oldname, newname string) error {
	o, err := f.path("rename", oldname)
	if err != nil {
		return err
	}
	n, err := f.path("rename", newname)
	if err != nil {
		return err
	}
	return os.Rename(o, n)
}

// Link is os.Link (dst.Linker).
func (f *osFS) Link(oldname, newname string) error {
	o, err := f.path("link", oldname)
	if err != nil {
		return err
	}
	n, err := f.path("link", newname)
	if err != nil {
		return err
	}
	return os.Link(o, n)
}

func (f *osFS) Stat(name string) (fs.FileInfo, error) {
	p, err := f.path("stat", name)
	if err != nil {
		return nil, err
	}
	return os.Stat(p)
}

func (f *osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	p, err := f.path("open", name)
	if err != nil {
		return nil, err
	}
	return os.ReadDir(p)
}

type osNet struct{}

func (osNet) Listen(network, address string) (net.Listener, error) {
	return net.Listen(network, address)
}

func (osNet) Dial(network, address string) (net.Conn, error) {
	return net.Dial(network, address)
}

func (osNet) ListenPacket(network, address string) (net.PacketConn, error) {
	return net.ListenPacket(network, address)
}

func (osNet) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

var (
	_ dst.FS  = (*osFS)(nil)
	_ dst.Net = osNet{}
)
