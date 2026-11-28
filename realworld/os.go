package realworld

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/prince776/go-dst-kit/dst"
)

// osFS is the operating system's filesystem, confined to a directory with
// os.Root (names can't escape it).
type osFS struct {
	dir  string
	once sync.Once
	root *os.Root
	err  error
}

func newFS(dir string) *osFS {
	if dir == "" {
		dir = "."
	}
	return &osFS{dir: dir}
}

func (f *osFS) r() (*os.Root, error) {
	f.once.Do(func() { f.root, f.err = os.OpenRoot(f.dir) })
	return f.root, f.err
}

func (f *osFS) OpenFile(name string, flag int, perm fs.FileMode) (dst.File, error) {
	r, err := f.r()
	if err != nil {
		return nil, err
	}
	file, err := openInRoot(r, name, flag, perm)
	if err != nil {
		return nil, err // not a typed-nil *os.File
	}
	return &rootFile{file, name}, nil
}

// openInRoot is r.OpenFile, retrying a create that fails with ENOENT a
// few times: a workaround for macOS's kernel ([create-like-os]), to drop
// once macOS is fixed. os.Root opens with openat(2), and on macOS (26.3
// at least) concurrent openat creates of one name fail spuriously with
// ENOENT (hundreds in 2,400 creates by 8 goroutines; open(2), which
// os.OpenFile uses, never does), so without the retry the real World's
// FS would fail where the os package doesn't. One retry was always
// enough; an ENOENT that persists (a missing directory) is returned as
// it is.
func openInRoot(r *os.Root, name string, flag int, perm fs.FileMode) (*os.File, error) {
	file, err := r.OpenFile(name, flag, perm)
	for try := 0; err != nil && flag&os.O_CREATE != 0 && try < 3 && errors.Is(err, fs.ErrNotExist); try++ {
		file, err = r.OpenFile(name, flag, perm)
	}
	return file, err
}

// rootFile is a file opened in the root. Its Name is the name it was
// opened with, as os.File's is (os.Root's files report the root's path
// joined to it, which the root then rejects as escaping).
type rootFile struct {
	*os.File
	name string
}

func (f *rootFile) Name() string { return f.name }

func (f *osFS) Mkdir(name string, perm fs.FileMode) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	return r.Mkdir(name, perm)
}

func (f *osFS) MkdirAll(name string, perm fs.FileMode) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	err = r.MkdirAll(name, perm)
	// A workaround for Go before 1.25.7 ([create-like-os]), to drop once
	// go.mod requires it: os.Root.MkdirAll failed on a directory on the
	// way that another goroutine created meanwhile.
	// Each pass gets at least one directory further, so one per path
	// element is enough; a file in the way still fails.
	for try := strings.Count(name, "/"); try >= 0 && errors.Is(err, fs.ErrExist); try-- {
		err = r.MkdirAll(name, perm)
	}
	return err
}

func (f *osFS) Remove(name string) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	return r.Remove(name)
}

func (f *osFS) RemoveAll(name string) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	return r.RemoveAll(name)
}

func (f *osFS) Rename(oldname, newname string) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	return r.Rename(oldname, newname)
}

// Link is os.Link inside the root (dst.Linker).
func (f *osFS) Link(oldname, newname string) error {
	r, err := f.r()
	if err != nil {
		return err
	}
	return r.Link(oldname, newname)
}

func (f *osFS) Stat(name string) (fs.FileInfo, error) {
	r, err := f.r()
	if err != nil {
		return nil, err
	}
	return r.Stat(name)
}

func (f *osFS) ReadDir(name string) ([]fs.DirEntry, error) {
	r, err := f.r()
	if err != nil {
		return nil, err
	}
	return fs.ReadDir(r.FS(), name)
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
