package realworld

import (
	"context"
	"io/fs"
	"net"
	"os"
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
	file, err := r.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err // not a typed-nil *os.File
	}
	return file, nil
}

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
	return r.MkdirAll(name, perm)
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
