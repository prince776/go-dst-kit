//go:build unix

package realworld

import (
	"errors"
	"io/fs"
	"os"
	"syscall"

	"github.com/prince776/go-dst-kit/dst"
)

// LockFile is dst.LockFile: flock(2), LOCK_EX|LOCK_NB, on the file opened
// (created if need be) by name; unlock releases it and closes the file.
func (f *osFS) LockFile(name string) (func() error, error) {
	p, err := f.path("flock", name)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = dst.ErrLocked
		}
		return nil, &fs.PathError{Op: "flock", Path: name, Err: err}
	}
	return func() error {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		if err1 := file.Close(); err == nil {
			err = err1
		}
		return err
	}, nil
}
