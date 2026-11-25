package dst

import (
	"io"
	"io/fs"
	"os"
	"path"
)

// Create is os.Create on fsys.
func Create(fsys FS, name string) (File, error) {
	return fsys.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

// Open is os.Open on fsys.
func Open(fsys FS, name string) (File, error) { return fsys.OpenFile(name, os.O_RDONLY, 0) }

// ReadFile is os.ReadFile on fsys.
func ReadFile(fsys FS, name string) ([]byte, error) {
	f, err := Open(fsys, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// WriteFile is os.WriteFile on fsys. Like os.WriteFile it doesn't Sync.
func WriteFile(fsys FS, name string, data []byte, perm fs.FileMode) error {
	f, err := fsys.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// SyncDir makes the entries of directory dir durable (the fsync of a
// directory that a create, rename or remove needs to survive a crash).
func SyncDir(fsys FS, dir string) error {
	f, err := Open(fsys, path.Clean(dir))
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
