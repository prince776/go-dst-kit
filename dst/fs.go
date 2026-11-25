package dst

import (
	"io"
	"io/fs"
)

// FS is a filesystem with the os package's semantics. The real World's is
// the operating system's, rooted at a directory; a simulated World's is an
// in-memory disk with a page cache that loses unsynced data on a crash.
//
// Durability follows POSIX: written data survives a crash only after the
// file's Sync, and a created, renamed or removed name only after a Sync of
// its parent directory (open the directory and call Sync).
type FS interface {
	OpenFile(name string, flag int, perm fs.FileMode) (File, error)
	Mkdir(name string, perm fs.FileMode) error
	MkdirAll(path string, perm fs.FileMode) error
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (fs.FileInfo, error)
	ReadDir(name string) ([]fs.DirEntry, error)
}

// File is an open file or directory, like *os.File.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.Closer
	Name() string
	Stat() (fs.FileInfo, error)
	// Sync makes the file's data durable; on a directory, it makes the
	// directory's entries durable.
	Sync() error
	Truncate(size int64) error
	ReadDir(n int) ([]fs.DirEntry, error)
}
