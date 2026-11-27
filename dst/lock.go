package dst

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// ErrLocked is returned by LockFile when another holder has the lock. It
// wraps syscall.EWOULDBLOCK, as flock(2)'s error does.
var ErrLocked = fmt.Errorf("file is locked: %w", syscall.EWOULDBLOCK)

// Locker is a filesystem with advisory file locks. The real World's (on
// unix) and the simulated disk are Lockers.
type Locker interface {
	LockFile(name string) (unlock func() error, err error)
}

// LockFile takes an exclusive advisory lock on name in fsys, creating the
// file if it doesn't exist, without waiting: if anyone holds it, LockFile
// returns ErrLocked. It is flock(2) with LOCK_EX|LOCK_NB, the lock file
// databases keep in their directory (LevelDB's LOCK, rosedb's FLOCK): a
// second LockFile of the same file fails even from the same process, and
// the lock lasts until unlock is called or the process dies (a crash or a
// kill releases it; the file stays). A filesystem that isn't a Locker
// returns errors.ErrUnsupported.
//
// Verified:
//   - [lock-exclusive] While a lock is held, LockFile of the same file
//     fails with ErrLocked; after unlock it succeeds (both Worlds: the FS
//     contract suite).
//   - [lock-dies-with-process] A crash or a kill of the holder's machine
//     releases its locks; nothing else does (simulated disk).
func LockFile(fsys FS, name string) (unlock func() error, err error) {
	l, ok := fsys.(Locker)
	if !ok {
		return nil, fmt.Errorf("lock %s: %w", name, errors.ErrUnsupported)
	}
	return l.LockFile(name)
}

// Linker is a filesystem with hard links. The real World's and the
// simulated disk are Linkers.
type Linker interface {
	Link(oldname, newname string) error
}

// Link is os.Link on fsys: it makes newname a second name (a hard link)
// for the file oldname, as checkpoints and ingestion in storage engines
// do. The data is one file, the same through either name, and it lasts
// while either name does. The new name is a directory change: like a
// create, it survives a crash only once its directory is synced. Linking
// a directory, or onto a name that exists, fails. A filesystem that isn't
// a Linker returns errors.ErrUnsupported.
//
// Verified:
//   - [link-shares-data] After Link, a write through either name reads
//     through the other, removing one name keeps the data under the
//     other, and linking onto an existing name or a directory fails (both
//     Worlds: the FS contract suite).
//   - [link-is-a-dir-change] After a crash, a link survives only as the
//     directory rules allow: lost unless its directory was synced, never
//     with data the file's own rules would have lost (simulated disk: the
//     crash model).
func Link(fsys FS, oldname, newname string) error {
	l, ok := fsys.(Linker)
	if !ok {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: errors.ErrUnsupported}
	}
	return l.Link(oldname, newname)
}
