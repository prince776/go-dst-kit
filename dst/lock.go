package dst

import (
	"errors"
	"fmt"
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
