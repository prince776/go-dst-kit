// Package wal is a write-ahead log: an append-only file of checksummed
// records that survives crashes. It is ordinary Go written against a
// dst.World's filesystem, and an example of what deterministic simulation
// finds in storage code.
//
// A record is a 4-byte length, a 4-byte CRC-32 of the length and the
// data, and the data. The checksum covers the length so that zeros (a
// file whose size reached the disk before its data did) never read as a
// valid record: the CRC-32 of empty data is 0.
// Append returns only once the record is durable. Open recovers: it reads
// records until the first incomplete or corrupt one (a write torn by a
// crash), truncates the file there, and continues after it.
//
// When Append fails (a full disk writes part of the record; a failed fsync
// leaves it readable but never durable, though the next fsync succeeds),
// the log truncates the file back to the end of its last durable record
// before appending again: appending after the failed bytes would leave
// garbage, or a hole, before records acknowledged later.
//
// Bugs plants known mistakes; the test shows the simulator catches each.
//
// Verified:
//   - [acked-is-durable] Every record Append returned for survives any
//     crash, in order.
//   - [recovery-is-a-prefix] After a crash, Open returns a prefix of the
//     appended records. It may lack only unacknowledged ones at the end:
//     never garbage, never a gap.
//   - [failed-append-vanishes] A record whose Append failed (a full disk,
//     an I/O error, a failed fsync) never appears after recovery, and
//     records appended after it are kept.
//   - [real-and-simulated] The same code runs on the real filesystem.
//
// Depends on: dst
//
//dst:simulated
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"

	"github.com/prince776/go-dst-kit/dst"
)

// Bugs are mistakes the log can be told to make.
type Bugs struct {
	// NoFsync returns from Append before the record is durable.
	NoFsync bool
	// NoDirSync doesn't fsync the directory after creating the log, so a
	// crash can lose the file itself.
	NoDirSync bool
	// NoTruncateTornTail leaves a torn record at the end of the file and
	// appends after it, so the next recovery stops at the torn record and
	// loses everything appended later.
	NoTruncateTornTail bool
	// NoRepairAfterError keeps appending after a failed Append: after a
	// short write the next record follows a partial one, and after a
	// failed fsync (whose data is gone for good) it follows a hole, so
	// recovery stops there and loses records acknowledged later.
	NoRepairAfterError bool
	// ChecksumDataOnly checksums the data alone, not the length: zeros
	// past the data written back (a crash that kept the file's size but
	// not its last pages) read as valid empty records.
	ChecksumDataOnly bool
}

// Log is an open write-ahead log. It is not safe for concurrent use.
type Log struct {
	f       dst.File
	bugs    Bugs
	records [][]byte
	size    int64 // the end of the last durable record
	broken  bool  // a failed Append may have left bytes after size
	// Recovered reports what Open found: whether it discarded a torn tail.
	TornTail bool
}

const header = 8

// Open opens the log in file name on fsys, creating it if needed, and
// recovers its records.
func Open(fsys dst.FS, name string, bugs Bugs) (*Log, error) {
	_, statErr := fsys.Stat(name)
	created := errors.Is(statErr, os.ErrNotExist)
	f, err := fsys.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if created {
		if err := f.Sync(); err != nil {
			return nil, err
		}
		if !bugs.NoDirSync {
			if err := dst.SyncDir(fsys, path.Dir(name)); err != nil {
				return nil, err
			}
		}
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	records, valid := parse(b, bugs)
	l := &Log{f: f, bugs: bugs, records: records, size: valid, TornTail: valid < int64(len(b))}
	if l.TornTail && !bugs.NoTruncateTornTail {
		if err := f.Truncate(valid); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, err
		}
	}
	end := valid
	if bugs.NoTruncateTornTail {
		end = int64(len(b))
	}
	if _, err := f.Seek(end, io.SeekStart); err != nil {
		return nil, err
	}
	l.size = end
	return l, nil
}

// parse returns the complete, intact records at the start of b and the
// length of that prefix.
func parse(b []byte, bugs Bugs) ([][]byte, int64) {
	var out [][]byte
	off := 0
	for len(b)-off >= header {
		n := int(binary.BigEndian.Uint32(b[off:]))
		sum := binary.BigEndian.Uint32(b[off+4:])
		if n > len(b)-off-header {
			break // torn: the data didn't all make it
		}
		data := b[off+header : off+header+n]
		if bugs.checksum(b[off:off+4], data) != sum {
			break // torn or corrupt
		}
		out = append(out, append([]byte(nil), data...))
		off += header + n
	}
	return out, int64(off)
}

// checksum is the CRC-32 of a record's length field and its data.
func (bugs Bugs) checksum(length, data []byte) uint32 {
	if bugs.ChecksumDataOnly {
		return crc32.ChecksumIEEE(data)
	}
	return crc32.Update(crc32.ChecksumIEEE(length), crc32.IEEETable, data)
}

// Read returns the records in file name without changing it.
func Read(fsys dst.FS, name string) ([][]byte, error) { return ReadWith(fsys, name, Bugs{}) }

// ReadWith is Read for a log written with bugs (whose record format may
// differ).
func ReadWith(fsys dst.FS, name string, bugs Bugs) ([][]byte, error) {
	b, err := dst.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	records, _ := parse(b, bugs)
	return records, nil
}

// Append adds a record and returns its index once it is durable.
func (l *Log) Append(data []byte) (int, error) {
	if l.broken {
		if err := l.repair(); err != nil {
			return 0, err
		}
	}
	rec := make([]byte, header+len(data))
	binary.BigEndian.PutUint32(rec, uint32(len(data)))
	binary.BigEndian.PutUint32(rec[4:], l.bugs.checksum(rec[:4], data))
	copy(rec[header:], data)
	if _, err := l.f.Write(rec); err != nil {
		return 0, l.failed(fmt.Errorf("wal: append: %w", err))
	}
	if !l.bugs.NoFsync {
		if err := l.f.Sync(); err != nil {
			return 0, l.failed(fmt.Errorf("wal: sync: %w", err))
		}
	}
	l.size += int64(len(rec))
	l.records = append(l.records, append([]byte(nil), data...))
	return len(l.records) - 1, nil
}

// failed handles a failed Append: the record isn't durable, and the file
// may hold part of it, or all of it unsynced (a failed fsync's data never
// becomes durable), after the last durable record. The log cuts it off
// now, or before the next Append if that fails too.
func (l *Log) failed(err error) error {
	if l.bugs.NoRepairAfterError {
		l.size, _ = l.f.Seek(0, io.SeekCurrent) // carry on after it
		return err
	}
	l.broken = true
	if rerr := l.repair(); rerr != nil {
		return errors.Join(err, rerr)
	}
	return err
}

// repair truncates the file to the end of the last durable record.
func (l *Log) repair() error {
	if err := l.f.Truncate(l.size); err != nil {
		return fmt.Errorf("wal: repair: %w", err)
	}
	if _, err := l.f.Seek(l.size, io.SeekStart); err != nil {
		return fmt.Errorf("wal: repair: %w", err)
	}
	l.broken = false
	return nil
}

// Records returns every record, recovered and appended.
func (l *Log) Records() [][]byte { return l.records }

// Close closes the log.
func (l *Log) Close() error { return l.f.Close() }
