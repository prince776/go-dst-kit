// Package wal is a write-ahead log: an append-only file of checksummed
// records that survives crashes. It is ordinary Go written against a
// dst.World's filesystem, and an example of what deterministic simulation
// finds in storage code.
//
// A record is a 4-byte length, a 4-byte CRC-32 of the data, and the data.
// Append returns only once the record is durable. Open recovers: it reads
// records until the first incomplete or corrupt one (a write torn by a
// crash), truncates the file there, and continues after it.
//
// Bugs plants known mistakes; the test shows the simulator catches each.
//
// Verified:
//   - [acked-is-durable] Every record Append returned for survives any
//     crash, in order.
//   - [recovery-is-a-prefix] After a crash, Open returns a prefix of the
//     appended records. It may lack only unacknowledged ones at the end:
//     never garbage, never a gap.
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
}

// Log is an open write-ahead log. It is not safe for concurrent use.
type Log struct {
	f       dst.File
	bugs    Bugs
	records [][]byte
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
	records, valid := parse(b)
	l := &Log{f: f, bugs: bugs, records: records, TornTail: valid < int64(len(b))}
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
	return l, nil
}

// parse returns the complete, intact records at the start of b and the
// length of that prefix.
func parse(b []byte) ([][]byte, int64) {
	var out [][]byte
	off := 0
	for len(b)-off >= header {
		n := int(binary.BigEndian.Uint32(b[off:]))
		sum := binary.BigEndian.Uint32(b[off+4:])
		if n > len(b)-off-header {
			break // torn: the data didn't all make it
		}
		data := b[off+header : off+header+n]
		if crc32.ChecksumIEEE(data) != sum {
			break // torn or corrupt
		}
		out = append(out, append([]byte(nil), data...))
		off += header + n
	}
	return out, int64(off)
}

// Read returns the records in file name without changing it.
func Read(fsys dst.FS, name string) ([][]byte, error) {
	b, err := dst.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	records, _ := parse(b)
	return records, nil
}

// Append adds a record and returns its index once it is durable.
func (l *Log) Append(data []byte) (int, error) {
	rec := make([]byte, header+len(data))
	binary.BigEndian.PutUint32(rec, uint32(len(data)))
	binary.BigEndian.PutUint32(rec[4:], crc32.ChecksumIEEE(data))
	copy(rec[header:], data)
	if _, err := l.f.Write(rec); err != nil {
		return 0, fmt.Errorf("wal: append: %w", err)
	}
	if !l.bugs.NoFsync {
		if err := l.f.Sync(); err != nil {
			return 0, fmt.Errorf("wal: sync: %w", err)
		}
	}
	l.records = append(l.records, append([]byte(nil), data...))
	return len(l.records) - 1, nil
}

// Records returns every record, recovered and appended.
func (l *Log) Records() [][]byte { return l.records }

// Close closes the log.
func (l *Log) Close() error { return l.f.Close() }
