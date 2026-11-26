// Package objstore is a fake object store with conditional writes, of the
// kind you would write for a cloud storage client in tests, and a small
// client library on top: an append-only log kept in one object.
//
// The fake uses fault points (dst.Inject), so a simulation can make it
// unavailable (the request is refused: nothing happens), make a write
// time out after it took effect, slow calls down, or take the whole
// service down. The client must cope: a write that timed out may or may
// not have happened.
//
// The log appends by read-modify-write with an ETag precondition (a lost
// race is retried), and each entry has a unique ID: after an ambiguous
// failure the client re-reads and looks for its ID before writing again,
// and it reports an operation as not applied only when it knows.
//
// Verified:
//   - [no-duplicates] No entry is ever in the log twice.
//   - [applied-is-durable] An append reported applied is in the log.
//   - [not-applied-is-true] An append reported not applied is not in the
//     log.
//
// Depends on: dst
//
//dst:simulated
package objstore

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Errors the store returns.
var (
	// ErrUnavailable: the store refused the request; nothing happened.
	ErrUnavailable = errors.New("objstore: service unavailable")
	// ErrTimeout: no reply came; the request may have taken effect.
	ErrTimeout = errors.New("objstore: request timed out")
	// ErrPrecondition: the object's ETag didn't match.
	ErrPrecondition = errors.New("objstore: precondition failed")
)

// Latency is the store's latency for each request.
const Latency = time.Millisecond

type object struct {
	data []byte
	etag uint64
}

// Service is the store's backend, shared by every caller.
type Service struct {
	mu      dst.Mutex
	objects map[string]*object
	nextTag uint64
}

// NewService returns an empty store.
func NewService() *Service { return &Service{objects: map[string]*object{}} }

// Bucket returns a client of the store for code running in w.
func (s *Service) Bucket(w *dst.World) *Bucket { return &Bucket{w: w, s: s} }

// Peek returns an object's data without going through a client (for
// test checks; call it where no goroutine can run in between).
func (s *Service) Peek(key string) []byte {
	if o := s.objects[key]; o != nil {
		return o.data
	}
	return nil
}

// Bucket is a client of the store.
type Bucket struct {
	w *dst.World
	s *Service
}

// Get returns an object's data and ETag. A missing object has no data and
// ETag "0".
func (b *Bucket) Get(key string) ([]byte, string, error) {
	b.w.Sleep(Latency)
	if dst.Inject(b.w, "objstore.get") != dst.None {
		return nil, "", ErrUnavailable // a failed read has no effect either way
	}
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	o := b.s.objects[key]
	if o == nil {
		return nil, "0", nil
	}
	return bytes.Clone(o.data), strconv.FormatUint(o.etag, 10), nil
}

// Put replaces an object's data. With ifMatch set, only if the object's
// ETag is ifMatch ("0": only if it doesn't exist). It returns the new
// ETag.
func (b *Bucket) Put(key string, data []byte, ifMatch string) (string, error) {
	b.w.Sleep(Latency)
	f := dst.Inject(b.w, "objstore.put")
	if f == dst.Fail {
		return "", ErrUnavailable
	}
	tag, err := b.put(key, data, ifMatch)
	if f == dst.FailAfter {
		return "", ErrTimeout // it happened, but the reply was lost
	}
	return tag, err
}

func (b *Bucket) put(key string, data []byte, ifMatch string) (string, error) {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	o := b.s.objects[key]
	cur := "0"
	if o != nil {
		cur = strconv.FormatUint(o.etag, 10)
	}
	if ifMatch != "" && ifMatch != cur {
		return "", ErrPrecondition
	}
	b.s.nextTag++
	b.s.objects[key] = &object{data: bytes.Clone(data), etag: b.s.nextTag}
	return strconv.FormatUint(b.s.nextTag, 10), nil
}

// ---- the client library ----

// Bugs are classic mistakes the tests must catch.
type Bugs struct {
	// BlindRetry retries a write that timed out without checking whether
	// it happened: the entry can land twice.
	BlindRetry bool
	// TimeoutAsFailure reports a write that timed out as not applied,
	// though it may have been.
	TimeoutAsFailure bool
	// NoConditional writes without an ETag precondition: two clients that
	// read the same version overwrite each other's entries.
	NoConditional bool
}

// Outcome is what an append did, as far as the client knows.
type Outcome int

const (
	Unknown    Outcome = iota // it may or may not be in the log
	Applied                   // it is in the log
	NotApplied                // it is not in the log, and won't be
)

func (o Outcome) String() string { return [...]string{"unknown", "applied", "not applied"}[o] }

// Log is an append-only log of entry IDs kept in one object.
type Log struct {
	w    *dst.World
	b    *Bucket
	key  string
	bugs Bugs
	// Attempts bounds the tries of one append (default 8).
	Attempts int
}

// NewLog returns a client of the log at key.
func NewLog(w *dst.World, b *Bucket, key string, bugs Bugs) *Log {
	return &Log{w: w, b: b, key: key, bugs: bugs, Attempts: 8}
}

// Entries parses a log object.
func Entries(data []byte) []string { return strings.Fields(string(data)) }

// Append adds id (unique, without spaces) to the log.
func (l *Log) Append(id string) Outcome {
	ambiguous := false // a write of ours may have happened
	for attempt := range l.Attempts {
		if attempt > 0 {
			l.w.Sleep(time.Duration(attempt) * 2 * time.Millisecond) // back off
		}
		data, etag, err := l.b.Get(l.key)
		if err != nil {
			continue
		}
		if !l.bugs.BlindRetry && contains(Entries(data), id) {
			return Applied // an earlier attempt landed
		}
		if l.bugs.NoConditional {
			etag = ""
		}
		_, err = l.b.Put(l.key, append(data, id+"\n"...), etag)
		switch {
		case err == nil:
			return Applied
		case errors.Is(err, ErrTimeout):
			if l.bugs.TimeoutAsFailure {
				return NotApplied
			}
			ambiguous = true // the next read tells
		}
		// ErrUnavailable or ErrPrecondition: it didn't happen; try again.
	}
	if ambiguous {
		return Unknown
	}
	return NotApplied
}

func contains(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}
