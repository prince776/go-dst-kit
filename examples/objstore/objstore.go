// Package objstore is a fake object store with conditional writes, of the
// kind you would write for a cloud storage client in tests, a wrapper that
// adds faults to it, and a small client library on top: an append-only
// log kept in one object.
//
// The fake (Service, Bucket) is plain and correct: usable in any test.
// Faulty wraps any Store with fault points (dst.Call), so a simulation can
// make the store refuse requests (nothing happens), lose the reply to a
// write that took effect, slow calls down past the caller's deadline, or
// go down entirely. The client must cope: a write that timed out may or
// may not have happened.
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
	"context"
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

// Store is the object store's client API.
type Store interface {
	// Get returns an object's data and ETag. A missing object has no
	// data and ETag "0".
	Get(ctx context.Context, key string) ([]byte, string, error)
	// Put replaces an object's data. With ifMatch set, only if the
	// object's ETag is ifMatch ("0": only if it doesn't exist). It
	// returns the new ETag.
	Put(ctx context.Context, key string, data []byte, ifMatch string) (string, error)
}

// ---- the fake ----

// Latency is the fake store's latency for each request.
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

// Bucket is a client of the fake store. It never fails; Faulty adds
// failures. Like a server that has received a request, it ignores the
// caller's ctx.
type Bucket struct {
	w *dst.World
	s *Service
}

func (b *Bucket) Get(_ context.Context, key string) ([]byte, string, error) {
	b.w.Sleep(Latency)
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	o := b.s.objects[key]
	if o == nil {
		return nil, "0", nil
	}
	return bytes.Clone(o.data), strconv.FormatUint(o.etag, 10), nil
}

func (b *Bucket) Put(_ context.Context, key string, data []byte, ifMatch string) (string, error) {
	b.w.Sleep(Latency)
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

// ---- the fault wrapper ----

// Faulty wraps a store with the fault points objstore.get and
// objstore.put. Each method is one dst.Call, mapping the two faults to
// the errors a real client returns.
func Faulty(w *dst.World, s Store) Store { return faulty{w: w, s: s} }

type faulty struct {
	w *dst.World
	s Store
}

var (
	unavailable = func() error { return ErrUnavailable }
	timedOut    = func() error { return ErrTimeout }
	// A lost reply to a read looks like a refused read: nothing changed.
	readErrs = dst.Errors{Before: unavailable, After: unavailable}
	// A lost reply to a write is ambiguous: it may have happened.
	writeErrs = dst.Errors{Before: unavailable, After: timedOut}
)

func (f faulty) Get(ctx context.Context, key string) ([]byte, string, error) {
	type reply struct {
		data []byte
		etag string
	}
	r, err := dst.Call(ctx, f.w, "objstore.get", readErrs, func() (reply, error) {
		data, etag, err := f.s.Get(ctx, key)
		return reply{data, etag}, err
	})
	return r.data, r.etag, err
}

func (f faulty) Put(ctx context.Context, key string, data []byte, ifMatch string) (string, error) {
	return dst.Call(ctx, f.w, "objstore.put", writeErrs, func() (string, error) {
		return f.s.Put(ctx, key, data, ifMatch)
	})
}

// ---- the client library ----

// Bugs are classic mistakes the tests must catch.
type Bugs struct {
	// BlindRetry retries a write that timed out without checking whether
	// it happened: the entry can land twice.
	BlindRetry bool
	// TimeoutAsFailure reports a write that timed out (or failed in any
	// way that isn't a definite refusal) as not applied, though it may
	// have been.
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

// RequestTimeout is how long the log waits for each request.
const RequestTimeout = 100 * time.Millisecond

// Log is an append-only log of entry IDs kept in one object.
type Log struct {
	w    *dst.World
	s    Store
	key  string
	bugs Bugs
	// Attempts bounds the tries of one append (default 8).
	Attempts int
}

// NewLog returns a client of the log at key.
func NewLog(w *dst.World, s Store, key string, bugs Bugs) *Log {
	return &Log{w: w, s: s, key: key, bugs: bugs, Attempts: 8}
}

// definite reports whether a failed write certainly didn't happen. Any
// other error (a timeout, the request's deadline, something unexpected)
// leaves it unknown.
func definite(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrPrecondition)
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
		ctx, cancel := dst.WithTimeout(l.w, context.Background(), RequestTimeout)
		data, etag, err := l.s.Get(ctx, l.key)
		cancel()
		if err != nil {
			continue // a failed read changed nothing
		}
		if !l.bugs.BlindRetry && contains(Entries(data), id) {
			return Applied // an earlier attempt landed
		}
		if l.bugs.NoConditional {
			etag = ""
		}
		ctx, cancel = dst.WithTimeout(l.w, context.Background(), RequestTimeout)
		_, err = l.s.Put(ctx, l.key, append(data, id+"\n"...), etag)
		cancel()
		switch {
		case err == nil:
			return Applied
		case !definite(err):
			if l.bugs.TimeoutAsFailure {
				return NotApplied
			}
			ambiguous = true // the next read tells
		}
		// A definite failure: it didn't happen; try again.
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
