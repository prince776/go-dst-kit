package check_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/prince776/go-dst-kit/check"
)

// The state is a value id per key, as in pebble's and rosedb's models; an
// op is a batch of key writes (id 0 deletes).
type write struct {
	key int
	id  uint32
}

func newKV(keys int) *check.Prefix[[]uint32, []write] {
	p := check.NewPrefix(make([]uint32, keys), func(s []uint32, b []write) []uint32 {
		s = slices.Clone(s)
		for _, w := range b {
			s[w.key] = w.id
		}
		return s
	}, slices.Equal[[]uint32])
	p.Diff = func(got, want []uint32) []string {
		var d []string
		for k := range got {
			if got[k] != want[k] {
				d = append(d, fmt.Sprintf("k%d: got %d, want %d", k, got[k], want[k]))
			}
		}
		return d
	}
	return p
}

// Verifies: [recovery-is-a-prefix], [lost-durable-reported]
// Fails if: a recovered prefix at or above the floor isn't found (the
// longest, when two prefixes have one state), a state that is no prefix
// passes or is taken for a lost one, or a prefix below the floor passes
// or isn't reported as lost durable writes, naming them. The cases are
// rosedb's (single writes; power loss with the durable floor, kill with
// the acknowledged one) and pebble's (batches: part of one is no
// prefix).
func TestPrefixRecover(t *testing.T) {
	p := newKV(3)
	a := p.Invoke("a: k0=1", []write{{0, 1}})
	b := p.Invoke("b: k1=2", []write{{1, 2}})
	c := p.Invoke("c: k0=3,k2=3", []write{{0, 3}, {2, 3}}) // a batch
	p.Invoke("d: k0=1", []write{{0, 1}})                   // in flight; its state differs from c's
	p.Ack(a, true)                                         // synced: durable
	p.Ack(b, false)                                        // acknowledged, not synced
	p.Ack(c, false)
	if p.Durable() != 1 || p.Acknowledged() != 3 || p.Len() != 4 {
		t.Fatalf("durable %d, acknowledged %d, len %d", p.Durable(), p.Acknowledged(), p.Len())
	}
	for _, c := range []struct {
		name  string
		got   []uint32
		floor int
		want  int
		err   string // "" for none; "lost" or "prefix"
		in    []string
	}{
		{"power loss keeps the durable write", []uint32{1, 0, 0}, p.Durable(), 1, "", nil},
		{"power loss keeps more", []uint32{1, 2, 0}, p.Durable(), 2, "", nil},
		{"the write in flight landed", []uint32{1, 2, 3}, p.Durable(), 4, "", nil}, // d sets k0 back to 1
		{"power loss lost the durable write", []uint32{0, 0, 0}, p.Durable(), 0, "lost", []string{`lost: a: k0=1`, "first 0 writes", "first 1 were durable"}},
		{"kill lost an acknowledged write", []uint32{1, 0, 0}, p.Acknowledged(), 0, "lost", []string{"lost: b: k1=2, c: k0=3,k2=3"}},
		{"part of a batch", []uint32{3, 2, 0}, p.Durable(), 0, "prefix", []string{"closest is the first 3", `"c: k0=3,k2=3"`, "k2: got 0, want 3"}},
		{"a write without an earlier one", []uint32{0, 2, 0}, 0, 0, "prefix", []string{"no prefix"}},
	} {
		got, err := p.Recover(c.got, c.floor)
		var lost *check.LostDurableError
		var notPrefix *check.NotPrefixError
		switch {
		case c.err == "" && (err != nil || got != c.want):
			t.Errorf("%s: Recover = %d, %v; want %d", c.name, got, err, c.want)
		case c.err == "lost" && !errors.As(err, &lost):
			t.Errorf("%s: Recover = %d, %v; want a LostDurableError", c.name, got, err)
		case c.err == "prefix" && !errors.As(err, &notPrefix):
			t.Errorf("%s: Recover = %d, %v; want a NotPrefixError", c.name, got, err)
		}
		for _, s := range c.in {
			if err == nil || !strings.Contains(err.Error(), s) {
				t.Errorf("%s: error %v lacks %q", c.name, err, s)
			}
		}
	}

	// Two prefixes with one state: the longest.
	q := newKV(1)
	q.Invoke("x=1", []write{{0, 1}})
	q.Invoke("x=0", []write{{0, 0}})
	if s, err := q.Recover([]uint32{0}, 0); s != 2 || err != nil {
		t.Errorf("Recover of a state two prefixes share = %d, %v; want the longest, 2", s, err)
	}
}

// Verifies: [refused-is-absent], [durable-follows-crashes]
// Fails if: a refused write (the last, or one in the middle, as when
// writes the adopter orders are refused out of order) stays in a prefix
// or leaves a later state computed with it; or Truncate doesn't make a
// power loss's recovery durable, or lets a kill raise durability or keep
// it above what was kept.
func TestPrefixRefusedAndTruncate(t *testing.T) {
	p := newKV(3)
	p.Invoke("a", []write{{0, 1}})
	b := p.Invoke("b", []write{{1, 2}})
	p.Invoke("c", []write{{2, 3}})
	p.Refused(b)
	if p.Len() != 2 || !slices.Equal(p.State(2), []uint32{1, 0, 3}) || p.Name(1) != "c" {
		t.Fatalf("after refusing b: len %d, state %v", p.Len(), p.State(2))
	}
	if _, err := p.Recover([]uint32{1, 2, 3}, 0); err == nil {
		t.Error("a refused write's effect recovered as a prefix")
	}
	p.Refused(1)
	if p.Len() != 1 || len(p.State(1)) != 3 {
		t.Fatalf("after refusing the last: len %d", p.Len())
	}

	q := newKV(1)
	for i := range 4 {
		q.Ack(q.Invoke(fmt.Sprint(i), []write{{0, uint32(i + 1)}}), false)
	}
	q.MarkDurable(1)
	q.Truncate(3, false) // a power loss recovered three
	if q.Durable() != 3 || q.Len() != 3 {
		t.Errorf("after a power loss recovering 3: durable %d, len %d", q.Durable(), q.Len())
	}
	q.Invoke("4", []write{{0, 9}})
	q.Truncate(4, true) // a kill: what's kept may be only in the page cache
	if q.Durable() != 3 {
		t.Errorf("a kill moved durability to %d, want 3", q.Durable())
	}
	q.Truncate(2, true)
	if q.Durable() != 2 {
		t.Errorf("durability %d above the 2 writes kept", q.Durable())
	}
}

// Verifies: [window-is-what-a-read-saw]
// Fails if: Window's low end isn't the last write acknowledged before
// the read began (one acknowledged after it, or never, counts too), or
// its high end isn't every write invoked.
func TestPrefixWindow(t *testing.T) {
	p := newKV(1)
	a := p.Invoke("a", []write{{0, 1}})
	b := p.Invoke("b", []write{{0, 2}})
	p.Ack(a, false)
	since := p.Now() // a read begins: b in flight
	p.Ack(b, false)  // acknowledged while the read ran
	p.Invoke("c", []write{{0, 3}})
	if lo, hi := p.Window(since); lo != 1 || hi != 3 {
		t.Errorf("Window = [%d, %d], want [1, 3]", lo, hi)
	}
	if lo, _ := p.Window(p.Now()); lo != 2 {
		t.Errorf("a read after b's ack: lo %d, want 2", lo)
	}
}

// Verifies: [values-identify-writes]
// Fails if: a value doesn't decode to its id, or a damaged or foreign
// one decodes.
func TestValue(t *testing.T) {
	for _, size := range []int{0, 5, 64, 1000} {
		v := check.Value("k07", 12345, size)
		if size > 20 && len(v) != size {
			t.Errorf("size %d: %d bytes", size, len(v))
		}
		if id, err := check.DecodeValue("k07", v); err != nil || id != 12345 {
			t.Errorf("size %d: DecodeValue = %d, %v", size, id, err)
		}
	}
	v := check.Value("k07", 12345, 64)
	flipped := slices.Clone(v)
	flipped[40] ^= 1
	for _, c := range []struct {
		name, key string
		v         []byte
		want      string
	}{
		{"another key's", "k08", v, "another key"},
		{"a prefix key", "k0", v, "another key"},
		{"garbage", "k07", []byte("hello"), "not a value"},
		{"empty", "k07", nil, "not a value"},
		{"cut short", "k07", v[:63], "63 bytes, want 64"},
		{"extended", "k07", append(slices.Clone(v), 0), "65 bytes, want 64"},
		{"a byte changed", "k07", flipped, "damaged at byte 40"},
		{"header only", "k07", v[:14], "damaged"},
	} {
		if id, err := check.DecodeValue(c.key, c.v); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: DecodeValue = %d, %v; want an error with %q", c.name, id, err, c.want)
		}
	}
}
