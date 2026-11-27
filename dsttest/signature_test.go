package dsttest_test

import (
	"errors"
	"testing"

	"github.com/prince776/go-dst-kit/dsttest"
)

// Verifies: Signature maps failures of one bug to one signature and of
// different bugs to different ones.
// Fails if: two of the same kind differ, or different kinds collide.
func TestSignature(t *testing.T) {
	same := [][2]string{
		{`invariant "one leader per term" broken: term 4 has two leaders: n0 and n1`,
			`invariant "one leader per term" broken: term 13 has two leaders: n3 and n0`},
		{"final check \"linearizable\" failed: not linearizable (partition \"b\"):\n  52 operations",
			"final check \"linearizable\" failed: not linearizable (partition \"c\"):\n  7 operations"},
		{`final check "acked" failed: write op177 was acknowledged but no node applied it`,
			`final check "acked" failed: write op9 was acknowledged but no node applied it`},
		{"goroutine n0/x#18 panicked: timeout after 1.5s", "goroutine n2/x#3 panicked: timeout after 250ms"},
	}
	for _, p := range same {
		if a, b := dsttest.Signature(errors.New(p[0])), dsttest.Signature(errors.New(p[1])); a != b {
			t.Errorf("one kind, two signatures:\n  %s\n  %s", a, b)
		}
	}
	diff := [][2]string{
		{`final check "acked" failed: lost`, `final check "linearizable" failed: lost`},
		{`invariant "x" broken: term N has two leaders`, `invariant "x" broken: log mismatch at N`},
	}
	for _, p := range diff {
		if a := dsttest.Signature(errors.New(p[0])); a == dsttest.Signature(errors.New(p[1])) {
			t.Errorf("two kinds, one signature: %s", a)
		}
	}
	if dsttest.Signature(nil) != "" {
		t.Errorf("a nil error has a signature")
	}
}
