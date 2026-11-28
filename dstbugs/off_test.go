//go:build !dstbugs

package dstbugs_test

import (
	"os/exec"
	"regexp"
	"testing"

	"github.com/prince776/go-dst-kit/dstbugs"
)

// Verifies: [off-is-free]
// Fails if: without the tag a switch turns on from the environment,
// Hits counts, Tagged is true, or Bug or Fix doesn't inline (a call in
// production code); or the tagged build's own test (TestOnReadsEnv)
// fails, or doesn't run.
func TestOffIsFree(t *testing.T) {
	t.Setenv("DSTBUG", "lost-ack")
	t.Setenv("DSTFIX", "deliver-unlocked")
	if dstbugs.Tagged || dstbugs.Bug("lost-ack") || dstbugs.Fix("deliver-unlocked") || dstbugs.Hits("lost-ack") != 0 {
		t.Fatal("a switch is live in a build without the dstbugs tag")
	}
	out, err := exec.Command("go", "build", "-gcflags=-m", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	for _, fn := range []string{"Bug", "Fix"} {
		if !regexp.MustCompile(`can inline ` + fn + `\b`).Match(out) {
			t.Errorf("%s doesn't inline:\n%s", fn, out)
		}
	}
}

// TestTaggedBuild runs the tagged build's tests, so the normal suite
// covers both builds.
func TestTaggedBuild(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the package again")
	}
	out, err := exec.Command("go", "test", "-count=1", "-tags", "dstbugs", "-run", "^TestOnReadsEnv$", "-v", ".").CombinedOutput()
	if err != nil || !regexp.MustCompile(`--- PASS: TestOnReadsEnv`).Match(out) {
		t.Fatalf("the tagged build's test: %v\n%s", err, out)
	}
}
