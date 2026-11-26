package dsttest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Path names a run: a root seed and, optionally, forks. The root seed
// decides the run's configuration (fault mix, features, fault-point
// settings, clocks) and its choices until the first fork; each fork
// reseeds every stream of choices (scheduling, fault timing, machine
// randomness, disk, network, fault-point calls) from its own seed.
//
// A path replays exactly against the same code. After the code changes
// it is still a valid run with the same configuration, but may play
// differently: forks anchored to a marker drift less than ones anchored
// to a step.
//
// Its text form, accepted by -dst.path:
//
//	17                          the seed alone
//	17/"leader elected"#2:99    then, just after the 2nd Sometimes("leader elected"), seed 99
//	17/@4821:99                 then, from step 4821, seed 99
type Path struct {
	Seed  uint64
	Forks []Fork
}

// Fork continues a run with fresh randomness from Seed: just after the
// Nth time the Sometimes marker Marker is reached, or, with no Marker, at
// scheduler step Step.
type Fork struct {
	Marker string `json:"marker,omitempty"`
	N      int    `json:"n,omitempty"`
	Step   uint64 `json:"step,omitempty"`
	Seed   uint64 `json:"seed"`
}

// Then returns p with one more fork.
func (p Path) Then(f Fork) Path {
	return Path{Seed: p.Seed, Forks: append(append([]Fork(nil), p.Forks...), f)}
}

func (p Path) String() string {
	var b strings.Builder
	fmt.Fprint(&b, p.Seed)
	for _, f := range p.Forks {
		if f.Marker != "" {
			fmt.Fprintf(&b, "/%s#%d:%d", strconv.Quote(f.Marker), f.N, f.Seed)
		} else {
			fmt.Fprintf(&b, "/@%d:%d", f.Step, f.Seed)
		}
	}
	return b.String()
}

// ParsePath parses a path's text form.
func ParsePath(s string) (Path, error) {
	bad := func(why string) (Path, error) { return Path{}, fmt.Errorf("dsttest: path %q: %s", s, why) }
	i := strings.IndexByte(s, '/')
	if i < 0 {
		i = len(s)
	}
	seed, err := strconv.ParseUint(s[:i], 10, 64)
	if err != nil || seed == 0 {
		return bad("the root seed must be a positive number")
	}
	p := Path{Seed: seed}
	rest := s[i:]
	for rest != "" {
		if rest[0] != '/' {
			return bad("forks are separated by /")
		}
		rest = rest[1:]
		var f Fork
		switch {
		case strings.HasPrefix(rest, "@"):
			j := strings.IndexByte(rest, ':')
			if j < 0 {
				return bad("a step fork is @STEP:SEED")
			}
			if f.Step, err = strconv.ParseUint(rest[1:j], 10, 64); err != nil {
				return bad("bad step")
			}
			rest = rest[j:]
		case strings.HasPrefix(rest, `"`):
			q, err := strconv.QuotedPrefix(rest)
			if err != nil {
				return bad("bad marker quoting")
			}
			f.Marker, _ = strconv.Unquote(q)
			rest = rest[len(q):]
			j := strings.IndexByte(rest, ':')
			if !strings.HasPrefix(rest, "#") || j < 0 {
				return bad(`a marker fork is "MARKER"#N:SEED`)
			}
			if f.N, err = strconv.Atoi(rest[1:j]); err != nil || f.N < 1 {
				return bad("bad marker count")
			}
			rest = rest[j:]
		default:
			return bad(`a fork is @STEP:SEED or "MARKER"#N:SEED`)
		}
		j := strings.IndexByte(rest, '/')
		if j < 0 {
			j = len(rest)
		}
		if f.Seed, err = strconv.ParseUint(rest[1:j], 10, 64); err != nil {
			return bad("bad fork seed")
		}
		rest = rest[j:]
		p.Forks = append(p.Forks, f)
	}
	return p, nil
}

// flag is the command-line flag that replays p.
func (p Path) flag() string {
	if len(p.Forks) == 0 {
		return fmt.Sprintf("-dst.seed=%d", p.Seed)
	}
	return "-dst.path='" + p.String() + "'"
}

func (p Path) schedForks() []sched.Fork {
	var out []sched.Fork
	for _, f := range p.Forks {
		out = append(out, sched.Fork{Marker: f.Marker, N: f.N, Step: f.Step, Seed: f.Seed})
	}
	return out
}
