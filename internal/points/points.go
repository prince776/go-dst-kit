// Package points is the registry behind dst.Inject and World.Buggify: the
// per-simulation decisions of named fault points.
//
// Each point's configuration is drawn when it is first used, from a random
// stream derived from the seed and the point's name, and each call's
// decision comes from that point's own stream: adding a point, or calling
// one more often, leaves every other point's configuration and decisions
// as they were. Nothing fires while the registry is inactive (outside the
// fault phase), and an inactive or quiet call draws nothing from the
// scheduler, so code with fault points schedules exactly as without them.
//
// Verified:
//   - [points-independent] A point's configuration and its sequence of
//     decisions don't depend on which other points exist or how often
//     they are called.
//   - [points-vary-by-seed] Each fault point fails in some seeds and is
//     quiet in others; each Buggify point fires in some seeds and never in
//     others.
//   - [inactive-is-quiet] An inactive registry never injects or fires.
//   - [outage-fails-a-service] While a service is down, every call to a
//     point under it fails, and only those.
//   - [overrides-by-prefix] An override applies to the points its prefix
//     matches; the longest matching prefix wins.
//
// Depends on: nothing
package points

import (
	"hash/fnv"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

// Kind is a fault point's decision.
type Kind int

const (
	None      Kind = iota
	Fail           // the operation didn't happen; the caller sees an error
	FailAfter      // the operation happened; the caller sees an error anyway
)

func (k Kind) String() string {
	switch k {
	case Fail:
		return "fail"
	case FailAfter:
		return "fail-after"
	}
	return "none"
}

// Config is a point's behaviour for a seed. The zero Config never fires.
type Config struct {
	Fail      float64       // chance a call fails without happening
	FailAfter float64       // chance a call fails after happening
	Slow      float64       // chance a call is slowed
	Latency   time.Duration // mean added latency of a slowed call (exponential, capped at 10x)
	Fire      float64       // Buggify points: chance a call returns true
}

// Registry holds one simulation's fault points.
type Registry struct {
	seed      uint64
	active    bool
	overrides map[string]Config
	pts       map[string]*point
	down      map[string]bool // services in an outage
	// Logf, if set, logs each point's configuration when first used.
	Logf func(format string, args ...any)
	// Cover, if set, counts a point's firing ("point NAME: fail", ...).
	Cover func(name string)
	// SlowOnly makes points add latency and never fail (Buggify points
	// never fire): so crashes can land between a request's steps when
	// fault points are otherwise off.
	SlowOnly bool
	// NewRand, if set, makes each point's stream of per-call decisions
	// (so a fork of the run reseeds them); else they come from the seed.
	NewRand func(label string) *rand.Rand
}

type point struct {
	cfg Config
	rng *rand.Rand
}

// New returns a registry for the seed; overrides replace the drawn
// configuration of the points their prefix matches (longest wins).
func New(seed uint64, overrides map[string]Config) *Registry {
	return &Registry{seed: seed, overrides: overrides, pts: map[string]*point{}, down: map[string]bool{}}
}

// SetActive turns injection on (the fault phase) or off.
func (r *Registry) SetActive(on bool) { r.active = on }

// Active reports whether injection is on.
func (r *Registry) Active() bool { return r.active }

// Service is a point's service: its name up to the first ".".
func Service(name string) string {
	s, _, _ := strings.Cut(name, ".")
	return s
}

// Services lists the services of the points used so far, sorted.
func (r *Registry) Services() []string {
	var out []string
	for name := range r.pts {
		if s := Service(name); !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out
}

// SetDown starts or ends an outage of a service.
func (r *Registry) SetDown(service string, down bool) {
	if down {
		r.down[service] = true
	} else {
		delete(r.down, service)
	}
}

func (r *Registry) get(name string, buggify bool) *point {
	if p := r.pts[name]; p != nil {
		return p
	}
	h := fnv.New64a()
	h.Write([]byte(name))
	k := h.Sum64()
	rng := rand.New(rand.NewPCG(r.seed, k))
	if r.NewRand != nil {
		rng = r.NewRand("point/" + name)
	}
	p := &point{
		rng: rng,
		cfg: draw(rand.New(rand.NewPCG(r.seed^0x9e3779b97f4a7c15, k)), buggify),
	}
	if c, ok := r.override(name); ok {
		p.cfg = c
	} else if r.SlowOnly {
		p.cfg = slowOnly(rand.New(rand.NewPCG(r.seed^0x5bd1e995, k)), buggify)
	}
	r.pts[name] = p
	if r.Logf != nil {
		r.Logf("fault point %s: %+v", name, p.cfg)
	}
	return p
}

func (r *Registry) override(name string) (Config, bool) {
	best, found := -1, Config{}
	for prefix, c := range r.overrides {
		if strings.HasPrefix(name, prefix) && len(prefix) > best {
			best, found = len(prefix), c
		}
	}
	return found, best >= 0
}

// draw picks a point's configuration for a seed. Fault points are quiet
// in a third of seeds; otherwise they fail 0.1% to 20% of calls (a random
// share of those after the operation happened), and in some seeds also
// slow calls down by 1ms to 1s. Buggify points follow FoundationDB: active
// in a quarter of seeds, firing on a quarter of calls.
func draw(r *rand.Rand, buggify bool) Config {
	if buggify {
		if r.IntN(4) == 0 {
			return Config{Fire: 0.25}
		}
		return Config{}
	}
	var c Config
	if r.IntN(3) != 0 {
		p := math.Pow(10, -3+2.3*r.Float64())
		after := r.Float64()
		c.Fail, c.FailAfter = p*(1-after), p*after
	}
	if r.IntN(3) == 0 {
		c.Slow = 0.1 * r.Float64()
		c.Latency = time.Duration(math.Pow(10, 3*r.Float64())) * time.Millisecond
	}
	return c
}

// slowOnly draws a point's latency when it may not fail: two thirds of
// points slow 10% to 50% of calls, by 1ms to 300ms.
func slowOnly(r *rand.Rand, buggify bool) Config {
	if buggify || r.IntN(3) == 0 {
		return Config{}
	}
	return Config{Slow: 0.1 + 0.4*r.Float64(), Latency: time.Duration(math.Pow(10, 2.5*r.Float64())) * time.Millisecond}
}

// Inject decides one call of a fault point: whether it fails, and how much
// latency to add.
func (r *Registry) Inject(name string) (Kind, time.Duration) {
	p := r.get(name, false) // registered even when inactive, for Services
	if !r.active {
		return None, 0
	}
	if r.down[Service(name)] {
		r.cover("point " + name + ": outage")
		return Fail, 0
	}
	c := p.cfg
	var lat time.Duration
	if c.Slow > 0 && p.rng.Float64() < c.Slow {
		lat = time.Duration(min(p.rng.ExpFloat64(), 10) * float64(c.Latency))
		r.cover("point " + name + ": slow")
	}
	if c.Fail+c.FailAfter > 0 {
		switch x := p.rng.Float64(); {
		case x < c.Fail:
			r.cover("point " + name + ": fail")
			return Fail, lat
		case x < c.Fail+c.FailAfter:
			r.cover("point " + name + ": fail-after")
			return FailAfter, lat
		}
	}
	return None, lat
}

// Buggify decides one call of a Buggify point.
func (r *Registry) Buggify(name string) bool {
	p := r.get(name, true)
	if !r.active || p.cfg.Fire == 0 || p.rng.Float64() >= p.cfg.Fire {
		return false
	}
	r.cover("buggify " + name)
	return true
}

// Config returns a point's configuration for this seed (drawing it if
// the point is new).
func (r *Registry) Config(name string, buggify bool) Config { return r.get(name, buggify).cfg }

func (r *Registry) cover(name string) {
	if r.Cover != nil {
		r.Cover(name)
	}
}
