package points

import (
	"slices"
	"testing"
	"time"
)

// decisions calls the point n times and records what it decided.
func decisions(r *Registry, name string, n int) []string {
	var out []string
	for range n {
		k, lat := r.Inject(name)
		out = append(out, k.String()+"/"+lat.String())
	}
	return out
}

// noisy finds a seed where the point fails and is slowed, so the
// comparisons below have something to compare.
func noisy(t *testing.T, name string) uint64 {
	for seed := uint64(1); seed < 1000; seed++ {
		c := New(seed, nil).Config(name, false)
		if c.Fail > 0.01 && c.Slow > 0 {
			return seed
		}
	}
	t.Fatal("no seed fails and slows the point")
	return 0
}

// Verifies: [points-independent]
// Fails if: a point's configuration or decisions change when other points
// exist or are called in between; or (the negative check) two different
// points share their decisions.
func TestPointsIndependent(t *testing.T) {
	seed := noisy(t, "store.put")
	alone := New(seed, nil)
	alone.SetActive(true)
	want := decisions(alone, "store.put", 2000)

	crowded := New(seed, nil)
	crowded.SetActive(true)
	crowded.Inject("other.get") // registered first
	var got []string
	for range 2000 {
		crowded.Inject("other.get")
		crowded.Buggify("store.flaky")
		got = append(got, decisions(crowded, "store.put", 1)...)
		crowded.Inject("zzz")
	}
	if !slices.Equal(got, want) {
		t.Error("store.put decided differently once other points were in use")
	}
	if alone.Config("store.put", false) != crowded.Config("store.put", false) {
		t.Error("store.put's configuration depends on the other points")
	}
	// Negative: the comparison can tell points apart.
	other := New(seed, nil)
	other.SetActive(true)
	other.overrides = map[string]Config{"store.get": alone.Config("store.put", false)}
	if slices.Equal(decisions(other, "store.get", 2000), want) {
		t.Error("two points with the same configuration decided identically: streams aren't per point")
	}
}

// Verifies: [points-vary-by-seed]
// Fails if: across seeds a fault point never fails, always fails, never
// is slowed, or never fails after the operation; or a Buggify point is
// active in every seed or none.
func TestPointsVaryBySeed(t *testing.T) {
	var quiet, failing, after, slow, bugOn, bugOff int
	for seed := uint64(1); seed <= 300; seed++ {
		r := New(seed, nil)
		r.SetActive(true)
		ks := map[Kind]int{}
		slowed := false
		for range 3000 {
			k, lat := r.Inject("svc.op")
			ks[k]++
			slowed = slowed || lat > 0
		}
		switch {
		case ks[Fail]+ks[FailAfter] == 0:
			quiet++
		default:
			failing++
		}
		if ks[FailAfter] > 0 {
			after++
		}
		if slowed {
			slow++
		}
		fired := false
		for range 100 {
			fired = fired || r.Buggify("svc.knob")
		}
		if fired {
			bugOn++
		} else {
			bugOff++
		}
	}
	if quiet == 0 || failing == 0 || after == 0 || slow == 0 || bugOn == 0 || bugOff == 0 {
		t.Errorf("over 300 seeds: %d quiet, %d failing, %d failing after, %d slowed; buggify on in %d, off in %d", quiet, failing, after, slow, bugOn, bugOff)
	}
}

// Verifies: [inactive-is-quiet]
// Fails if: an inactive registry injects a fault, latency or Buggify, or
// (negative) the same registry, once active, never does.
func TestInactiveIsQuiet(t *testing.T) {
	r := New(noisy(t, "svc.op"), map[string]Config{"svc.knob": {Fire: 1}})
	r.SetDown("svc", true)
	for range 1000 {
		if k, lat := r.Inject("svc.op"); k != None || lat != 0 {
			t.Fatalf("inactive registry injected %v, %v", k, lat)
		}
		if r.Buggify("svc.knob") {
			t.Fatal("inactive registry fired a Buggify point")
		}
	}
	r.SetActive(true)
	if k, _ := r.Inject("svc.op"); k != Fail || !r.Buggify("svc.knob") {
		t.Error("the active registry didn't inject during an outage, or fire a certain Buggify point")
	}
}

// Verifies: [outage-fails-a-service]
// Fails if: during an outage a call under the service succeeds, a call
// under another service fails, or calls still fail once it ends.
func TestOutage(t *testing.T) {
	r := New(1, map[string]Config{"": {}}) // all points quiet
	r.SetActive(true)
	r.SetDown("store", true)
	for range 100 {
		if k, _ := r.Inject("store.put"); k != Fail {
			t.Fatalf("store.put during the store's outage: %v", k)
		}
		if k, _ := r.Inject("queue.send"); k != None {
			t.Fatalf("queue.send during the store's outage: %v", k)
		}
		if k, _ := r.Inject("storefront.get"); k != None {
			t.Fatal("storefront.get failed: services are matched by name, not by string prefix")
		}
	}
	r.SetDown("store", false)
	if k, _ := r.Inject("store.put"); k != None {
		t.Errorf("store.put after the outage: %v", k)
	}
	if got := r.Services(); !slices.Equal(got, []string{"queue", "store", "storefront"}) {
		t.Errorf("services %v", got)
	}
}

// Verifies: [overrides-by-prefix]
// Fails if: an override misses a point it matches, touches one it doesn't,
// or a shorter prefix beats a longer one.
func TestOverridesByPrefix(t *testing.T) {
	r := New(noisy(t, "other.op"), map[string]Config{
		"store.":    {Fail: 1},
		"store.get": {},
	})
	r.SetActive(true)
	if k, _ := r.Inject("store.put"); k != Fail {
		t.Errorf("store.put under a Fail: 1 override: %v", k)
	}
	if k, _ := r.Inject("store.get"); k != None {
		t.Errorf("store.get under its own quiet override: %v", k)
	}
	if r.Config("other.op", false) == (Config{}) {
		t.Error("other.op matches no override but lost its drawn configuration")
	}
	if lat := r.Config("store.put", false).Latency; lat != 0 {
		t.Errorf("an override replaces the whole configuration; latency %v remained", time.Duration(lat))
	}
}
