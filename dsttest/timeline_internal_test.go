package dsttest

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Verifies: [timeline-is-faithful]
// Fails if: a machine's Downtime or Pauses, or a pair's Cut, differs from
// the crash/kill/restart, pause/resume and block/heal events in the run's
// event log (read independently, in both directions: every logged span is
// recorded and every recorded one was logged); a machine runs inside one
// of its Down or Pause intervals; Dead or Paused disagrees with whether its
// last interval is open; or (against vacuity) the sweep never records a
// downtime, a pause, a loss or a one-way cut.
func TestTimelineIsFaithful(t *testing.T) {
	names := []string{"a", "b", "c"}
	hosts := append([]string{"test"}, names...)
	opts := Options{Faults: Faults{Crash: true, Kill: true, Pause: true, Partition: true, Gray: true, Loss: true},
		FaultPhase: 3 * time.Second, RecoveryPhase: time.Second}
	var downs, pauses, oneWay, lost int
	for seed := uint64(1); seed <= 40; seed++ {
		type rec struct {
			down, pause map[string][]span
			cut         map[string][]span
		}
		var got rec
		ops := map[string]int{}
		var log []string
		res := runSeed(seed, opts, func(h *Harness) {
			for _, n := range names {
				h.Machine(n, func(w *dst.World) {
					for {
						ops[n]++
						w.Sleep(time.Millisecond)
					}
				})
			}
			h.OnLoss(func(*Machine) bool { return true }, func(*Machine) {})
			frozen := map[string]int{} // ops when the machine's last interval opened
			h.Always("no machine runs while down or paused", func() error {
				for _, m := range h.Machines() {
					d, p := m.Downtime(), m.Pauses()
					openD := len(d) > 0 && d[len(d)-1].To.IsZero()
					openP := len(p) > 0 && p[len(p)-1].To.IsZero()
					if openD != m.Dead() || openP != m.Paused() {
						return fmt.Errorf("%s: dead %v paused %v, but the timeline has down open %v, pause open %v", m.Name(), m.Dead(), m.Paused(), openD, openP)
					}
					if !openD && !openP {
						delete(frozen, m.Name())
						continue
					}
					if v, ok := frozen[m.Name()]; !ok {
						frozen[m.Name()] = ops[m.Name()]
					} else if v != ops[m.Name()] {
						return fmt.Errorf("%s ran inside a down or pause interval", m.Name())
					}
				}
				return nil
			})
			h.Final("record the timeline", func(*dst.World) error {
				base := h.World().Now().Add(-h.sim.S.Now())
				conv := func(iv []Interval) []span {
					var out []span
					for _, i := range iv {
						s := span{from: i.From.Sub(base), open: i.To.IsZero()}
						if !s.open {
							s.to = i.To.Sub(base)
						}
						out = append(out, s)
					}
					return out
				}
				got = rec{map[string][]span{}, map[string][]span{}, map[string][]span{}}
				for _, m := range h.Machines() {
					got.down[m.Name()] = conv(m.Downtime())
					got.pause[m.Name()] = conv(m.Pauses())
					if m.Lost() {
						lost++
					}
				}
				for _, x := range hosts {
					for _, y := range hosts {
						if x != y {
							got.cut[x+">"+y] = conv(h.Cut(x, y))
						}
					}
				}
				return nil
			})
		}, func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) })
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		want := fromLog(t, log)
		for _, n := range names {
			if !reflect.DeepEqual(got.down[n], want.down[n]) {
				t.Fatalf("seed %d: %s's downtime %v, the log says %v", seed, n, got.down[n], want.down[n])
			}
			if !reflect.DeepEqual(got.pause[n], want.pause[n]) {
				t.Fatalf("seed %d: %s's pauses %v, the log says %v", seed, n, got.pause[n], want.pause[n])
			}
			downs += len(got.down[n])
			pauses += len(got.pause[n])
		}
		for k, c := range got.cut {
			if !reflect.DeepEqual(c, want.cut[k]) {
				t.Fatalf("seed %d: cut %s %v, the log says %v", seed, k, c, want.cut[k])
			}
			x, y, _ := strings.Cut(k, ">")
			if len(c) > len(got.cut[y+">"+x]) {
				oneWay++
			}
		}
	}
	if downs == 0 || pauses == 0 || oneWay == 0 || lost == 0 {
		t.Fatalf("vacuous: %d downtimes, %d pauses, %d one-way cuts, %d losses recorded", downs, pauses, oneWay, lost)
	}
}

type span struct {
	from, to time.Duration
	open     bool
}

// fromLog rebuilds the spans from the event log's lines ("%12v %s": true
// time, then the event).
func fromLog(t *testing.T, log []string) (out struct{ down, pause, cut map[string][]span }) {
	out.down, out.pause, out.cut = map[string][]span{}, map[string][]span{}, map[string][]span{}
	openAt := func(m map[string][]span, k string, at time.Duration) {
		if s := m[k]; len(s) == 0 || !s[len(s)-1].open {
			m[k] = append(m[k], span{from: at, open: true})
		}
	}
	closeAt := func(m map[string][]span, k string, at time.Duration) {
		if s := m[k]; len(s) > 0 && s[len(s)-1].open {
			s[len(s)-1].to, s[len(s)-1].open = at, false
		}
	}
	for _, l := range log {
		ts, ev, _ := strings.Cut(strings.TrimSpace(l), " ")
		at, err := time.ParseDuration(ts)
		if err != nil {
			continue
		}
		f := strings.Fields(ev)
		switch {
		case len(f) >= 2 && (f[0] == "crash" || f[0] == "kill"):
			openAt(out.down, f[1], at)
			closeAt(out.pause, f[1], at) // a crash ends a pause
		case len(f) >= 2 && f[0] == "restart":
			closeAt(out.down, f[1], at)
		case len(f) == 2 && f[0] == "pause":
			openAt(out.pause, f[1], at)
		case len(f) == 2 && f[0] == "resume":
			closeAt(out.pause, f[1], at)
		case len(f) == 5 && f[0] == "network:" && f[1] == "block":
			openAt(out.cut, f[2]+">"+f[4], at)
		case ev == "network: heal":
			for k := range out.cut {
				closeAt(out.cut, k, at)
			}
		}
	}
	return out
}
