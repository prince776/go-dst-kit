package dsttest

import "testing"

// Progress levels are the value itself up to 8, then four per doubling:
// non-decreasing in the value, every level reached by some value, 0 for
// anything not positive. Fails on a level that goes down, skips a level,
// or a wrong small value.
func TestProgressLevels(t *testing.T) {
	for v := int64(-3); v <= 8; v++ {
		if got, want := progressLevel(v), int(max(v, 0)); got != want {
			t.Fatalf("level(%d) = %d, want %d", v, got, want)
		}
	}
	prev := progressLevel(8)
	for v := int64(9); v < 1<<20; v++ {
		lv := progressLevel(v)
		if lv < prev || lv > prev+1 {
			t.Fatalf("level(%d) = %d after level(%d) = %d: want the same or one more", v, lv, v-1, prev)
		}
		prev = lv
	}
	if lv := progressLevel(1 << 20); lv < 70 || lv > 90 {
		t.Errorf("level(2^20) = %d, want about four per doubling (≈80)", lv)
	}
}
