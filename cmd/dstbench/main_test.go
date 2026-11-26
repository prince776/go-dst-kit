package main

import "testing"

// The feature split flags real differences, not small-count noise. Fails
// if 1/141 against 7/159 (seen on llm-tor) counts, or 0/148 against
// 152/152 doesn't.
func TestSignificant(t *testing.T) {
	for _, c := range []struct {
		sp   split
		want bool
	}{
		{split{FailOn: 1, On: 141, FailOff: 7, Off: 159}, false},
		{split{FailOn: 0, On: 148, FailOff: 152, Off: 152}, true},
		{split{FailOn: 143, On: 146, FailOff: 0, Off: 154}, true},
		{split{FailOn: 7, On: 148, FailOff: 2, Off: 152}, false},
		{split{FailOn: 0, On: 10, FailOff: 0, Off: 10}, false},
	} {
		if got := significant(c.sp); got != c.want {
			t.Errorf("%+v: %v, want %v", c.sp, got, c.want)
		}
	}
}
