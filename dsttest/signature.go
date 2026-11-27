package dsttest

import (
	"regexp"
	"strings"
)

var (
	sigQuoted = regexp.MustCompile(`"[^"]*"`)
	sigNumber = regexp.MustCompile(`[0-9]+(\.[0-9]+)?([µm]?s|ns|h)?`)
	sigHex    = regexp.MustCompile(`\b[0-9a-f]{8,}\b`)
)

// Signature is a failure's kind: the first line of its message with the
// details that vary between runs of one bug taken out, so failures with
// one signature are probably one bug, and a new signature probably isn't
// one seen before. Numbers (counts, indexes, terms, durations, node and
// key numbers) and long hex become N; quoted values after the first
// ": " (keys, values, names in the failure) become Q, while the check's
// own quoted name before it stays. For instance
//
//	invariant "one leader per term" broken: term 4 has two leaders: n0 and n1
//
// becomes
//
//	invariant "one leader per term" broken: term N has two leaders: nN and nN
func Signature(err error) string {
	if err == nil {
		return ""
	}
	s, _, _ := strings.Cut(err.Error(), "\n")
	head, tail, ok := strings.Cut(s, ": ")
	if ok {
		tail = sigQuoted.ReplaceAllString(tail, "Q")
		s = head + ": " + tail
	}
	s = sigHex.ReplaceAllString(s, "N")
	return sigNumber.ReplaceAllString(s, "N")
}
