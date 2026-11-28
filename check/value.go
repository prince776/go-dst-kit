package check

import (
	"bytes"
	"fmt"
	"strconv"
)

// Value is a value that names the key and write it is for, so a read
// identifies the write it came from, byte for byte: "key/id/size/", then
// filler derived from id up to size bytes (at least the header). Give
// each write its own id, and keys without "/"; DecodeValue reads it
// back.
//
// Verified:
//   - [values-identify-writes] DecodeValue returns the id of a value
//     Value made for that key, and an error for anything else: another
//     key's value, garbage, a value cut short or extended, a changed
//     byte.
func Value(key string, id uint64, size int) []byte {
	b := fmt.Appendf(nil, "%s/%d/%d/", key, id, size)
	for i := len(b); i < size; i++ {
		b = append(b, filler(id, i))
	}
	return b
}

func filler(id uint64, i int) byte { return byte(id*31 + uint64(i)*7) }

// DecodeValue returns the id of the write v (read for key) came from, or
// an error saying how v is not a value Value made for key: written for
// another key, not a value at all, or damaged.
func DecodeValue(key string, v []byte) (uint64, error) {
	head := v[:min(len(v), len(key)+48)]
	short := func() string { return strconv.Quote(string(head)) }
	if !bytes.HasPrefix(v, []byte(key+"/")) {
		if i := bytes.IndexByte(head, '/'); i > 0 {
			return 0, fmt.Errorf("%s: a value written for another key (%d bytes): %s", key, len(v), short())
		}
		return 0, fmt.Errorf("%s: not a value (%d bytes): %s", key, len(v), short())
	}
	rest := v[len(key)+1:]
	idEnd := bytes.IndexByte(rest, '/')
	if idEnd < 0 {
		return 0, fmt.Errorf("%s: not a value (%d bytes): %s", key, len(v), short())
	}
	id, err1 := strconv.ParseUint(string(rest[:idEnd]), 10, 64)
	rest = rest[idEnd+1:]
	sizeEnd := bytes.IndexByte(rest, '/')
	if err1 != nil || sizeEnd < 0 {
		return 0, fmt.Errorf("%s: not a value (%d bytes): %s", key, len(v), short())
	}
	size, err2 := strconv.Atoi(string(rest[:sizeEnd]))
	if err2 != nil {
		return 0, fmt.Errorf("%s: not a value (%d bytes): %s", key, len(v), short())
	}
	if want := Value(key, id, size); !bytes.Equal(v, want) {
		if len(v) != len(want) {
			return 0, fmt.Errorf("%s: write %d's value is damaged: %d bytes, want %d", key, id, len(v), len(want))
		}
		at := 0
		for at < len(v) && v[at] == want[at] {
			at++
		}
		return 0, fmt.Errorf("%s: write %d's value is damaged at byte %d of %d", key, id, at, len(v))
	}
	return id, nil
}
