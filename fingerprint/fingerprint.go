package fingerprint

import (
	"encoding/binary"
	"sort"

	"github.com/zeebo/xxh3"
)

// separatorByte separates tag names from tag values in the hash input,
// matching the companion writer project.
const separatorByte = byte(0xff)

// Compute calculates a 128-bit fingerprint of labels using xxh3.Hash128,
// matching the algorithm used in the companion Go writer project.
//
// Input byte layout (keys sorted alphabetically):
//
//	key1 + 0xff + val1 + key2 + 0xff + val2 + ...
//
// The 16-byte result is Lo (uint64 LE) followed by Hi (uint64 LE).
// All labels including __name__ are included.
func Compute(labels map[string]string) [16]byte {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := make([]byte, 0, 256)
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, separatorByte)
		b = append(b, labels[k]...)
	}

	h := xxh3.Hash128(b)

	var out [16]byte
	binary.LittleEndian.PutUint64(out[0:], h.Lo)
	binary.LittleEndian.PutUint64(out[8:], h.Hi)
	return out
}
