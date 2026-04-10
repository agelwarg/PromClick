package fingerprint

import (
	"crypto/md5"
	"sort"
)

// Compute calculates an MD5 fingerprint from labels, producing a FixedString(16)-
// compatible [16]byte value.
// All labels (including __name__) are included in the hash.
// Keys are sorted alphabetically; input is built as "k\xffv\xffk\xffv\xff...".
func Compute(labels map[string]string) [16]byte {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	b := make([]byte, 0, 256)
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, 0xff)
		b = append(b, labels[k]...)
		b = append(b, 0xff)
	}
	return md5.Sum(b)
}
