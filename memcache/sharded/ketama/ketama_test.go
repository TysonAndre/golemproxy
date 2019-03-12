package ketama

// Original source: https://github.com/dgryski/go-ketama/blob/master/ketama_test.go
// TODO: Manually verify compatibility with twemproxy (not perl), including in edge cases.

import (
	"fmt"
	"testing"
)

func TestBasicCompat(t *testing.T) {
	var compatTest = []Bucket{
		{"server1", 8672},
		{"server10", 9595},
		{"server2", 10825},
		{"server3", 10057},
		{"server4", 10238},
		{"server5", 9079},
		{"server6", 11149},
		{"server7", 10211},
		{"server8", 10251},
		{"server9", 9923},
	}

	var buckets []Bucket

	for i := 1; i <= 10; i++ {
		b := &Bucket{Label: fmt.Sprintf("server%d", i), Weight: 1}
		buckets = append(buckets, *b)
	}

	k, _ := NewKetama(buckets)

	m := make(map[string]int)

	for i := 0; i < 100000; i++ {
		s := k.Hash(uint32(i) * uint32(3156322237))
		m[s]++
	}

	for _, tt := range compatTest {
		if m[tt.Label] != tt.Weight {
			t.Errorf("basic compatibility check failed key=%s expected=%d got=%d", tt.Label, tt.Weight, m[tt.Label])
		}
	}
}
