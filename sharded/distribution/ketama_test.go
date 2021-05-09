package distribution

// Original source: https://github.com/dgryski/go-ketama/blob/master/ketama_test.go
// TODO: Manually verify compatibility with twemproxy (not perl), including in edge cases.

import (
	"fmt"
	"testing"
)

// FIXME: Finish writing this test and fixing the ketama implementation.
// This asserts that an array of random integers get hashed to the expected bucket by asserting that the count of uint32s mapped to the bucket
// is the same as the count for the ketama implementation this aims to be compatible with.

func TestBasicCompat(t *testing.T) {
	type Pair struct {
		Server        string
		ExpectedCount int
	}
	// TODO: Verify that the result is the same as what twemproxy's algorithm would return.
	var compatTest = []Pair{
		{"server1", 8672},
		{"server2", 10825},
		{"server3", 10057},
		{"server4", 10238},
		{"server5", 9079},
		{"server6", 11149},
		{"server7", 10211},
		{"server8", 10251},
		{"server9", 9923},
		{"server10", 9595},
	}

	var buckets []Bucket

	for i := 1; i <= 10; i++ {
		label := fmt.Sprintf("server%d", i)
		b := &Bucket{Label: label, Weight: 1, Data: i}
		buckets = append(buckets, *b)
	}

	k, _ := NewKetama(buckets)

	m := make(map[int]int)

	for i := uint32(0); i < 100000; i++ {
		idxFromKetama := k.Get(i*uint32(3156322237)) - 1
		m[idxFromKetama]++
	}

	for i, tt := range compatTest {
		server := tt.Server
		actualCount := m[i]
		if actualCount != tt.ExpectedCount {
			t.Errorf("basic compatibility check failed key=%s expected=%d got=%d", server, tt.ExpectedCount, actualCount)
		}
	}
}
