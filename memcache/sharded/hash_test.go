package sharded

import (
	"testing"

	"github.com/TysonAndre/golemproxy/testutil"
)

func TestFnv64a(t *testing.T) {
	testutil.ExpectEquals(t, uint32(0x84222325), fnv64a([]byte("")), "unexpected value for the empty string")
	testutil.ExpectEquals(t, uint32(0x197c2b25), fnv64a([]byte("test")), "unexpected value for test string")

	fnv64aCallback := createHasher("fnv1a_64")
	testutil.ExpectEquals(t, uint32(0x84222325), fnv64aCallback([]byte("")), "unexpected value for the empty string")
}
