// Package testutil contains reusable utilities for unit tests
package testutil

import (
	"reflect"
	"testing"
)

// ExpectEquals will report a test error if reflect.DeepEqual(expected, actual) is false.
func ExpectEquals(t *testing.T, expected interface{}, actual interface{}, msg string) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("ExpectEquals failed: %s: %#v != %#v", msg, expected, actual)
	}
}

// ExpectStringEquals will report a test error if the strings expected and actual differ.
func ExpectStringEquals(t *testing.T, expected string, actual string, msg string) {
	t.Helper()
	if expected != actual {
		t.Errorf("ExpectStringEquals failed: %s: %q != %q", msg, expected, actual)
	}
}
