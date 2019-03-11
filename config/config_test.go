package config

import (
	"github.com/TysonAndre/golemproxy/testutil"

	"testing"
)

// Check that this can parse the raw contents of yml files without errors.
func TestLoadExample(t *testing.T) {
	contents, err := ParseFile("./config.yml.example")
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Fprintf(os.Stderr, "read contents %#v\n", contents)
	main := contents["main"]
	failover := contents["main-fail"]

	// testutil.ExpectStringEquals(t, "main-fail", *(main.Failover), "unexpected value for failover")
	testutil.ExpectEquals(
		t,
		[]TCPServer{
			{Host: "127.0.0.1", Port: 11211, Key: "127.0.0.1:11211", Weight: 1},
			{Host: "127.0.0.1", Port: 11212, Key: "127.0.0.1:11212", Weight: 1},
		},
		main.Servers,
		"unexpected value for main servers",
	)
	testutil.ExpectEquals(
		t,
		[]TCPServer{
			{Host: "127.0.0.1", Port: 11221, Key: "127.0.0.1:11221", Weight: 1},
			{Host: "127.0.0.1", Port: 11222, Key: "127.0.0.1:11222", Weight: 1},
		},
		failover.Servers,
		"unexpected value for failover servers",
	)
}
