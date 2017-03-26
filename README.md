## About

This is a text protocol memcache client library for the Go programming language
(http://golang.org/).

This is a fork of https://github.com/bradfitz/gomemcache
(Has a tiny optimization for uses with only one memcache instance, e.g. local twemproxy)

## Installing

### Using *go get*

    $ go get github.com/TysonAndre/gomemcache/memcache

After this command *gomemcache* is ready to use. Its source will be in:

    $GOPATH/src/github.com/TysonAndre/gomemcache/memcache

## Example

    import (
            "github.com/TysonAndre/gomemcache/memcache"
    )

    func main() {
         mc := memcache.New("10.0.0.1:11211")
         mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

         it, err := mc.Get("foo")
         ...
    }

## Full docs, see:

See https://godoc.org/github.com/TysonAndre/gomemcache/memcache

Or run:

    $ godoc github.com/TysonAndre/gomemcache/memcache

