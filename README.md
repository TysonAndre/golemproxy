## About

This is a local memcache proxy written in Golang. Like [twemproxy](https://github.com/twitter/twemproxy), it supports pipelining and shards requests to multiple servers.

The memcached client this uses is based on https://github.com/bradfitz/gomemcache

- This rewrites parts of it and adds pipelining support to that library.

**This is a work in progress and is not usable yet**

## Installing

This is not usable yet.

### Using *go get*

    $ go get github.com/TysonAndre/golemproxy

See https://godoc.org/github.com/TysonAndre/golemproxy

Or run:

    $ godoc github.com/TysonAndre/golemproxy
