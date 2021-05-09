## About

[golemproxy](https://github.com/TysonAndre/golemproxy) is a local memcache proxy written in Golang. Like [twemproxy](https://github.com/twitter/twemproxy), it supports pipelining and shards requests to multiple servers.

The memcached client this uses is based on https://github.com/bradfitz/gomemcache

- This rewrites parts of it and adds pipelining support to that library.
- For the most part, requests are sent from the client unmodified, and responses are sent from the server unmodified,
  though multigets with 2 or more keys need to be split up for requests and combined for responses.

**This is a work in progress - it can only proxy some types of commands and networking failure modes have not been tested. Other commands are proxied incorrectly.**

### Motivation

This is based on [twemproxy](https://github.com/twitter/twemproxy), which is written in C and single threaded, and is limited to only a single CPU core.
While it is very efficient, there are the following drawbacks to the approach:

1. The code is hard to reason about and requires a lot of background knowledge
   (reasoning about an asynchronous state machine written in C, async programming in C in general, reasoning about memory management correctness, detecting out of bounds memory accesses, etc).

2. Multiple async event processing backends may behave slightly differently across OSes and build options (epoll, kqueue, evport)
3. It's harder to unit test code or detect race conditions in C compared to golang.
4. Complicated datastructures such as channels and mutexes are harder to implement or install in a cross-platform way in C.
5. There is a performance bottleneck if there are dozens of CPU cores but only one nutcracker instance
   (this can be worked around by running multiple nutcracker instances, but that requires manual tuning and complicates administration)
6. According to https://blog.golang.org/ismmkeynote, golang's garbage collector should have sub-millisecond pause times in the latest releases of golang.

   This may be better than attempting to extend twemproxy in C++ instead of C (e.g. https://www.cplusplus.com/reference/thread/thread/ and other async primitives instead of state machines)
7. Golang abstracts away the management of detecting incoming data and ability to send more outgoing data with goroutines.

### Configuration

```
# Use 'main' as a pool name
main:
  # A TCP listen address or a unix socket path can be used
  #listen: /var/tmp/golemproxy.0
  listen: 127.0.0.1:21211
  # TODO: Support other hash types
  hash: fnv1a_64
  # TODO: Support other distributions
  distribution: ketama
  # auto_eject_hosts is not yet supported
  # server_retry_timeout is not yet supported, this will retry aggressively and discard all pending requests to a given server on failure
  timeout: 1000
  backlog: 1024
  preconnect: true
  servers:
#     IP:port:weight       Name(optional) for ketama distribution
    - 127.0.0.1:11211:1
    - 127.0.0.1:11212:1
```

### Similar work

Others have proposed adding multithreading support for twemproxy.

https://github.com/Netflix/dynomite is a [fork of twemproxy](https://github.com/Netflix/dynomite/wiki/FAQ#is-this-a-fork-from-twemproxy) adding replication

https://github.com/memcached/memcached/pull/716/files is a proposal to add an embedded memcache proxy to `memcached` 1.6 itself using the text/meta protocols

## TODOs

- Support more hash algorithms - only one is supported right now.
- Support memcache `noreply` requests.
- Finish supporting other memcache request types.
- Support more distributions other than ketama.
- Support evicting hosts with `auto_eject_hosts: true`
- Support redis
- Be more aggressive about validating if requests are correctly formatted

## Installing

```
go build
# Running it
./golemproxy -config config.yml.example
```

## Benchmarking

```
cd benchmark
go build
./benchmark -help
# After starting up a proxy on port 127.0.0.1:21211
./benchmark -server 127.0.0.1:21211 -workers 10
```

### Using *go get*

(no published stable tags exist)
