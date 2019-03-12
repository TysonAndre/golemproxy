<?php

// A simple integration test -
// verifies that this is able to respond to requests from a php client library

$cache = new Memcached();
$cache->addServer('/var/tmp/golemproxy.0', 0);
$cache->setOptions([
    Memcached::OPT_SEND_TIMEOUT => 1000000,
    Memcached::OPT_RECV_TIMEOUT => 1000000,
    Memcached::OPT_CONNECT_TIMEOUT => 100,  // millis
    Memcached::OPT_SERIALIZER => Memcached::SERIALIZER_IGBINARY,
    Memcached::OPT_COMPRESSION => true,
    Memcached::OPT_COMPRESSION_TYPE => 'fastlz',
    Memcached::OPT_AUTO_EJECT_HOSTS => false,
    Memcached::OPT_RETRY_TIMEOUT => 1,
]);
var_export($cache->set('golemproxy_test', ['foo']));
var_export($cache->get('golemproxy_test'));
