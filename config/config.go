package config

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
)

// RawConfig is the structure unserialized from the yaml config file.
type RawConfig struct {
	Listen string `yaml:"listen"`
	// Failover     *string `yaml:"failover"`
	Hash         string `yaml:"hash"`
	Distribution string `yaml:"distribution"`
	// TODO: Implement these options
	Timeout    uint `yaml:"timeout"`
	Backlog    uint `yaml:"backlog"`
	Preconnect bool `yaml:"preconnect"`
	// AutoEjectHosts bool     `yaml:"auto_eject_hosts"`
	Servers []string `yaml:"servers"`
}

func (raw *RawConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Use a different type to avoid recursing
	// https://github.com/go-yaml/yaml/issues/165#issuecomment-255223956
	type TmpConfig RawConfig
	result := TmpConfig{
		Timeout: 1000,
		Backlog: 1024,
		// Not specifying hash or distribution - those are mandatory to avoid misconfiguration
		// Hash:         "fnv1a_64",
		// Distribution: "ketama",
	}
	if err := unmarshal(&result); err != nil {
		return err
	}

	*raw = RawConfig(result)
	return nil
}

var _ yaml.Unmarshaler = &RawConfig{}

// TCPServer represents the config for a TCP Server (including the weight of requests it responds to)
type TCPServer struct {
	// Host to connect to
	Host string
	Port uint16
	// Key for hashing memcache keys to individual servers
	Key    string
	Weight uint
}

// Config is the validated data from the config file.
type Config struct {
	Listen string
	// optional failover - must exist. TODO: implement
	// Failover     *string `yaml:"failover"`
	// The hashing algorithm used for memcache keys to decide what remote server to send requests to.
	Hash string
	// The distribution algorithm used on hashes of memcache keys to decide which server to send values to.
	Distribution string
	// Timeout is the timeout in milliseconds when golemproxy assumes a connection to a server is dead.
	// TODO: implement
	Timeout uint `yaml:"timeout"`
	// Backlog is the maximum number of in-flight requests to an individual proxy server. If this is exceeded, then requests from the client will be rejected
	// TODO: implement
	Backlog uint `yaml:"backlog"`
	// Preconnect indicates if golemproxy should connect to remote servers before any incoming requests from the client arrive.
	Preconnect bool `yaml:"preconnect"`
	// AutoEjectHosts bool `yaml:"auto_eject_hosts"`
	Servers []TCPServer
}

func makeServer(raw string) (TCPServer, error) {
	failf := func(fmtString string, args ...interface{}) (TCPServer, error) {
		return TCPServer{}, fmt.Errorf(fmtString, args...)
	}
	raw = strings.TrimSpace(raw)
	parts := strings.Fields(raw)
	if len(parts) < 1 || len(parts) > 2 {
		return failf("expected 1 or 2 parts in %q, got %d", raw, len(parts))
	}

	server := parts[0]
	serverParts := strings.Split(server, ":")
	if len(serverParts) != 3 {
		return failf("expected IP:port:weight, got %q", server)
	}
	host := serverParts[0]
	if host == "" {
		return failf("empty host in %q", server)
	}
	port, err := strconv.ParseUint(serverParts[1], 10, 16)
	if err != nil || port == 0 {
		return failf("invalid port %q in %q: %v", serverParts[1], raw, err)
	}
	weight, err := strconv.ParseUint(serverParts[2], 10, 32)
	if err != nil || weight == 0 {
		return failf("invalid weight %q in %q: %v", serverParts[2], raw, err)
	}

	config := TCPServer{
		Host:   host,
		Port:   uint16(port),
		Weight: uint(weight),
	}
	if len(parts) > 2 {
		config.Key = parts[1]
	} else {
		config.Key = fmt.Sprintf("%s:%d", host, port)
	}
	return config, nil
}

func makeServers(rawServers []string) ([]TCPServer, error) {
	servers := []TCPServer{}
	for _, raw := range rawServers {
		server, err := makeServer(raw)
		if err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	return servers, nil
}

func BuildFromRawConfig(rawConfigs map[string]RawConfig, path string) (map[string]Config, error) {
	if len(rawConfigs) == 0 {
		return nil, fmt.Errorf("Did not parse any config entries from %q", path)
	}
	result := make(map[string]Config)
	errorMsgs := []string{}
	for name, raw := range rawConfigs {
		if len(raw.Listen) == 0 {
			errorMsgs = append(errorMsgs, fmt.Sprintf("empty listen for %q", name))
		}
		if raw.Hash != "fnv1a_64" {
			errorMsgs = append(errorMsgs, fmt.Sprintf(`unsupported hash %q for %q. "fnv1a_64" is supported`, raw.Hash, name))
		}
		if raw.Timeout < 10 || raw.Timeout > 60000 {
			errorMsgs = append(errorMsgs, fmt.Sprintf("unsupported/missing timeout %d for %q. Must be between 10ms and 60000ms", raw.Timeout, name))
		}
		if raw.Timeout < 10 || raw.Timeout > 60000 {
			errorMsgs = append(errorMsgs, fmt.Sprintf("unsupported/missing timeout %d for %q. Must be between 10ms and 60000ms", raw.Timeout, name))
		}
		servers, err := makeServers(raw.Servers)
		if err != nil {
			errorMsgs = append(errorMsgs, fmt.Sprintf("Invalid server in servers for %q: %v", name, err))
		}
		config := Config{
			Listen:       raw.Listen,
			Hash:         raw.Hash,
			Distribution: raw.Distribution,
			Timeout:      raw.Timeout,
			Backlog:      raw.Backlog,
			Preconnect:   raw.Preconnect,
			Servers:      servers,
		}
		result[name] = config
	}
	if len(errorMsgs) > 0 {
		return nil, errors.New(strings.Join(errorMsgs, "; "))
	}
	return result, nil
}

func ParseFile(path string) (map[string]Config, error) {
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Failed to load %q: %s", path, err)
	}
	config, err := parseRawConfigs(contents, path)
	if err != nil {
		return nil, err
	}
	return BuildFromRawConfig(config, path)
}

func parseRawConfigs(contents []byte, path string) (map[string]RawConfig, error) {
	result := make(map[string]RawConfig)
	err := yaml.Unmarshal([]byte(contents), result)
	if err != nil {
		log.Fatalf("Failed to read %q: %v", path, err)
	}

	return result, nil
}
