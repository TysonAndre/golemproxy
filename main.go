// package main is the entry point for the memcache proxy golemproxy
package main

/**
 * Copyright 2018 Tyson Andre
 */

import (
	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/server"

	"flag"
	"fmt"
	"os"
)

var configFileFlag = flag.String("config", "", "Config file path")

func main() {
	flag.Parse()
	configFile := *configFileFlag
	if configFile == "" {
		fmt.Fprintf(os.Stderr, "-config path/to/config.yml is required")
		flag.Usage()
		os.Exit(1)
	}
	configs, err := config.ParseFile(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config file %q: %v", configFile, err)
	}
	fmt.Fprintf(os.Stderr, "Starting: %#v\n", configs)
	server.Run(configs)
}
