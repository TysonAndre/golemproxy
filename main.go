/**
 * Copyright 2018 Tyson Andre
 */
package main

import (
	"github.com/TysonAndre/golemproxy/config"

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
	_ = configs
}
