// package main is the entry point for the memcache proxy golemproxy
package main

/**
 * Copyright 2018 Tyson Andre
 */

import (
	"log"

	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/memcache/proxy"
	"github.com/sevlyar/go-daemon"

	"flag"
	"fmt"
	"os"
)

var (
	configFileFlag    = flag.String("c", "", "Config file path")
	statsPortFlag     = flag.Uint("s", 22222, "Stats port (set to 0 to disable)")
	verboseLevelFlag  = flag.Int("v", 5, "Logging level (default: 5, min: 0, max: 11)")
	daemonizeFlag     = flag.Bool("d", false, "Whether to daemonize")
	outputPathFlag    = flag.String("o", "", "set logging file (default: stderr)")
	pidFilePath       = flag.String("p", "", "set pid file (default: off)")
	mbufSizeFlag      = flag.Int("m", 0, "mbuf chunk size for twemproxy compat (IGNORED)")
	statsIntervalFlag = flag.Int("i", 30000, "stats interval in msec for twemproxy compat (IGNORED)")
)

var flagAlias = map[string]string{
	"conf-file":      "c",
	"stats-port":     "s",
	"daemonize":      "d",
	"output":         "o",
	"pid-file":       "p",
	"verbose":        "v",
	"mbuf-size":      "m",
	"stats-interval": "s",
}

func daemonize() bool {
	outputFile := *outputPathFlag
	pidFile := *pidFilePath
	if outputFile == "" {
		outputFile = "golemproxy.log"
	}
	// TODO: Configure the log file
	context := &daemon.Context{
		LogFileName: outputFile,
		LogFilePerm: 0644,
		WorkDir:     "./",
		Umask:       027,
	}
	if pidFile != "" {
		context.PidFileName = pidFile
		context.PidFilePerm = 0640
	}

	d, err := context.Reborn()
	if err != nil {
		log.Fatal("Unable to run daemon: ", err)
		os.Exit(1)
	}
	return d != nil
}

func parseAllFlags() {
	for long, short := range flagAlias {
		original := flag.Lookup(short)
		flag.Var(original.Value, long, fmt.Sprintf("%s\n(Alias of -%s)", original.Usage, short))
	}
	flag.Parse()
	unexpectedArgs := flag.Args()
	if len(unexpectedArgs) > 0 {
		fmt.Fprintf(os.Stderr, "Unexpected args: %q\n", unexpectedArgs)
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	parseAllFlags()

	configFile := *configFileFlag
	if configFile == "" {
		fmt.Fprintf(os.Stderr, "-config path/to/config.yml is required\n")
		flag.Usage()
		os.Exit(1)
	}
	configs, err := config.ParseFile(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config file %q: %v\n", configFile, err)
	}
	if *daemonizeFlag {
		fmt.Fprintf(os.Stderr, "Going to daemonize\n")
		if daemonize() {
			os.Exit(0)
		}
	}
	fmt.Fprintf(os.Stderr, "Starting: %#v\n\n", configs)
	proxy.Run(configs, *statsPortFlag)
}
