package main

import (
	"flag"
	"github.com/nmorvil/singer-tap-riot/internal/tap"
	"github.com/nmorvil/singer-tap-riot/pkg/singer"
	"log"
	"os"
)

func main() {
	var (
		configPath    = flag.String("config", "", "Path to config file (required)")
		statePath     = flag.String("state", "", "Path to state file")
		catalogPath   = flag.String("catalog", "", "Path to catalog file")
		discoveryMode = flag.Bool("discover", false, "Run in discovery mode")
		outputPath    = flag.String("output", "", "Path to output file")
	)

	var singerTap *singer.Tap
	if *outputPath == "" {
		singerTap = singer.NewTap()
	} else {
		f, err := os.Open(*outputPath)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		singerTap = singer.NewTapWithWriter(f)
	}

	flag.Parse()

	if *discoveryMode {
		err := tap.RunDiscovery(singerTap)
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	if *configPath == "" {
		flag.Usage()
		log.Fatal("Error: --config is required")
	}

	cfg, err := tap.LoadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	catalog := tap.CreateCatalog()
	if *catalogPath != "" {
		catalog, err = singer.LoadCatalog(*catalogPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	var state *singer.State
	if *statePath != "" {
		state, err = singer.LoadState(*statePath)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		state = &singer.State{Value: make(map[string]map[string]int64)}
	}

	tap.RunSync(singerTap, cfg, catalog, state)

}
