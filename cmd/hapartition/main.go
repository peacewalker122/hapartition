package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/peacewalker122/hapartition/internal/gossip"
	"github.com/peacewalker122/hapartition/internal/mgmt"
	"github.com/peacewalker122/hapartition/internal/server"
)

func main() {
	nodeID := flag.String("node-id", "", "Unique node ID (default: os.Hostname())")
	redisPort := flag.String("port", "6379", "Redis-compatible TCP port")
	httpPort := flag.String("http", "8080", "HTTP management port")
	gossipPort := flag.Int("gossip-port", 7946, "Gossip (memberlist) port")
	gossipJoin := flag.String("join", "", "Comma-separated gossip seed addresses (host:port)")
	replicaRF := flag.Int("rf", 2, "Replication factor")
	advertiseAddr := flag.String("advertise-addr", "", "Address advertised in MOVED redirects and gossip meta (default: :<port>)")
	flag.Parse()

	id := *nodeID
	if id == "" {
		var err error
		id, err = os.Hostname()
		if err != nil {
			log.Fatalf("failed to get hostname: %v", err)
		}
	}

	redisAddr := ":" + *redisPort
	ringAddr := *advertiseAddr
	if ringAddr == "" {
		ringAddr = redisAddr
	}

	// Create the Redis-compatible TCP server
	redisSrv := server.New(redisAddr, id)
	// Use the advertised address for hashring entries so MOVED redirects
	// point to the routable pod address instead of :port
	if ringAddr != redisAddr {
		redisSrv.SetAdvertiseAddr(ringAddr)
	}

	// Create gossip handler with memberlist
	gossipCfg := gossip.Config{
		NodeID:         id,
		BindAddr:       "0.0.0.0",
		BindPort:       *gossipPort,
		RedisAddr:      ringAddr,
		Store:          redisSrv.Store(),
		Ring:           redisSrv.Ring(),
		ReplicaRF:      *replicaRF,
		AntiEntropySec: 30,
	}

	// Set up static discoverer if --join is provided
	if *gossipJoin != "" {
		seeds := strings.Split(*gossipJoin, ",")
		gossipCfg.Discoverer = &staticDiscoverer{seeds: seeds}
	}

	g, err := setupGossip(gossipCfg)
	if err != nil {
		log.Fatalf("gossip: %v", err)
	}

	// Wire gossip into the Redis server
	redisSrv.SetGossip(g)

	// Start Redis-compatible TCP server
	if err := redisSrv.ListenAndServe(); err != nil {
		log.Fatalf("redis: %v", err)
	}

	// Start HTTP management server (shares the same gossip state)
	httpAddr := ":" + *httpPort
	httpSrv := mgmt.New(httpAddr, g)
	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("http: %v", err)
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shutdown HTTP server first
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	// Leave gossip cluster
	if err := g.Leave(3 * time.Second); err != nil {
		log.Printf("gossip leave error: %v", err)
	}

	// Then shutdown Redis server
	if err := redisSrv.Shutdown(ctx); err != nil {
		log.Printf("redis shutdown error: %v", err)
	}

	log.Println("stopped")
}

func setupGossip(cfg gossip.Config) (*gossip.Handler, error) {
	g := gossip.New(cfg)
	if err := g.Start(); err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}
	return g, nil
}

// staticDiscoverer returns a static list of seed addresses.
type staticDiscoverer struct {
	seeds []string
}

func (d *staticDiscoverer) Discover() ([]string, error) {
	return d.seeds, nil
}
