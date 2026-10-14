package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
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
	bindHost := flag.String("bind", "127.0.0.1", "Redis and HTTP listener host")
	gossipBindHost := flag.String("gossip-bind-address", "127.0.0.1", "Gossip listener host")
	redisPort := flag.String("port", "6379", "Redis-compatible TCP port")
	httpPort := flag.String("http", "8080", "HTTP management port")
	gossipPort := flag.Int("gossip-port", 7946, "Gossip (memberlist) port")
	gossipJoin := flag.String("join", "", "Comma-separated gossip seed addresses (host:port)")
	replicaRF := flag.Int("rf", 2, "Replication factor")
	writeConcern := flag.Int("w", 0, "Write concern (1 to RF, default: RF = all replicas must ACK)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file for mTLS")
	tlsKey := flag.String("tls-key", "", "TLS private key file for mTLS")
	tlsCA := flag.String("tls-ca", "", "CA certificate file for verifying peer certificates")
	advertiseAddr := flag.String("advertise-addr", "", "Address advertised in MOVED redirects and gossip meta (default: :<port>)")
	flag.Parse()

	authToken := os.Getenv("HAPARTITION_AUTH_TOKEN")
	for _, host := range []string{*bindHost, *gossipBindHost} {
		ip := net.ParseIP(host)
		if authToken == "" && host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			log.Fatalf("HAPARTITION_AUTH_TOKEN is required when bind address %q is not loopback", host)
		}
	}

	id := *nodeID
	if id == "" {
		var err error
		id, err = os.Hostname()
		if err != nil {
			log.Fatalf("failed to get hostname: %v", err)
		}
	}

	redisAddr := net.JoinHostPort(*bindHost, *redisPort)
	ringAddr := *advertiseAddr
	if ringAddr == "" {
		ringAddr = redisAddr
	}

	redisSrv := server.New(redisAddr, id)
	redisSrv.SetAuthToken(authToken)
	if ringAddr != redisAddr {
		redisSrv.SetAdvertiseAddr(ringAddr)
	}

	wc := *writeConcern
	if wc <= 0 || wc > *replicaRF {
		wc = *replicaRF
	}

	gossipCfg := gossip.Config{
		NodeID:         id,
		BindAddr:       *gossipBindHost,
		BindPort:       *gossipPort,
		RedisAddr:      ringAddr,
		Store:          redisSrv.Store(),
		Ring:           redisSrv.Ring(),
		ReplicaRF:      *replicaRF,
		WriteConcern:   wc,
		AntiEntropySec: 30,
	}
	if authToken != "" {
		secret := sha256.Sum256([]byte(authToken))
		gossipCfg.SecretKey = secret[:]
	}

	// Load TLS config for mTLS gossip.
	if *tlsCert != "" && *tlsKey != "" {
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("tls: load cert/key: %v", err)
		}

		var caPool *x509.CertPool
		if *tlsCA != "" {
			caPEM, err := os.ReadFile(*tlsCA)
			if err != nil {
				log.Fatalf("tls: read CA: %v", err)
			}
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caPEM) {
				log.Fatalf("tls: no certificates found in CA file")
			}
		} else {
			var err error
			caPool, err = x509.SystemCertPool()
			if err != nil {
				log.Fatalf("tls: system cert pool: %v", err)
			}
		}

		gossipCfg.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    caPool,
			RootCAs:      caPool,
			MinVersion:   tls.VersionTLS12,
			ServerName:   "hapartition",
		}
	}

	if *gossipJoin != "" {
		gossipCfg.Discoverer = &staticDiscoverer{seeds: strings.Split(*gossipJoin, ",")}
	}

	g, err := setupGossip(gossipCfg)
	if err != nil {
		log.Fatalf("gossip: %v", err)
	}

	redisSrv.SetGossip(g)
	redisSrv.SetWriteConcern(wc)

	if err := redisSrv.ListenAndServe(); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = redisSrv.Shutdown(ctx)
		cancel()
		_ = g.Leave(3 * time.Second)
		log.Fatalf("redis: %v", err)
	}

	httpAddr := net.JoinHostPort(*bindHost, *httpPort)
	httpSrv := mgmt.New(httpAddr, g)
	httpSrv.SetAuthToken(authToken)
	if err := httpSrv.ListenAndServe(); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = redisSrv.Shutdown(ctx)
		cancel()
		_ = g.Leave(3 * time.Second)
		log.Fatalf("http: %v", err)
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %v, shutting down...", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
	if err := g.Leave(3 * time.Second); err != nil {
		log.Printf("gossip leave error: %v", err)
	}
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
