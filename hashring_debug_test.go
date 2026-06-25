package hapartition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/peacewalker122/hapartition/internal/gossip"
	"github.com/peacewalker122/hapartition/internal/server"
)

func TestDebugHashring(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "test-node")
	defer cleanup()

	// Check what node ID the ring returns for various keys
	keys := []string{"foo", "bar", "baz", "test", "key123", "a", "b", "c"}
	for _, k := range keys {
		nodeID, nodeAddr := s.Ring().GetNode(k)
		t.Logf("key=%s -> nodeID=%s addr=%s (server nodeID=%s)", k, nodeID, nodeAddr, s.Addr())
	}

	// Check if any keys DON'T map to our node
	localCount := 0
	remoteCount := 0
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("testkey-%d", i)
		nodeID, _ := s.Ring().GetNode(k)
		if nodeID == "test-node" {
			localCount++
		} else {
			remoteCount++
			if remoteCount <= 5 {
				t.Logf("REMOTE key=%s -> nodeID=%s", k, nodeID)
			}
		}
	}
	t.Logf("local=%d remote=%d", localCount, remoteCount)

	// Now test SET via Redis protocol
	rdb := newRedisClient(t, addr)
	defer rdb.Close()

	remoteFailures := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("bench-%d", i)
		v := fmt.Sprintf("val-%d", i)
		err := rdb.Set(context.Background(), k, v, 0).Err()
		if err != nil {
			remoteFailures++
			if remoteFailures <= 3 {
				t.Logf("SET %s failed: %v", k, err)
			}
		}
	}
	t.Logf("SET failures (MOVED): %d/100", remoteFailures)

	// Check store size
	t.Logf("store.Len() = %d", s.Store().Len())
}

func TestDebugHashringAfterGossipStart(t *testing.T) {
	// Replicate the exact startup sequence from main.go
	s := server.New("127.0.0.1:0", "test-node")
	if err := s.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	// Before gossip: check ring
	t.Log("=== Before gossip ===")
	localCount := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		nodeID, _ := s.Ring().GetNode(k)
		if nodeID == "test-node" {
			localCount++
		}
	}
	t.Logf("local keys: %d/100", localCount)

	// The ring snapshot shows all virtual nodes
	snap := s.Ring().RingSnapshot()
	t.Logf("ring entries: %d", len(snap))
	if len(snap) > 0 {
		t.Logf("first entry: nodeID=%s addr=%s", snap[0].NodeID, snap[0].Address)
	}
}

func TestDebugHashringWithGossip(t *testing.T) {
	// Replicate the exact startup sequence from main.go
	redisSrv := server.New("127.0.0.1:0", "test-node")
	if err := redisSrv.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		redisSrv.Shutdown(ctx)
		redisSrv.Wait()
	}()

	// Create gossip handler (like main.go)
	gossipCfg := gossip.Config{
		NodeID:    "test-node",
		BindAddr:  "127.0.0.1",
		BindPort:  0, // let OS pick
		RedisAddr: redisSrv.Addr(),
		Store:     redisSrv.Store(),
		Ring:      redisSrv.Ring(),
	}
	g := gossip.New(gossipCfg)
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	defer g.Leave(time.Second)

	// Wire gossip into Redis server
	redisSrv.SetGossip(g)

	t.Logf("server addr = %q", redisSrv.Addr())
	t.Logf("gossip NodeID() = %q", g.NodeID())
	// Check the internal nodeID() via ring snapshot
	snap2 := redisSrv.Ring().RingSnapshot()
	if len(snap2) > 0 {
		t.Logf("ring nodeID = %q", snap2[0].NodeID)
	}

	// Check ring after gossip join
	snap := redisSrv.Ring().RingSnapshot()
	t.Logf("ring entries after gossip: %d", len(snap))
	if len(snap) > 0 {
		t.Logf("first entry: nodeID=%s addr=%s", snap[0].NodeID, snap[0].Address)
	}

	// Check if all keys map to local node
	localCount := 0
	remoteCount := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		nodeID, _ := redisSrv.Ring().GetNode(k)
		if nodeID == g.NodeID() {
			localCount++
		} else {
			remoteCount++
			if remoteCount <= 3 {
				t.Logf("REMOTE key=%s -> nodeID=%s (expected %s)", k, nodeID, g.NodeID())
			}
		}
	}
	t.Logf("local=%d remote=%d", localCount, remoteCount)

	// Test SET via Redis protocol
	rdb := newRedisClient(t, redisSrv.Addr())
	defer rdb.Close()

	remoteFailures := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("bench-%d", i)
		v := fmt.Sprintf("val-%d", i)
		err := rdb.Set(context.Background(), k, v, 0).Err()
		if err != nil {
			remoteFailures++
			if remoteFailures <= 3 {
				t.Logf("SET %s failed: %v", k, err)
			}
		}
	}
	t.Logf("SET failures (MOVED): %d/100", remoteFailures)
	t.Logf("store.Len() = %d", redisSrv.Store().Len())
}

func newRedisClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	return redis.NewClient(&redis.Options{Addr: addr})
}
