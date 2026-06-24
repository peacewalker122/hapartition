package hapartition

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/peacewalker122/hapartition/internal/server"
)

// startServer starts a test server and returns client + cleanup.
func startServer(t *testing.T, nodeID string) (*redis.Client, func()) {
	t.Helper()
	s := server.New("127.0.0.1:0", nodeID)
	if err := s.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	host, port, _ := net.SplitHostPort(s.Addr())
	rdb := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(host, port),
	})

	cleanup := func() {
		rdb.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}
	return rdb, cleanup
}

// startServerRaw returns the raw server (for ring manipulation) + addr + cleanup.
func startServerRaw(t *testing.T, nodeID string) (*server.Server, string, func()) {
	t.Helper()
	s := server.New("127.0.0.1:0", nodeID)
	if err := s.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := s.Addr()
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}
	return s, addr, cleanup
}

// --- Non-clustered tests (single node) ---

func TestIntegrationPing(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	val, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if val != "PONG" {
		t.Fatalf("expected PONG, got %q", val)
	}
}

func TestIntegrationSetGet(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	ok, err := rdb.Set(context.Background(), "mykey", "myvalue", 0).Result()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if ok != "OK" {
		t.Fatalf("expected OK, got %q", ok)
	}

	val, err := rdb.Get(context.Background(), "mykey").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "myvalue" {
		t.Fatalf("expected myvalue, got %q", val)
	}
}

func TestIntegrationGetMissing(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	_, err := rdb.Get(context.Background(), "nonexistent").Result()
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil, got %v", err)
	}
}

func TestIntegrationDel(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	rdb.Set(context.Background(), "key1", "val1", 0)
	rdb.Set(context.Background(), "key2", "val2", 0)

	n, err := rdb.Del(context.Background(), "key1", "key2").Result()
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}

	_, err = rdb.Get(context.Background(), "key1").Result()
	if err != redis.Nil {
		t.Fatalf("expected redis.Nil after DEL, got %v", err)
	}
}

func TestIntegrationDelPartial(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	rdb.Set(context.Background(), "exists", "1", 0)

	n, err := rdb.Del(context.Background(), "exists", "missing").Result()
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}
}

func TestIntegrationSetOverwrite(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	rdb.Set(context.Background(), "k", "v1", 0)
	rdb.Set(context.Background(), "k", "v2", 0)

	val, _ := rdb.Get(context.Background(), "k").Result()
	if val != "v2" {
		t.Fatalf("expected v2 after overwrite, got %q", val)
	}
}

func TestIntegrationMultipleKeys(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("val-%d", i)
		rdb.Set(context.Background(), k, v, 0)
	}

	for _, i := range []int{0, 42, 99} {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("val-%d", i)
		val, _ := rdb.Get(context.Background(), k).Result()
		if val != v {
			t.Fatalf("GET %s expected %s, got %q", k, v, val)
		}
	}

	n, _ := rdb.Del(context.Background(), "key-0", "key-42", "key-99").Result()
	if n != 3 {
		t.Fatalf("expected 3, got %d", n)
	}
}

// --- Clustered tests (hashring / MOVED) ---

func TestIntegrationClusterSlots(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "node-1")
	defer cleanup()

	s.Ring().AddNode("node-2", "127.0.0.1:6380", 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	val, err := rdb.Do(context.Background(), "CLUSTER", "SLOTS").Result()
	if err != nil {
		t.Fatalf("CLUSTER SLOTS failed: %v", err)
	}
	if val == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegrationClusterNodes(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "node-1")
	defer cleanup()

	s.Ring().AddNode("node-2", "127.0.0.1:6380", 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	val, err := rdb.Do(context.Background(), "CLUSTER", "NODES").Result()
	if err != nil {
		t.Fatalf("CLUSTER NODES failed: %v", err)
	}
	str, _ := val.(string)
	if str == "" {
		t.Fatal("expected non-empty response")
	}
}

func TestIntegrationClusterInfo(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	val, err := rdb.Do(context.Background(), "CLUSTER", "INFO").Result()
	if err != nil {
		t.Fatalf("CLUSTER INFO failed: %v", err)
	}
	info, _ := val.(string)
	if info == "" {
		t.Fatal("expected non-empty cluster info")
	}
}

func TestIntegrationClusterKeyslot(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	val, err := rdb.Do(context.Background(), "CLUSTER", "KEYSLOT", "mykey").Result()
	if err != nil {
		t.Fatalf("CLUSTER KEYSLOT failed: %v", err)
	}
	slot, ok := val.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", val)
	}
	if slot < 0 || slot > 16383 {
		t.Fatalf("slot %d out of range 0-16383", slot)
	}
}

func TestIntegrationMovedForRemoteKey(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "node-1")
	defer cleanup()

	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)
	s.Ring().RemoveNode("node-1")

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	err := rdb.Set(context.Background(), "anykey", "val", 0).Err()
	if err == nil {
		t.Fatal("expected MOVED error")
	}
	// go-redis returns a redirect error for MOVED
	if !isMovedError(err) {
		t.Fatalf("expected MOVED error, got: %v", err)
	}
}

func TestIntegrationGetMovedForRemoteKey(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "node-1")
	defer cleanup()

	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)
	s.Ring().RemoveNode("node-1")

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	_, err := rdb.Get(context.Background(), "anykey").Result()
	if err == nil {
		t.Fatal("expected MOVED error")
	}
	if !isMovedError(err) {
		t.Fatalf("expected MOVED error, got: %v", err)
	}
}

func TestIntegrationSetLocalAndMoved(t *testing.T) {
	s, addr, cleanup := startServerRaw(t, "node-1")
	defer cleanup()

	s.Ring().AddNode("node-remote", "10.0.0.1:6380", 256)

	// Find a key that maps to local node
	var localKey string
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"} {
		nid, _ := s.Ring().GetNode(k)
		if nid == "node-1" {
			localKey = k
			break
		}
	}
	if localKey == "" {
		t.Skip("could not find local key")
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	// SET local key -> OK
	err := rdb.Set(context.Background(), localKey, "hello", 0).Err()
	if err != nil {
		t.Fatalf("SET local failed: %v", err)
	}

	// GET local key -> value
	val, _ := rdb.Get(context.Background(), localKey).Result()
	if val != "hello" {
		t.Fatalf("expected hello, got %q", val)
	}
}

// --- INFO command tests ---

func TestIntegrationInfo(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	val, err := rdb.Info(context.Background()).Result()
	if err != nil {
		t.Fatalf("INFO failed: %v", err)
	}
	if val == "" {
		t.Fatal("expected non-empty INFO")
	}
}

// --- Error handling tests ---

func TestIntegrationUnknownCommand(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	err := rdb.Do(context.Background(), "FOOBAR").Err()
	if err == nil {
		t.Fatal("expected error for unknown command")
	}
}

func TestIntegrationWrongArity(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	err := rdb.Do(context.Background(), "SET").Err()
	if err == nil {
		t.Fatal("expected arity error")
	}
}

// --- Multi-node clustered scenario ---

func TestIntegrationTwoNodeCluster(t *testing.T) {
	s1, addr1, cleanup1 := startServerRaw(t, "node-A")
	defer cleanup1()
	s2, addr2, cleanup2 := startServerRaw(t, "node-B")
	defer cleanup2()

	s1.Ring().AddNode("node-B", addr2, 256)
	s2.Ring().AddNode("node-A", addr1, 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr1})
	defer rdb.Close()

	err := rdb.Set(context.Background(), "hello", "world", 0).Err()
	if err != nil && !isMovedError(err) {
		t.Fatalf("SET failed: %v", err)
	}
}

func TestIntegrationClusterSlotsTwoNodes(t *testing.T) {
	s1, addr1, cleanup1 := startServerRaw(t, "node-A")
	defer cleanup1()
	s2, addr2, cleanup2 := startServerRaw(t, "node-B")
	defer cleanup2()

	s1.Ring().AddNode("node-B", addr2, 256)
	s2.Ring().AddNode("node-A", addr1, 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr1})
	defer rdb.Close()

	val, err := rdb.Do(context.Background(), "CLUSTER", "SLOTS").Result()
	if err != nil {
		t.Fatalf("CLUSTER SLOTS failed: %v", err)
	}
	if val == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestIntegrationClusterInfoTwoNodes(t *testing.T) {
	s1, addr1, cleanup1 := startServerRaw(t, "node-A")
	defer cleanup1()
	s2, addr2, cleanup2 := startServerRaw(t, "node-B")
	defer cleanup2()

	s1.Ring().AddNode("node-B", addr2, 256)
	s2.Ring().AddNode("node-A", addr1, 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr1})
	defer rdb.Close()

	val, err := rdb.Do(context.Background(), "CLUSTER", "INFO").Result()
	if err != nil {
		t.Fatalf("CLUSTER INFO failed: %v", err)
	}
	info, _ := val.(string)
	if info == "" {
		t.Fatal("expected non-empty cluster info")
	}
}

// --- Stress / bulk operations ---

func TestIntegrationBulkSetGet(t *testing.T) {
	rdb, cleanup := startServer(t, "node-1")
	defer cleanup()

	n := 1000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("bulk-%d", i)
		v := fmt.Sprintf("val-%d", i)
		if err := rdb.Set(context.Background(), k, v, 0).Err(); err != nil {
			t.Fatalf("SET %s failed: %v", k, err)
		}
	}

	for _, i := range []int{0, 100, 500, 999} {
		k := fmt.Sprintf("bulk-%d", i)
		v := fmt.Sprintf("val-%d", i)
		val, _ := rdb.Get(context.Background(), k).Result()
		if val != v {
			t.Fatalf("GET %s expected %s, got %q", k, v, val)
		}
	}
}

// --- Testcontainer: verify against real Redis ---

func TestIntegrationAgainstRealRedis(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	redisAddr := net.JoinHostPort(host, port.Port())

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	// Basic SET/GET against real Redis
	ok, err := rdb.Set(ctx, "rtk-key", "rtk-val", 0).Result()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}
	if ok != "OK" {
		t.Fatalf("expected OK, got %q", ok)
	}

	val, err := rdb.Get(ctx, "rtk-key").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "rtk-val" {
		t.Fatalf("expected rtk-val, got %q", val)
	}

	// DEL
	n, _ := rdb.Del(ctx, "rtk-key").Result()
	if n != 1 {
		t.Fatalf("expected 1, got %d", n)
	}

	// CLUSTER INFO against real Redis (non-clustered mode returns empty string)
	rdb.Do(ctx, "CLUSTER", "INFO").Result()
}

func TestIntegrationHapartitionVsRealRedis(t *testing.T) {
	// Spin up real Redis and hapartition, verify both respond identically
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}
	defer container.Terminate(ctx)

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "6379")
	realRedisAddr := net.JoinHostPort(host, port.Port())

	// Start hapartition
	s := server.New("127.0.0.1:0", "node-1")
	if err := s.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	// Clients
	realClient := redis.NewClient(&redis.Options{Addr: realRedisAddr})
	defer realClient.Close()
	hapClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer hapClient.Close()

	// Same SET/GET on both
	testKeys := []struct{ k, v string }{
		{"foo", "bar"},
		{"hello", "world"},
		{"counter", "42"},
	}

	for _, tc := range testKeys {
		realClient.Set(ctx, tc.k, tc.v, 0)
		hapClient.Set(ctx, tc.k, tc.v, 0)

		r1, _ := realClient.Get(ctx, tc.k).Result()
		r2, _ := hapClient.Get(ctx, tc.k).Result()

		if r1 != r2 {
			t.Fatalf("mismatch for key %s: real=%q hapartition=%q", tc.k, r1, r2)
		}
	}

	// DEL on both
	realN, _ := realClient.Del(ctx, "foo", "hello").Result()
	hapN, _ := hapClient.Del(ctx, "foo", "hello").Result()
	if realN != hapN {
		t.Fatalf("DEL count mismatch: real=%d hapartition=%d", realN, hapN)
	}
}

// --- helpers ---

func isMovedError(err error) bool {
	if err == nil {
		return false
	}
	// go-redis wraps MOVED as *redis.ClusterError or a generic error with "MOVED"
	return fmt.Sprintf("%v", err) != "" && containsMoved(err.Error())
}

func containsMoved(s string) bool {
	for i := 0; i+5 < len(s); i++ {
		if s[i:i+5] == "MOVED" {
			return true
		}
	}
	return false
}
