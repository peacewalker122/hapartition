package hapartition

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/peacewalker122/hapartition/internal/server"
)

// startServer starts a test server and returns addr + cleanup.
func startServer(t *testing.T, nodeID string) (addr string, cleanup func()) {
	t.Helper()
	s := server.New("127.0.0.1:0", nodeID)
	if err := s.ListenAndServe(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = s.Addr()
	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}
	return addr, cleanup
}

// redisCli runs redis-cli against addr with the given args and returns stdout.
func redisCli(t *testing.T, addr, password string, args ...string) (string, error) {
	t.Helper()
	cliArgs := []string{"-h", "127.0.0.1"}
	host, port, _ := net.SplitHostPort(addr)
	if host == "" {
		host = "127.0.0.1"
	}
	cliArgs = append(cliArgs, "-p", port)
	if password != "" {
		cliArgs = append(cliArgs, "-a", password)
	}
	cliArgs = append(cliArgs, "--no-auth-warning")
	cliArgs = append(cliArgs, args...)

	cmd := exec.Command("redis-cli", cliArgs...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// redisCliRaw runs redis-cli and returns raw stdout (no trim).
func redisCliRaw(t *testing.T, addr string, args ...string) string {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	if host == "" {
		host = "127.0.0.1"
	}
	cliArgs := []string{"-h", host, "-p", port}
	cliArgs = append(cliArgs, args...)
	cmd := exec.Command("redis-cli", cliArgs...)
	out, _ := cmd.CombinedOutput()
	return string(out)
}

// --- Non-clustered tests (single node) ---

func TestIntegrationPing(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, err := redisCli(t, addr, "", "PING")
	if err != nil {
		t.Fatalf("redis-cli error: %v, out: %s", err, out)
	}
	if out != "PONG" {
		t.Fatalf("expected PONG, got %q", out)
	}
}

func TestIntegrationSetGet(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	// SET
	out, err := redisCli(t, addr, "", "SET", "mykey", "myvalue")
	if err != nil {
		t.Fatalf("SET failed: %v, out: %s", err, out)
	}
	if out != "OK" {
		t.Fatalf("expected OK, got %q", out)
	}

	// GET
	out, err = redisCli(t, addr, "", "GET", "mykey")
	if err != nil {
		t.Fatalf("GET failed: %v, out: %s", err, out)
	}
	if out != "myvalue" {
		t.Fatalf("expected myvalue, got %q", out)
	}
}

func TestIntegrationGetMissing(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, _ := redisCli(t, addr, "", "GET", "nonexistent")
	// valkey-cli returns empty string or (nil) for missing keys
	if out != "(nil)" && out != "" {
		t.Fatalf("expected (nil) or empty, got %q", out)
	}
}

func TestIntegrationDel(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	redisCli(t, addr, "", "SET", "key1", "val1")
	redisCli(t, addr, "", "SET", "key2", "val2")

	out, err := redisCli(t, addr, "", "DEL", "key1", "key2")
	if err != nil {
		t.Fatalf("DEL failed: %v, out: %s", err, out)
	}
	// valkey-cli outputs "2" or "(integer) 2"
	if out != "2" && out != "(integer) 2" {
		t.Fatalf("expected 2, got %q", out)
	}

	// Verify both deleted
	out, _ = redisCli(t, addr, "", "GET", "key1")
	if out != "(nil)" && out != "" {
		t.Fatalf("expected (nil) after DEL, got %q", out)
	}
}

func TestIntegrationDelPartial(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	redisCli(t, addr, "", "SET", "exists", "1")

	out, err := redisCli(t, addr, "", "DEL", "exists", "missing")
	if err != nil {
		t.Fatalf("DEL failed: %v, out: %s", err, out)
	}
	if out != "1" && out != "(integer) 1" {
		t.Fatalf("expected 1, got %q", out)
	}
}

func TestIntegrationSetOverwrite(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	redisCli(t, addr, "", "SET", "k", "v1")
	redisCli(t, addr, "", "SET", "k", "v2")

	out, err := redisCli(t, addr, "", "GET", "k")
	if err != nil {
		t.Fatalf("GET failed: %v, out: %s", err, out)
	}
	if out != "v2" {
		t.Fatalf("expected v2 after overwrite, got %q", out)
	}
}

func TestIntegrationMultipleKeys(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("val-%d", i)
		redisCli(t, addr, "", "SET", k, v)
	}

	// Spot check
	for _, i := range []int{0, 42, 99} {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("val-%d", i)
		out, err := redisCli(t, addr, "", "GET", k)
		if err != nil {
			t.Fatalf("GET %s failed: %v", k, err)
		}
		if out != v {
			t.Fatalf("expected %s, got %q", v, out)
		}
	}

	out, err := redisCli(t, addr, "", "DEL", "key-0", "key-42", "key-99")
	if err != nil {
		t.Fatalf("DEL failed: %v, out: %s", err, out)
	}
	if out != "3" && out != "(integer) 3" {
		t.Fatalf("expected 3, got %q", out)
	}
}

// --- Clustered tests (hashring / MOVED) ---

func TestIntegrationClusterSlots(t *testing.T) {
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

	s.Ring().AddNode("node-2", "127.0.0.1:6380", 256)

	// CLUSTER SLOTS via redis-cli
	out, err := redisCli(t, s.Addr(), "", "CLUSTER", "SLOTS")
	if err != nil {
		t.Fatalf("CLUSTER SLOTS failed: %v, out: %s", err, out)
	}
	// Should contain node IDs
	if !strings.Contains(out, "node-1") && !strings.Contains(out, "node-2") {
		t.Fatalf("expected node IDs in output, got: %s", out)
	}
}

func TestIntegrationClusterNodes(t *testing.T) {
	// Start server — use Ring() directly to add nodes
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

	// Add a second node to the ring
	s.Ring().AddNode("node-2", "127.0.0.1:6380", 256)

	out, err := redisCli(t, s.Addr(), "", "CLUSTER", "NODES")
	if err != nil {
		t.Fatalf("CLUSTER NODES failed: %v, out: %s", err, out)
	}
	// Should contain node IDs and slot info
	if !strings.Contains(out, "node-1") || !strings.Contains(out, "node-2") {
		t.Fatalf("expected both node-1 and node-2 in CLUSTER NODES, got: %s", out)
	}
}

func TestIntegrationClusterInfo(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, err := redisCli(t, addr, "", "CLUSTER", "INFO")
	if err != nil {
		t.Fatalf("CLUSTER INFO failed: %v, out: %s", err, out)
	}
	if !strings.Contains(out, "cluster_state:ok") {
		t.Fatalf("expected cluster_state:ok, got: %s", out)
	}
	if !strings.Contains(out, "cluster_slots_assigned:16384") {
		t.Fatalf("expected cluster_slots_assigned:16384, got: %s", out)
	}
}

func TestIntegrationClusterKeyslot(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, err := redisCli(t, addr, "", "CLUSTER", "KEYSLOT", "mykey")
	if err != nil {
		t.Fatalf("CLUSTER KEYSLOT failed: %v, out: %s", err, out)
	}
	// Should be an integer between 0 and 16383
	var slot int
	if _, scanErr := fmt.Sscanf(out, "%d", &slot); scanErr != nil {
		t.Fatalf("expected integer response, got: %s", out)
	}
	if slot < 0 || slot > 16383 {
		t.Fatalf("slot %d out of range 0-16383", slot)
	}
}

func TestIntegrationMovedForRemoteKey(t *testing.T) {
	// Start server, add a remote node, remove local from ring so all keys go remote
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

	// Add remote, remove local so all keys MOVED
	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)
	s.Ring().RemoveNode("node-1")

	addr := s.Addr()

	// SET should return MOVED
	out, err := redisCli(t, addr, "", "SET", "anykey", "val")
	if err == nil && !strings.Contains(out, "MOVED") {
		t.Fatalf("expected MOVED, got: %s", out)
	}
	// redis-cli may exit with error on MOVED
	if err != nil && !strings.Contains(out, "MOVED") {
		t.Fatalf("expected MOVED, got err=%v out=%s", err, out)
	}
}

func TestIntegrationGetMovedForRemoteKey(t *testing.T) {
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

	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)
	s.Ring().RemoveNode("node-1")

	out, _ := redisCli(t, s.Addr(), "", "GET", "anykey")
	if !strings.Contains(out, "MOVED") {
		t.Fatalf("expected MOVED, got: %s", out)
	}
}

func TestIntegrationSetLocalAndMoved(t *testing.T) {
	// With two nodes, some keys land locally, others get MOVED.
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

	s.Ring().AddNode("node-remote", "10.0.0.1:6380", 256)
	addr := s.Addr()

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

	// SET local key -> OK
	out, err := redisCli(t, addr, "", "SET", localKey, "hello")
	if err != nil {
		t.Fatalf("SET local failed: %v, out: %s", err, out)
	}
	if out != "OK" {
		t.Fatalf("expected OK for local SET, got %q", out)
	}

	// GET local key -> value
	out, err = redisCli(t, addr, "", "GET", localKey)
	if err != nil {
		t.Fatalf("GET local failed: %v, out: %s", err, out)
	}
	if out != "hello" {
		t.Fatalf("expected hello, got %q", out)
	}
}

// --- INFO command tests ---

func TestIntegrationInfo(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, err := redisCli(t, addr, "", "INFO")
	if err != nil {
		t.Fatalf("INFO failed: %v, out: %s", err, out)
	}
	if !strings.Contains(out, "redis_version:") {
		t.Fatalf("expected redis_version in INFO, got: %s", out)
	}
	if !strings.Contains(out, "cluster_enabled:1") {
		t.Fatalf("expected cluster_enabled:1 in INFO, got: %s", out)
	}
}

// --- Error handling tests ---

func TestIntegrationUnknownCommand(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, _ := redisCli(t, addr, "", "FOOBAR")
	if !strings.Contains(out, "ERR unknown command") {
		t.Fatalf("expected unknown command error, got: %s", out)
	}
}

func TestIntegrationWrongArity(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	out, _ := redisCli(t, addr, "", "SET")
	if !strings.Contains(out, "ERR wrong number of arguments") {
		t.Fatalf("expected arity error, got: %s", out)
	}
}

// --- Multi-node clustered scenario ---

func TestIntegrationTwoNodeCluster(t *testing.T) {
	// Start two servers, add each other to rings, test cross-node MOVED
	s1 := server.New("127.0.0.1:0", "node-A")
	s2 := server.New("127.0.0.1:0", "node-B")

	if err := s1.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	if err := s2.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s1.Shutdown(ctx)
		s2.Shutdown(ctx)
	}()

	// Wire the rings
	s1.Ring().AddNode("node-B", s2.Addr(), 256)
	s2.Ring().AddNode("node-A", s1.Addr(), 256)

	// Test via redis-cli on node-A
	out, err := redisCli(t, s1.Addr(), "", "SET", "hello", "world")
	if err != nil {
		t.Fatalf("SET failed: %v, out: %s", err, out)
	}

	// It's either OK (local) or MOVED (remote) — both are valid
	if out != "OK" && !strings.Contains(out, "MOVED") {
		t.Fatalf("expected OK or MOVED, got %q", out)
	}
}

func TestIntegrationClusterSlotsTwoNodes(t *testing.T) {
	s1 := server.New("127.0.0.1:0", "node-A")
	s2 := server.New("127.0.0.1:0", "node-B")

	if err := s1.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	if err := s2.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s1.Shutdown(ctx)
		s2.Shutdown(ctx)
	}()

	s1.Ring().AddNode("node-B", s2.Addr(), 256)
	s2.Ring().AddNode("node-A", s1.Addr(), 256)

	// CLUSTER SLOTS on node-A should show both nodes
	out, err := redisCli(t, s1.Addr(), "", "CLUSTER", "SLOTS")
	if err != nil {
		t.Fatalf("CLUSTER SLOTS failed: %v, out: %s", err, out)
	}
	if !strings.Contains(out, "node-A") || !strings.Contains(out, "node-B") {
		t.Fatalf("expected both node-A and node-B in CLUSTER SLOTS, got: %s", out)
	}
}

func TestIntegrationClusterInfoTwoNodes(t *testing.T) {
	s1 := server.New("127.0.0.1:0", "node-A")
	s2 := server.New("127.0.0.1:0", "node-B")

	if err := s1.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	if err := s2.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s1.Shutdown(ctx)
		s2.Shutdown(ctx)
	}()

	s1.Ring().AddNode("node-B", s2.Addr(), 256)
	s2.Ring().AddNode("node-A", s1.Addr(), 256)

	out, err := redisCli(t, s1.Addr(), "", "CLUSTER", "INFO")
	if err != nil {
		t.Fatalf("CLUSTER INFO failed: %v, out: %s", err, out)
	}
	if !strings.Contains(out, "cluster_known_nodes:2") {
		t.Fatalf("expected cluster_known_nodes:2, got: %s", out)
	}
}

// --- Stress / bulk operations ---

func TestIntegrationBulkSetGet(t *testing.T) {
	addr, cleanup := startServer(t, "node-1")
	defer cleanup()

	n := 1000
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("bulk-%d", i)
		v := fmt.Sprintf("val-%d", i)
		out, err := redisCli(t, addr, "", "SET", k, v)
		if err != nil {
			t.Fatalf("SET %s failed: %v, out: %s", k, err, out)
		}
		if out != "OK" {
			t.Fatalf("SET %s expected OK, got %q", k, out)
		}
	}

	// Verify random sample
	for _, i := range []int{0, 100, 500, 999} {
		k := fmt.Sprintf("bulk-%d", i)
		v := fmt.Sprintf("val-%d", i)
		out, err := redisCli(t, addr, "", "GET", k)
		if err != nil {
			t.Fatalf("GET %s failed: %v", k, err)
		}
		if out != v {
			t.Fatalf("GET %s expected %s, got %q", k, v, out)
		}
	}
}
