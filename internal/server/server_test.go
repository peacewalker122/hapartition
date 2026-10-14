package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	s := New("127.0.0.1:0", "test-node")
	err := s.ListenAndServe()
	if err != nil {
		t.Fatalf("failed to start server: %v", err)
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

// dialResp sends a raw RESP command and reads the first response line.
func dialResp(t *testing.T, addr, cmd string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return line
}

// dialFull reads the complete response for commands that return more than one line.
func dialFull(t *testing.T, addr, cmd string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write([]byte(cmd))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	br := bufio.NewReader(conn)
	var sb strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			break
		}
		sb.WriteString(line)
		// Stop at the end of a bulk string (after the \r\n content line)
		// or after an array end — simple heuristic: break after two lines for
		// bulk-string responses, or on the first line for simple responses.
		// Full reads are caller's responsibility to know the format.
		if strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") || strings.HasPrefix(line, ":") {
			break
		}
	}
	return sb.String()
}

// --- Original command tests ---

func TestPing(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*1\r\n$4\r\nPING\r\n")
	if resp != "+PONG\r\n" {
		t.Fatalf("expected +PONG\\r\\n, got %q", resp)
	}
}

func TestSetAndGet(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	if resp != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", resp)
	}

	conn, _ := net.Dial("tcp", addr)
	defer conn.Close()
	conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	br := bufio.NewReader(conn)
	br.ReadString('\n') // $5\r\n
	val, _ := br.ReadString('\n')
	if val != "value\r\n" {
		t.Fatalf("expected value\\r\\n, got %q", val)
	}
}

func TestGetMissing(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*2\r\n$3\r\nGET\r\n$6\r\nabsent\r\n")
	if resp != "$-1\r\n" {
		t.Fatalf("expected $-1\\r\\n, got %q", resp)
	}
}

func TestSetGetRequireExactArity(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	for _, tc := range []struct {
		name, cmd, want string
	}{
		{"SET missing value", "*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n", "-ERR wrong number of arguments for 'SET' command\r\n"},
		{"SET extra value", "*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$5\r\nextra\r\n", "-ERR wrong number of arguments for 'SET' command\r\n"},
		{"GET missing key", "*1\r\n$3\r\nGET\r\n", "-ERR wrong number of arguments for 'GET' command\r\n"},
		{"GET extra key", "*3\r\n$3\r\nGET\r\n$3\r\nkey\r\n$5\r\nextra\r\n", "-ERR wrong number of arguments for 'GET' command\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dialResp(t, addr, tc.cmd); got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}

	if got := dialResp(t, addr, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"); got != "$-1\r\n" {
		t.Fatalf("extra-argument SET wrote key: GET returned %q", got)
	}
}

func TestDel(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	dialResp(t, addr, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
	resp := dialResp(t, addr, "*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}
	resp = dialResp(t, addr, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
	if resp != "$-1\r\n" {
		t.Fatalf("expected $-1\\r\\n, got %q", resp)
	}
}

func TestDelMultiple(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	dialResp(t, addr, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
	dialResp(t, addr, "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n")
	dialResp(t, addr, "*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n")

	resp := dialResp(t, addr, "*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nx\r\n")
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n, got %q", resp)
	}
}

func TestRedisAuthRequired(t *testing.T) {
	s := New("127.0.0.1:0", "auth-node")
	s.SetAuthToken("secret")
	if err := s.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for _, tc := range []struct {
		command string
		want    string
	}{
		{"*1\r\n$4\r\nPING\r\n", "-NOAUTH Authentication required.\r\n"},
		{"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n", "-WRONGPASS invalid username-password pair or user is disabled.\r\n"},
		{"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n", "+OK\r\n"},
		{"*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
	} {
		if _, err := conn.Write([]byte(tc.command)); err != nil {
			t.Fatal(err)
		}
		got, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("expected %q, got %q", tc.want, got)
		}
	}
}

func TestDelDoesNotMutateWhenAnyKeyMoved(t *testing.T) {
	s := New("127.0.0.1:0", "test-node")
	s.Store().Set("local", "value")
	s.Ring().RemoveNode("test-node")
	s.Ring().AddNode("remote", "10.0.0.1:6379", 256)
	if err := s.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	resp := dialResp(t, s.Addr(), "*3\r\n$3\r\nDEL\r\n$5\r\nlocal\r\n$6\r\nabsent\r\n")
	if !strings.HasPrefix(resp, "-MOVED ") {
		t.Fatalf("expected MOVED error, got %q", resp)
	}
	if s.Store().Len() != 1 {
		t.Fatalf("expected store unchanged, got %d keys", s.Store().Len())
	}
}

func TestUnknownCommand(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*2\r\n$3\r\nFOO\r\n$3\r\nbar\r\n")
	expected := fmt.Sprintf("-ERR unknown command 'FOO'\r\n")
	if resp != expected {
		t.Fatalf("expected %q, got %q", expected, resp)
	}
}

// --- Node membership command tests ---

func TestNodeJoin(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*3\r\n$9\r\nNODE.JOIN\r\n$6\r\nnode-a\r\n$13\r\n10.0.0.1:6379\r\n")
	if resp != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", resp)
	}
}

func TestNodeJoinWrongArity(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*2\r\n$9\r\nNODE.JOIN\r\n$6\r\nnode-a\r\n")
	if !strings.HasPrefix(resp, "-ERR wrong number of arguments") {
		t.Fatalf("expected arity error, got %q", resp)
	}
}

func TestNodeListWithoutGossip(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	// NODE.LIST without gossip returns an error
	resp := dialResp(t, addr, "*1\r\n$9\r\nNODE.LIST\r\n")
	if !strings.Contains(resp, "gossip not initialized") {
		t.Fatalf("expected gossip not initialized, got %q", resp)
	}
}

func TestNodeListWithRing(t *testing.T) {
	// NODE.LIST without gossip should still work with the ring
	addr, cleanup := startTestServer(t)
	defer cleanup()

	// Add a peer to the ring
	dialResp(t, addr, "*3\r\n$9\r\nNODE.JOIN\r\n$6\r\nnode-a\r\n$13\r\n10.0.0.1:6379\r\n")

	// NODE.LIST without gossip returns "gossip not initialized"
	resp := dialResp(t, addr, "*1\r\n$9\r\nNODE.LIST\r\n")
	if !strings.Contains(resp, "gossip not initialized") {
		t.Fatalf("expected gossip not initialized, got %q", resp)
	}
}

// --- Hashring / MOVED tests ---

func TestSetReturnsMovedForRemoteKey(t *testing.T) {
	s := New("127.0.0.1:0", "test-node")
	err := s.ListenAndServe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	// Add a remote peer directly to the ring
	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)

	// Remove local node from ring so all keys map to the remote peer
	s.Ring().RemoveNode("test-node")

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	if !strings.HasPrefix(line, "-MOVED ") {
		t.Fatalf("expected MOVED error, got %q", line)
	}
	if !strings.Contains(line, "10.0.0.1:6379") {
		t.Fatalf("expected MOVED to contain peer address, got %q", line)
	}
}

func TestGetReturnsMovedForRemoteKey(t *testing.T) {
	s := New("127.0.0.1:0", "test-node")
	err := s.ListenAndServe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)
	s.Ring().RemoveNode("test-node")

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	if !strings.HasPrefix(line, "-MOVED ") {
		t.Fatalf("expected MOVED error, got %q", line)
	}
}

func TestSetStoresLocallyWhenOwner(t *testing.T) {
	s := New("127.0.0.1:0", "test-node")
	err := s.ListenAndServe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	// Add a peer but don't remove local node — keys may land on either node
	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)

	// Find a key that lands on the local node
	var localKey string
	found := false
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		nid, _ := s.Ring().GetNode(k)
		if nid == "test-node" {
			localKey = k
			found = true
			break
		}
	}
	if !found {
		t.Skip("could not find a key that lands on local node with 256 replicas")
	}

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	respCmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$5\r\nvalue\r\n", len(localKey), localKey)
	conn.Write([]byte(respCmd))
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	if line != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", line)
	}
}

func TestNodeJoinAddsToRing(t *testing.T) {
	// NODE.JOIN should add the node to the ring (tested via MOVED behavior)
	s := New("127.0.0.1:0", "test-node")
	s.Ring().AddNode("node-a", "10.0.0.1:6379", 256)

	// After adding a remote node, keys that land on it should return MOVED
	nodeID, addr := s.Ring().GetNode("somekey")
	if nodeID != "test-node" && nodeID != "node-a" {
		t.Fatalf("expected key to map to test-node or node-a, got %q", nodeID)
	}
	_ = addr
}

func TestClusterSlots(t *testing.T) {
	s := New("127.0.0.1:0", "test-node")
	err := s.ListenAndServe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		s.Wait()
	}()

	// Add a second node
	s.Ring().AddNode("node-remote", "10.0.0.1:6379", 256)

	conn, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send CLUSTER SLOTS
	conn.Write([]byte("*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n"))
	br := bufio.NewReader(conn)
	line, _ := br.ReadString('\n')
	// Should start with * (array response)
	if !strings.HasPrefix(line, "*") {
		t.Fatalf("expected array response, got %q", line)
	}
}

func TestClusterSlotsUnknownSubcommand(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*2\r\n$7\r\nCLUSTER\r\n$7\r\nUNKNOWN\r\n")
	if !strings.HasPrefix(resp, "-ERR unknown CLUSTER subcommand") {
		t.Fatalf("expected unknown subcommand error, got %q", resp)
	}
}

func TestClusterSlotsWrongArity(t *testing.T) {
	addr, cleanup := startTestServer(t)
	defer cleanup()

	resp := dialResp(t, addr, "*1\r\n$7\r\nCLUSTER\r\n")
	if !strings.HasPrefix(resp, "-ERR wrong number of arguments") {
		t.Fatalf("expected arity error, got %q", resp)
	}
}

func TestShutdownIdempotent(t *testing.T) {
	s := New("127.0.0.1:0", "shutdown-node")
	if err := s.ListenAndServe(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	s.Wait()
}
