package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/peacewalker122/hapartition/internal/membership"
)

func startTestMgmt(t *testing.T) (addr string, m *membership.Membership, cleanup func()) {
	t.Helper()
	m = membership.New("test-node")
	s := New("127.0.0.1:0", m)

	// We need the actual bound address. Start with :0 to get a free port.
	err := s.ListenAndServe()
	if err != nil {
		t.Fatalf("mgmt listen: %v", err)
	}
	// The ListenAndServe binds to :0 and logs the address, but we don't
	// have access to the listener directly. Let me bind to a specific port.
	// Actually, we need to refactor to expose the address.
	// For now, let's use a fixed port or expose the addr.
	_ = s

	// Hmm, we don't have Addr() on mgmt.Server. Let me try a different approach.
	// Actually, let me just test via the ListenAndServe by hardcoding a port? No.
	// Let me close and restart with a different approach.
	s.Shutdown(context.Background())

	// Retry with :0 and use net.Listener to get port
	s2 := New("127.0.0.1:0", m)
	err = s2.ListenAndServe()
	if err != nil {
		t.Fatalf("mgmt listen: %v", err)
	}

	// Find the port from the listener... but we don't store it.
	// Let me add Addr() to mgmt.Server. For now a workaround.
	// Actually this is getting complicated. Let me just add an Addr method.
	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s2.Shutdown(ctx)
	}
	return "", m, cleanup
}

// Re-using the approach with a workaround: add Addr() to Server
func startMgmtServer(t *testing.T, m *membership.Membership) (baseURL string, cleanup func()) {
	t.Helper()
	s := New("127.0.0.1:0", m)
	err := s.ListenAndServe()
	if err != nil {
		t.Fatalf("mgmt listen: %v", err)
	}
	// Get the bound address via Addr()
	addr := s.Addr()

	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}
	return fmt.Sprintf("http://%s", addr), cleanup
}

// We need Addr() method on Server. Let me add it.
// Actually, I already wrote mgmt.go without Addr(). Let me add it.

func httpPost(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func httpGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func TestJoinEndpoint(t *testing.T) {
	m := membership.New("test-node")
	baseURL, cleanup := startMgmtServer(t, m)
	defer cleanup()

	resp := httpPost(t, baseURL+"/join", `{"node_id":"node-a","address":"10.0.0.1:6379"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
	}

	p, ok := m.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if p.Address != "10.0.0.1:6379" {
		t.Fatalf("expected address 10.0.0.1:6379, got %s", p.Address)
	}
}

func TestJoinBadRequest(t *testing.T) {
	m := membership.New("test-node")
	baseURL, cleanup := startMgmtServer(t, m)
	defer cleanup()

	// Missing fields
	resp := httpPost(t, baseURL+"/join", `{"node_id":""}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestHeartbeatEndpoint(t *testing.T) {
	m := membership.New("test-node")
	m.Join("node-a", "10.0.0.1:6379")

	baseURL, cleanup := startMgmtServer(t, m)
	defer cleanup()

	resp := httpPost(t, baseURL+"/heartbeat", `{"node_id":"node-a"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
	}

	// Unknown node
	resp = httpPost(t, baseURL+"/heartbeat", `{"node_id":"unknown"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestInfoEndpoint(t *testing.T) {
	m := membership.New("test-node")
	m.Join("node-a", "10.0.0.1:6379")
	m.Join("node-b", "10.0.0.2:6380")

	baseURL, cleanup := startMgmtServer(t, m)
	defer cleanup()

	resp := httpGet(t, baseURL+"/info")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body := readBody(t, resp)
	var info infoResponse
	if err := json.Unmarshal([]byte(body), &info); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, body)
	}
	if info.NodeID != "test-node" {
		t.Fatalf("expected node_id test-node, got %s", info.NodeID)
	}
	if len(info.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(info.Peers))
	}
}
