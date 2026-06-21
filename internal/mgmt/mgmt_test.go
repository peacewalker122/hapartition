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

	"github.com/peacewalker122/hapartition/internal/gossip"
	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// startTestMgmt creates a mgmt server with a minimal gossip handler for testing.
func startTestMgmt(t *testing.T) (baseURL string, cleanup func()) {
	t.Helper()

	st := store.New()
	ring := hashring.New("test-node")
	ring.AddNode("test-node", "127.0.0.1:6379", 256)

	cfg := gossip.Config{
		NodeID:         "test-node",
		BindAddr:       "127.0.0.1",
		BindPort:       0, // random port
		RedisAddr:      "127.0.0.1:6379",
		Store:          st,
		Ring:           ring,
		ReplicaRF:      2,
		AntiEntropySec: 0, // disable anti-entropy in tests
	}
	g := gossip.New(cfg)
	if err := g.Start(); err != nil {
		t.Fatalf("gossip start: %v", err)
	}

	s := New("127.0.0.1:0", g)
	err := s.ListenAndServe()
	if err != nil {
		t.Fatalf("mgmt listen: %v", err)
	}

	cleanup = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
		g.Leave(time.Second)
	}

	return fmt.Sprintf("http://%s", s.Addr()), cleanup
}

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
	baseURL, cleanup := startTestMgmt(t)
	defer cleanup()

	resp := httpPost(t, baseURL+"/join", `{"address":"10.0.0.1:7946"}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
	}
}

func TestJoinBadRequest(t *testing.T) {
	baseURL, cleanup := startTestMgmt(t)
	defer cleanup()

	resp := httpPost(t, baseURL+"/join", `{"address":""}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestInfoEndpoint(t *testing.T) {
	baseURL, cleanup := startTestMgmt(t)
	defer cleanup()

	resp := httpGet(t, baseURL+"/info")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
	}

	body := readBody(t, resp)
	var info infoResponse
	if err := json.Unmarshal([]byte(body), &info); err != nil {
		t.Fatalf("unmarshal: %v\nbody: %s", err, body)
	}
	if info.NodeID != "test-node" {
		t.Fatalf("expected node_id test-node, got %s", info.NodeID)
	}
}

func TestInfoEndpointMethodCheck(t *testing.T) {
	baseURL, cleanup := startTestMgmt(t)
	defer cleanup()

	resp, err := http.Post(baseURL+"/info", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /info: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestJoinMethodCheck(t *testing.T) {
	baseURL, cleanup := startTestMgmt(t)
	defer cleanup()

	resp, err := http.Get(baseURL + "/join")
	if err != nil {
		t.Fatalf("GET /join: %v", err)
	}
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
