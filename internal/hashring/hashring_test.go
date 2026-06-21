package hashring

import (
	"testing"
)

func TestNew(t *testing.T) {
	r := New("node-local")
	if r == nil {
		t.Fatal("expected non-nil ring")
	}
}

func TestAddNode(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 10)

	nodes := r.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0] != "node-a" {
		t.Fatalf("expected node-a, got %s", nodes[0])
	}
}

func TestRemoveNode(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 10)
	r.AddNode("node-b", "10.0.0.2:6380", 10)

	r.RemoveNode("node-a")
	nodes := r.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after remove, got %d", len(nodes))
	}
	if nodes[0] != "node-b" {
		t.Fatalf("expected node-b, got %s", nodes[0])
	}
}

func TestGetNode(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 10)

	nodeID, addr := r.GetNode("mykey")
	if nodeID == "" {
		t.Fatal("expected a node for mykey, got empty")
	}
	if nodeID != "node-a" {
		t.Fatalf("expected node-a, got %s", nodeID)
	}
	if addr != "10.0.0.1:6379" {
		t.Fatalf("expected 10.0.0.1:6379, got %s", addr)
	}
}

func TestGetNodeEmptyRing(t *testing.T) {
	r := New("node-local")
	nodeID, addr := r.GetNode("mykey")
	if nodeID != "" || addr != "" {
		t.Fatalf("expected empty for empty ring, got %s/%s", nodeID, addr)
	}
}

func TestKeyDistribution(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 100)
	r.AddNode("node-b", "10.0.0.2:6380", 100)

	// Count assignments across many keys
	const n = 10000
	counts := make(map[string]int)
	for i := 0; i < n; i++ {
		nid, _ := r.GetNode("key:" + string(rune('a'+i%26)) + ":" + string(rune('0'+i%10)))
		counts[nid]++
	}

	if len(counts) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(counts))
	}

	// Each node should have roughly half
	for nid, c := range counts {
		pct := float64(c) / float64(n) * 100
		t.Logf("  %s: %d (%.1f%%)", nid, c, pct)
	}

	// Allow 30% imbalance — 100 replicas isn't perfectly even
	total := counts["node-a"] + counts["node-b"]
	nodeAPct := float64(counts["node-a"]) / float64(total)
	if nodeAPct < 0.2 || nodeAPct > 0.8 {
		t.Fatalf("distribution too unbalanced: node-a has %.1f%%", nodeAPct*100)
	}
}

func TestDeterministic(t *testing.T) {
	r1 := New("node-local")
	r1.AddNode("node-a", "10.0.0.1:6379", 50)
	r1.AddNode("node-b", "10.0.0.2:6380", 50)

	r2 := New("node-local")
	r2.AddNode("node-a", "10.0.0.1:6379", 50)
	r2.AddNode("node-b", "10.0.0.2:6380", 50)

	for _, key := range []string{"key1", "key2", "key3", "hello", "world", "test"} {
		nid1, addr1 := r1.GetNode(key)
		nid2, addr2 := r2.GetNode(key)
		if nid1 != nid2 || addr1 != addr2 {
			t.Fatalf("inconsistent for %q: (%s,%s) vs (%s,%s)",
				key, nid1, addr1, nid2, addr2)
		}
	}
}

func TestAddNodeUpdate(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 10)

	// Re-add same node ID with different address — should replace old entries
	r.AddNode("node-a", "10.0.0.1:9999", 10)

	nid, addr := r.GetNode("mykey")
	if nid != "node-a" {
		t.Fatalf("expected node-a, got %s", nid)
	}
	if addr != "10.0.0.1:9999" {
		t.Fatalf("expected updated address, got %s", addr)
	}
}

func TestConcurrentAccess(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 50)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			r.AddNode("node-b", "10.0.0.2:6380", 50)
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			r.GetNode("key")
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			r.Nodes()
		}
		done <- struct{}{}
	}()
	for i := 0; i < 3; i++ {
		<-done
	}
}
