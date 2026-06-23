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

func TestGetReplicas(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 100)
	r.AddNode("node-b", "10.0.0.2:6380", 100)
	r.AddNode("node-c", "10.0.0.3:6379", 100)

	replicas := r.GetReplicas("mykey", 2)
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}
	if replicas[0].NodeID == "" || replicas[1].NodeID == "" {
		t.Fatal("expected non-empty node IDs")
	}
	if replicas[0].NodeID == replicas[1].NodeID {
		t.Fatal("expected two distinct replicas")
	}
}

func TestGetReplicasMoreThanNodes(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 100)

	replicas := r.GetReplicas("key", 5)
	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica (only 1 node in ring), got %d", len(replicas))
	}
}

func TestGetReplicasDeterministic(t *testing.T) {
	r1 := New("node-local")
	r1.AddNode("node-a", "10.0.0.1:6379", 50)
	r1.AddNode("node-b", "10.0.0.2:6380", 50)
	r1.AddNode("node-c", "10.0.0.3:6379", 50)

	r2 := New("node-local")
	r2.AddNode("node-a", "10.0.0.1:6379", 50)
	r2.AddNode("node-b", "10.0.0.2:6380", 50)
	r2.AddNode("node-c", "10.0.0.3:6379", 50)

	for _, key := range []string{"k1", "k2", "hello", "world"} {
		r1r := r1.GetReplicas(key, 2)
		r2r := r2.GetReplicas(key, 2)
		if len(r1r) != len(r2r) {
			t.Fatalf("mismatched replica count for %q", key)
		}
		for i := range r1r {
			if r1r[i].NodeID != r2r[i].NodeID {
				t.Fatalf("mismatched replica %d for %q: %s vs %s", i, key, r1r[i].NodeID, r2r[i].NodeID)
			}
		}
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

func TestGetSlotRangesSingleNode(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 256)

	ranges := r.GetSlotRanges()
	if len(ranges) == 0 {
		t.Fatal("expected at least 1 range")
	}
	if ranges[0].Start != 0 {
		t.Fatalf("expected first range start 0, got %d", ranges[0].Start)
	}
	if ranges[0].End != 16383 {
		t.Fatalf("expected last range end 16383, got %d", ranges[0].End)
	}
	if ranges[0].Node.NodeID != "node-a" {
		t.Fatalf("expected node-a, got %s", ranges[0].Node.NodeID)
	}
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range for single node, got %d", len(ranges))
	}
}

func TestGetSlotRangesTwoNodes(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 256)
	r.AddNode("node-b", "10.0.0.2:6380", 256)

	ranges := r.GetSlotRanges()
	if len(ranges) < 2 {
		t.Fatalf("expected at least 2 ranges for 2 nodes, got %d", len(ranges))
	}

	// Verify all 16384 slots are covered
	covered := 0
	nodeIDs := make(map[string]int)
	for _, sr := range ranges {
		if sr.End < sr.Start {
			t.Fatalf("invalid range: %d-%d", sr.Start, sr.End)
		}
		covered += sr.End - sr.Start + 1
		nodeIDs[sr.Node.NodeID]++
	}
	if covered != 16384 {
		t.Fatalf("expected 16384 slots covered, got %d", covered)
	}
	// Both nodes should appear
	if len(nodeIDs) != 2 {
		t.Fatalf("expected 2 distinct nodes, got %d: %v", len(nodeIDs), nodeIDs)
	}
}

func TestGetSlotRangesEmptyRing(t *testing.T) {
	r := New("node-local")
	ranges := r.GetSlotRanges()
	if ranges != nil {
		t.Fatalf("expected nil for empty ring, got %v", ranges)
	}
}

func TestGetSlotRangesAfterRemove(t *testing.T) {
	r := New("node-local")
	r.AddNode("node-a", "10.0.0.1:6379", 256)
	r.AddNode("node-b", "10.0.0.2:6380", 256)
	r.RemoveNode("node-b")

	ranges := r.GetSlotRanges()
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range after removing node-b, got %d", len(ranges))
	}
	if ranges[0].Node.NodeID != "node-a" {
		t.Fatalf("expected node-a, got %s", ranges[0].Node.NodeID)
	}
}
