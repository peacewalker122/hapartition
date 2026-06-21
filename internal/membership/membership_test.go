package membership

import (
	"testing"
	"time"
)

func TestJoinAndGet(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")

	p, ok := m.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if p.NodeID != "node-a" {
		t.Fatalf("expected node-a, got %s", p.NodeID)
	}
	if p.Address != "10.0.0.1:6379" {
		t.Fatalf("expected 10.0.0.1:6379, got %s", p.Address)
	}
	if p.Status != StatusHealthy {
		t.Fatalf("expected healthy, got %s", p.Status)
	}
	if p.LastSeen.IsZero() {
		t.Fatal("expected last_seen to be set")
	}
}

func TestJoinUpdatesExisting(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")

	// Wait a tiny bit so last_seen changes
	time.Sleep(time.Millisecond)
	m.Join("node-a", "10.0.0.2:6379")

	p, _ := m.Get("node-a")
	if p.Address != "10.0.0.2:6379" {
		t.Fatalf("expected updated address, got %s", p.Address)
	}
}

func TestPing(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")

	// Mark suspected then ping back
	m.Suspect("node-a")
	m.Ping("node-a")

	p, _ := m.Get("node-a")
	if p.Status != StatusHealthy {
		t.Fatalf("expected healthy after ping, got %s", p.Status)
	}
}

func TestPingUnknown(t *testing.T) {
	m := New("node-local")
	ok := m.Ping("nonexistent")
	if ok {
		t.Fatal("expected false for unknown peer")
	}
}

func TestSuspectAndDead(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")

	m.Suspect("node-a")
	p, _ := m.Get("node-a")
	if p.Status != StatusSuspected {
		t.Fatalf("expected suspected, got %s", p.Status)
	}

	m.MarkDead("node-a")
	p, _ = m.Get("node-a")
	if p.Status != StatusDead {
		t.Fatalf("expected dead, got %s", p.Status)
	}
}

func TestRemove(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")

	removed := m.Remove("node-a")
	if !removed {
		t.Fatal("expected remove to return true")
	}
	_, ok := m.Get("node-a")
	if ok {
		t.Fatal("expected node-a to be gone")
	}

	removed = m.Remove("node-a")
	if removed {
		t.Fatal("expected remove of missing node to return false")
	}
}

func TestPeers(t *testing.T) {
	m := New("node-local")
	m.Join("node-a", "10.0.0.1:6379")
	m.Join("node-b", "10.0.0.2:6379")
	m.Join("node-c", "10.0.0.3:6379")

	peers := m.Peers()
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d", len(peers))
	}
}

func TestCount(t *testing.T) {
	m := New("node-local")
	if m.Count() != 0 {
		t.Fatalf("expected 0, got %d", m.Count())
	}
	m.Join("node-a", "10.0.0.1:6379")
	if m.Count() != 1 {
		t.Fatalf("expected 1, got %d", m.Count())
	}
}

func TestNodeID(t *testing.T) {
	m := New("my-hostname")
	if m.NodeID() != "my-hostname" {
		t.Fatalf("expected my-hostname, got %s", m.NodeID())
	}
}

func TestParseStatus(t *testing.T) {
	tests := []struct {
		input string
		want  Status
		err   bool
	}{
		{"healthy", StatusHealthy, false},
		{"suspected", StatusSuspected, false},
		{"dead", StatusDead, false},
		{"unknown", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseStatus(tt.input)
			if tt.err {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %s, got %s", tt.want, got)
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	m := New("node-local")
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			m.Join("node-a", "10.0.0.1:6379")
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			m.Ping("node-a")
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			m.Peers()
		}
		done <- struct{}{}
	}()
	for i := 0; i < 3; i++ {
		<-done
	}
}
