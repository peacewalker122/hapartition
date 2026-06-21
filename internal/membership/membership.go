package membership

import (
	"fmt"
	"sync"
	"time"
)

// Status represents the health state of a peer node.
type Status int

const (
	StatusHealthy   Status = iota // node is responding
	StatusSuspected               // node missed a heartbeat window
	StatusDead                    // node has been offline too long
)

var statusNames = map[Status]string{
	StatusHealthy:   "healthy",
	StatusSuspected: "suspected",
	StatusDead:      "dead",
}

func (s Status) String() string {
	if n, ok := statusNames[s]; ok {
		return n
	}
	return "unknown"
}

// ParseStatus converts a status string to a Status value.
func ParseStatus(s string) (Status, error) {
	for k, v := range statusNames {
		if v == s {
			return k, nil
		}
	}
	return 0, fmt.Errorf("unknown status: %s", s)
}

// Peer represents a known node in the cluster.
type Peer struct {
	NodeID   string    `json:"node_id"`
	Address  string    `json:"address"`
	Status   Status    `json:"status"`
	LastSeen time.Time `json:"last_seen"`
}

// Membership tracks the local node's view of cluster peers.
type Membership struct {
	mu     sync.RWMutex
	nodeID string
	peers  map[string]*Peer
}

// New creates a Membership for the local node identified by nodeID.
func New(nodeID string) *Membership {
	return &Membership{
		nodeID: nodeID,
		peers:  make(map[string]*Peer),
	}
}

// NodeID returns the local node's identifier.
func (m *Membership) NodeID() string {
	return m.nodeID
}

// Join registers or updates a peer. If the peer is new, status is set to
// healthy. If it already exists, status is reset to healthy and last_seen
// is refreshed.
func (m *Membership) Join(nodeID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.peers[nodeID]; ok {
		p.Address = address
		p.Status = StatusHealthy
		p.LastSeen = time.Now()
	} else {
		m.peers[nodeID] = &Peer{
			NodeID:   nodeID,
			Address:  address,
			Status:   StatusHealthy,
			LastSeen: time.Now(),
		}
	}
}

// Ping updates the last_seen timestamp for a peer. Returns false if the peer
// is not known.
func (m *Membership) Ping(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return false
	}
	p.LastSeen = time.Now()
	// Reset to healthy on ping — still alive
	if p.Status == StatusSuspected || p.Status == StatusDead {
		p.Status = StatusHealthy
	}
	return true
}

// Suspect marks a peer as suspected (missed heartbeat).
func (m *Membership) Suspect(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return false
	}
	if p.Status == StatusHealthy {
		p.Status = StatusSuspected
	}
	return true
}

// MarkDead marks a peer as dead.
func (m *Membership) MarkDead(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return false
	}
	p.Status = StatusDead
	return true
}

// Remove deletes a peer from the membership list entirely.
func (m *Membership) Remove(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.peers[nodeID]
	if ok {
		delete(m.peers, nodeID)
	}
	return ok
}

// Get returns a snapshot of a single peer. The bool is false if not found.
func (m *Membership) Get(nodeID string) (Peer, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.peers[nodeID]
	if !ok {
		return Peer{}, false
	}
	return *p, true
}

// Peers returns a snapshot of all known peers (excluding the local node).
func (m *Membership) Peers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]Peer, 0, len(m.peers))
	for _, p := range m.peers {
		result = append(result, *p)
	}
	return result
}

// Count returns the total number of tracked peers (excluding the local node).
func (m *Membership) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.peers)
}
