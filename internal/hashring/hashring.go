package hashring

import (
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// Replica holds the identity of a replica node.
type Replica struct {
	NodeID  string
	Address string
}

// RingEntryInfo exposes a virtual ring node for the dashboard.
type RingEntryInfo struct {
	Hash    uint64 `json:"hash"`
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// Hashring is the interface for consistent hash ring implementations.
type Hashring interface {
	// AddNode adds a physical node with the given number of virtual replicas.
	AddNode(nodeID, address string, replicas int)
	// RemoveNode removes a physical node and all its virtual replicas.
	RemoveNode(nodeID string)
	// GetNode returns the nodeID and address that owns the given key.
	// Returns ("", "") when the ring is empty.
	GetNode(key string) (nodeID, address string)
	// GetReplicas returns the first count distinct physical nodes that are
	// responsible for the given key. The first entry is the primary owner.
	// Returns fewer than count if the ring has fewer nodes.
	GetReplicas(key string, count int) []Replica
	// Nodes returns all physical node IDs currently in the ring.
	Nodes() []string
	// RingSnapshot returns every virtual entry on the ring, sorted by hash.
	// Used by the dashboard to render the ring visualization.
	RingSnapshot() []RingEntryInfo
}

// ringEntry is a single virtual node on the ring.
type ringEntry struct {
	hash    uint64
	nodeID  string
	address string
}

// consistentHashring implements Hashring using a Ketama-style consistent
// hash ring with virtual nodes.
type consistentHashring struct {
	mu           sync.RWMutex
	localNodeID  string
	replicas     int
	entries      []ringEntry // sorted by hash
	nodeReplicas map[string]int
	nodes        map[string]struct{}
}

// New creates a consistent hash ring. localNodeID identifies this node
// in the cluster, and defaultReplicas is the number of virtual replicas per
// physical node when AddNode doesn't specify one.
func New(localNodeID string) Hashring {
	return &consistentHashring{
		localNodeID:  localNodeID,
		replicas:     256,
		entries:      nil,
		nodeReplicas: make(map[string]int),
		nodes:        make(map[string]struct{}),
	}
}

func (r *consistentHashring) AddNode(nodeID, address string, replicas int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if replicas <= 0 {
		replicas = r.replicas
	}

	// Remove existing entries for this node first (update case)
	r.removeNodeLocked(nodeID)

	r.nodes[nodeID] = struct{}{}
	r.nodeReplicas[nodeID] = replicas

	for i := 0; i < replicas; i++ {
		h := hashKey(nodeID + ":" + strconv.Itoa(i))
		r.entries = append(r.entries, ringEntry{
			hash:    h,
			nodeID:  nodeID,
			address: address,
		})
	}

	sort.Slice(r.entries, func(i, j int) bool {
		return r.entries[i].hash < r.entries[j].hash
	})
}

func (r *consistentHashring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.removeNodeLocked(nodeID)
}

func (r *consistentHashring) removeNodeLocked(nodeID string) {
	delete(r.nodes, nodeID)
	delete(r.nodeReplicas, nodeID)

	// Filter out all entries belonging to this node
	filtered := r.entries[:0]
	for _, e := range r.entries {
		if e.nodeID != nodeID {
			filtered = append(filtered, e)
		}
	}
	r.entries = filtered
}

func (r *consistentHashring) GetNode(key string) (nodeID, address string) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 {
		return "", ""
	}

	h := hashKey(key)
	// Binary search for first entry with hash >= key hash
	idx := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].hash >= h
	})
	// Wrap around to the first entry if past the end
	if idx == len(r.entries) {
		idx = 0
	}
	e := r.entries[idx]
	return e.nodeID, e.address
}

func (r *consistentHashring) GetReplicas(key string, count int) []Replica {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.entries) == 0 || count <= 0 {
		return nil
	}

	h := hashKey(key)
	idx := sort.Search(len(r.entries), func(i int) bool {
		return r.entries[i].hash >= h
	})
	if idx == len(r.entries) {
		idx = 0
	}

	seen := make(map[string]bool, count)
	replicas := make([]Replica, 0, count)

	// Walk the ring clockwise from the key's position, collecting distinct
	// physical nodes until we have enough or hit the full ring.
	for i := 0; i < len(r.entries); i++ {
		e := r.entries[(idx+i)%len(r.entries)]
		if seen[e.nodeID] {
			continue
		}
		seen[e.nodeID] = true
		replicas = append(replicas, Replica{NodeID: e.nodeID, Address: e.address})
		if len(replicas) >= count {
			break
		}
	}

	return replicas
}

func (r *consistentHashring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		result = append(result, n)
	}
	sort.Strings(result)
	return result
}

func (r *consistentHashring) RingSnapshot() []RingEntryInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]RingEntryInfo, len(r.entries))
	for i, e := range r.entries {
		out[i] = RingEntryInfo{Hash: e.hash, NodeID: e.nodeID, Address: e.address}
	}
	return out
}

// hashKey returns a 64-bit hash of the given key using xxHash.
func hashKey(key string) uint64 {
	return xxhash.Sum64String(key)
}
