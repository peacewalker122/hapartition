package gossip

import (
	"crypto/tls"
	"log"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/peacewalker122/hapartition/internal/gossip/pb"
	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// Discoverer finds seed nodes for memberlist to join.
type Discoverer interface {
	// Discover returns gossip addresses (host:port) to join.
	Discover() ([]string, error)
}

const eventRingSize = 200

// ClusterEvent is a cluster membership change recorded in the event log.
type ClusterEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`      // join, leave, update
	NodeName  string    `json:"node_name"`
	Address   string    `json:"address"`
	RedisAddr string    `json:"redis_addr"`
	Meta      string    `json:"meta,omitempty"`
}

// eventRing is a fixed-size circular buffer of cluster events.
type eventRing struct {
	mu    sync.RWMutex
	buf   []ClusterEvent
	pos   int
	count int
}

func newEventRing(capacity int) *eventRing {
	return &eventRing{buf: make([]ClusterEvent, capacity)}
}

func (r *eventRing) Push(e ClusterEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.pos] = e
	r.pos = (r.pos + 1) % len(r.buf)
	if r.count < len(r.buf) {
		r.count++
	}
}

func (r *eventRing) Recent(n int) []ClusterEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n <= 0 || n > r.count {
		n = r.count
	}
	out := make([]ClusterEvent, n)
	start := r.pos - n
	if start < 0 {
		// wrapped
		part := copy(out, r.buf[len(r.buf)+start:])
		copy(out[part:], r.buf[:r.pos])
	} else {
		copy(out, r.buf[start:r.pos])
	}
	return out
}

// Config for the gossip layer.
type Config struct {
	NodeID         string            // unique node ID
	BindAddr       string            // gossip bind address (e.g. "0.0.0.0")
	BindPort       int               // gossip bind port
	AdvertiseAddr  string            // optional, for NAT
	RedisAddr      string            // Redis TCP address, stored in node Meta
	Store          *store.Store      // shared store reference
	Ring           hashring.Hashring // shared hashring reference
	ReplicaRF      int               // replication factor (number of replicas per key)
	Discoverer     Discoverer        // seed discovery
	AntiEntropySec int               // anti-entropy interval in seconds (default 30)
	TLSConfig      *tls.Config       // optional mTLS config
	Transport      GossipTransport   // transport impl (nil = memberlist)
	Clock          Clock             // clock impl (nil = wall clock)
}

// Handler wraps a GossipTransport and implements data replication via gossip.
type Handler struct {
	cfg        Config
	transport  GossipTransport
	clock      Clock
	stopCh     chan struct{}

	events    *eventRing
	subMu     sync.RWMutex
	subNext   int
	subscribers map[int]chan ClusterEvent
}

// PushEvent records an event in the ring buffer and fans it out to SSE subscribers.
func (h *Handler) PushEvent(e ClusterEvent) {
	h.events.Push(e)
	h.subMu.RLock()
	for _, ch := range h.subscribers {
		select {
		case ch <- e:
		default:
			// drop if subscriber is slow
		}
	}
	h.subMu.RUnlock()
}

// Subscribe adds an SSE subscriber channel. Returns an unsubscribe func.
func (h *Handler) Subscribe(ch chan ClusterEvent) func() {
	h.subMu.Lock()
	id := h.subNext
	h.subNext++
	h.subscribers[id] = ch
	h.subMu.Unlock()
	return func() {
		h.subMu.Lock()
		delete(h.subscribers, id)
		h.subMu.Unlock()
	}
}

// RecentEvents returns the last n cluster events.
func (h *Handler) RecentEvents(n int) []ClusterEvent {
	return h.events.Recent(n)
}

// New creates a gossip handler. Call Start() to begin.
func New(cfg Config) *Handler {
	// Default transport is memberlist if none set.
	transport := cfg.Transport
	if transport == nil {
		transport = NewMemberlistTransport()
	}

	clock := cfg.Clock
	if clock == nil {
		clock = wallClock{}
	}

	return &Handler{
		cfg:         cfg,
		transport:   transport,
		clock:       clock,
		stopCh:      make(chan struct{}),
		events:      newEventRing(eventRingSize),
		subscribers: make(map[int]chan ClusterEvent),
	}
}

// Start initialises the transport and joins the cluster.
func (h *Handler) Start() error {
	if h.cfg.AntiEntropySec <= 0 {
		h.cfg.AntiEntropySec = 30
	}

	// Set up callbacks from transport → gossip handler
	h.transport.SetHandlers(TransportHandlers{
		OnMessage:         h.handleMessage,
		OnJoin:            h.handleJoin,
		OnLeave:           h.handleLeave,
		OnUpdate:          h.handleUpdate,
		OnLocalState:      h.handleLocalState,
		OnMergeRemoteState: h.handleMergeRemoteState,
	})

	// Start the transport (binds ports, creates memberlist, etc.)
	tCfg := TransportConfig{
		NodeID:    h.cfg.NodeID,
		BindAddr:  h.cfg.BindAddr,
		BindPort:  h.cfg.BindPort,
		RedisAddr: h.cfg.RedisAddr,
		TLSConfig: h.cfg.TLSConfig,
	}
	if err := h.transport.Start(tCfg); err != nil {
		return err
	}

	// Join the cluster via discoverer
	if h.cfg.Discoverer != nil {
		seeds, err := h.cfg.Discoverer.Discover()
		if err != nil {
			log.Printf("gossip: discover seeds: %v (will retry in background)", err)
		} else if len(seeds) > 0 {
			_, err := h.transport.Join(seeds)
			if err != nil {
				log.Printf("gossip: initial join %v: %v (will retry in background)", seeds, err)
			} else {
				log.Printf("gossip: joined cluster")
			}
		}
		// Retry join in background until we see at least one peer.
		go h.retryJoin(seeds)
	}

	// Start anti-entropy loop
	go h.antiEntropyLoop()

	return nil
}

// --- transport callbacks ---

func (h *Handler) handleMessage(from string, buf []byte) {
	var batch pb.EntryBatch
	if err := proto.Unmarshal(buf, &batch); err != nil {
		log.Printf("gossip: unmarshal message: %v", err)
		return
	}
	h.HandleReplication(batch.Entries)
}

func (h *Handler) handleJoin(nodeID, redisAddr string) {
	h.cfg.Ring.AddNode(nodeID, redisAddr, 256)
	h.PushEvent(ClusterEvent{
		Timestamp: h.clock.Now(),
		Type:      "join",
		NodeName:  nodeID,
		RedisAddr: redisAddr,
	})
	log.Printf("gossip: node joined: %s (%s)", nodeID, redisAddr)
}

func (h *Handler) handleLeave(nodeID string) {
	h.cfg.Ring.RemoveNode(nodeID)
	h.PushEvent(ClusterEvent{
		Timestamp: h.clock.Now(),
		Type:      "leave",
		NodeName:  nodeID,
	})
	log.Printf("gossip: node left: %s", nodeID)
}

func (h *Handler) handleUpdate(nodeID, redisAddr string) {
	h.PushEvent(ClusterEvent{
		Timestamp: h.clock.Now(),
		Type:      "update",
		NodeName:  nodeID,
		RedisAddr: redisAddr,
	})
}

func (h *Handler) handleLocalState() []byte {
	snapshot := h.serializeStore()
	if snapshot == nil {
		return nil
	}
	buf, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("gossip: marshal local state: %v", err)
		return nil
	}
	return buf
}

func (h *Handler) handleMergeRemoteState(buf []byte) {
	var snapshot pb.EntryBatch
	if err := proto.Unmarshal(buf, &snapshot); err != nil {
		log.Printf("gossip: unmarshal remote state: %v", err)
		return
	}
	for _, e := range snapshot.Entries {
		h.cfg.Store.SetWithVersion(e.Key, e.Value, e.Version)
	}
	log.Printf("gossip: merged %d entries from anti-entropy", len(snapshot.Entries))
}

// --- retry join ---

func (h *Handler) retryJoin(seeds []string) {
	if len(seeds) == 0 {
		return
	}
	// If we already have peers on first try, nothing to do.
	if len(h.transport.Members()) > 1 {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			if len(h.transport.Members()) > 1 {
				return
			}
			seeds, err := h.cfg.Discoverer.Discover()
			if err != nil {
				log.Printf("gossip: retry discover: %v", err)
				continue
			}
			if len(seeds) == 0 {
				continue
			}
			_, err = h.transport.Join(seeds)
			if err == nil {
				log.Printf("gossip: joined cluster via retry")
				return
			}
		}
	}
}

// Broadcast asynchronously sends a key write to the cluster.
func (h *Handler) Broadcast(key, value string, version int64) {
	entry := &pb.Entry{Key: key, Value: value, Version: version}
	batch := &pb.EntryBatch{Entries: []*pb.Entry{entry}}
	buf, err := proto.Marshal(batch)
	if err != nil {
		log.Printf("gossip: marshal broadcast: %v", err)
		return
	}
	h.transport.Broadcast(buf, key)
}

// NodeID returns this node's ID.
func (h *Handler) NodeID() string {
	return h.transport.LocalNodeID()
}

// Leave gracefully leaves the cluster.
func (h *Handler) Leave(timeout time.Duration) error {
	close(h.stopCh)
	return h.transport.Leave(timeout)
}

// HandleReplication is called when the server receives a replication message.
// Public so the server can expose it via tests.
func (h *Handler) HandleReplication(entries []*pb.Entry) {
	for _, e := range entries {
		replicas := h.cfg.Ring.GetReplicas(e.Key, h.cfg.ReplicaRF)
		isReplica := false
		for _, r := range replicas {
			if r.NodeID == h.NodeID() {
				isReplica = true
				break
			}
		}
		if !isReplica {
			continue
		}
		h.cfg.Store.SetWithVersion(e.Key, e.Value, e.Version)
	}
}

// --- anti-entropy ---

func (h *Handler) antiEntropyLoop() {
	// Re-arm After each tick — works for both wallClock (time.After creates a
	// new timer) and ManualClock (channel fires on Advance).
	for {
		select {
		case <-h.stopCh:
			return
		case <-h.clock.After(time.Duration(h.cfg.AntiEntropySec) * time.Second):
			h.exchangeState()
		}
	}
}

func (h *Handler) exchangeState() {
	snapshot := h.serializeStore()
	if snapshot == nil {
		return
	}

	members := h.transport.Members()
	if len(members) <= 1 {
		return
	}

	// Exclude self
	var peers []MemberInfo
	for _, m := range members {
		if m.Name != h.NodeID() {
			peers = append(peers, m)
		}
	}
	if len(peers) == 0 {
		return
	}

	// Pick the first peer for this round
	peer := peers[0]
	snapshotBuf, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("gossip: marshal snapshot: %v", err)
		return
	}

	if err := h.transport.SendReliable(peer.Name, snapshotBuf); err != nil {
		log.Printf("gossip: anti-entropy send to %s: %v", peer.Name, err)
	}
}

func (h *Handler) serializeStore() *pb.EntryBatch {
	snapshot := h.cfg.Store.Snapshot()
	if len(snapshot) == 0 {
		return nil
	}
	entries := make([]*pb.Entry, 0, len(snapshot))
	for k, e := range snapshot {
		entries = append(entries, &pb.Entry{
			Key:     k,
			Value:   e.Value,
			Version: e.Version,
		})
	}
	return &pb.EntryBatch{Entries: entries}
}

// KeyCount returns the number of keys in the local store.
func (h *Handler) KeyCount() int {
	return h.cfg.Store.Len()
}

// Ring returns the shared hashring.
func (h *Handler) Ring() hashring.Hashring {
	return h.cfg.Ring
}

// MembersInfo returns all cluster members with their decoded Redis addresses.
func (h *Handler) MembersInfo() []MemberInfo {
	return h.transport.Members()
}

// DecodeMetaRedisAddr extracts the Redis address from memberlist node metadata.
func DecodeMetaRedisAddr(meta []byte) string {
	if len(meta) == 0 {
		return ""
	}
	var nm pb.NodeMeta
	if err := proto.Unmarshal(meta, &nm); err != nil {
		return ""
	}
	return nm.RedisAddr
}
