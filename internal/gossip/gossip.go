package gossip

import (
	"crypto/tls"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
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

// DefaultReplicateTimeout is the timeout for waiting for ACKs from replicas
// during a quorum write.
const DefaultReplicateTimeout = 2 * time.Second

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
	WriteConcern   int               // minimum replicas that must ACK a write (1 to ReplicaRF)
	Discoverer     Discoverer        // seed discovery
	AntiEntropySec int               // anti-entropy interval in seconds (default 30)
	TLSConfig      *tls.Config       // optional mTLS config
	SecretKey      []byte
	Transport      GossipTransport   // transport impl (nil = memberlist)
	Clock          Clock             // clock impl (nil = wall clock)
}

type ackResult struct {
	nodeID string
	err    error
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

	pendingMu      sync.Mutex
	pendingReqs    map[string]chan ackResult
	antiEntropyMu  sync.Mutex
	antiEntropyPos int
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
	transport := cfg.Transport
	if transport == nil {
		transport = NewMemberlistTransport()
	}

	clock := cfg.Clock
	if clock == nil {
		clock = wallClock{}
	}

	// Default write concern: write to all replicas.
	if cfg.WriteConcern <= 0 || cfg.WriteConcern > cfg.ReplicaRF {
		cfg.WriteConcern = cfg.ReplicaRF
	}

	return &Handler{
		cfg:         cfg,
		transport:   transport,
		clock:       clock,
		stopCh:      make(chan struct{}),
		events:      newEventRing(eventRingSize),
		subscribers: make(map[int]chan ClusterEvent),
		pendingReqs: make(map[string]chan ackResult),
	}
}

// Start initialises the transport and joins the cluster.
func (h *Handler) Start() error {
	if h.cfg.AntiEntropySec <= 0 {
		h.cfg.AntiEntropySec = 30
	}

	h.transport.SetHandlers(TransportHandlers{
		OnMessage:         h.handleMessage,
		OnJoin:            h.handleJoin,
		OnLeave:           h.handleLeave,
		OnUpdate:          h.handleUpdate,
		OnLocalState:      h.handleLocalState,
		OnMergeRemoteState: h.handleMergeRemoteState,
	})

	tCfg := TransportConfig{
		NodeID:    h.cfg.NodeID,
		BindAddr:  h.cfg.BindAddr,
		BindPort:  h.cfg.BindPort,
		RedisAddr: h.cfg.RedisAddr,
		TLSConfig: h.cfg.TLSConfig,
		SecretKey: h.cfg.SecretKey,
	}
	if err := h.transport.Start(tCfg); err != nil {
		return err
	}

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
		go h.retryJoin(seeds)
	}

	go h.antiEntropyLoop()
	return nil
}

// --- Transport callbacks ---

func (h *Handler) handleMessage(from string, buf []byte) {
	var msg pb.GossipMessage
	if err := proto.Unmarshal(buf, &msg); err != nil {
		log.Printf("gossip: unmarshal message: %v", err)
		return
	}

	switch p := msg.Payload.(type) {
	case *pb.GossipMessage_EntryBatch:
		h.HandleReplication(p.EntryBatch.Entries)
	case *pb.GossipMessage_ReplicateReq:
		h.handleReplicateRequest(p.ReplicateReq)
	case *pb.GossipMessage_ReplicateResp:
		h.handleReplicateResponse(p.ReplicateResp)
	default:
		log.Printf("gossip: unknown message payload type")
	}
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

// --- Quorum write ---

// ReplicateWithQuorum writes the key locally and synchronously replicates to
// enough remote replicas to satisfy w (write concern). Returns nil on success
// or an error if quorum cannot be reached within the timeout.
// w must be in [1, ReplicaRF].
func (h *Handler) ReplicateWithQuorum(key string, entry store.Entry, w int) error {
	// Always apply locally first.
	h.cfg.Store.SetWithVersion(key, entry)

	if w <= 1 {
		return nil
	}

	replicas := h.cfg.Ring.GetReplicas(key, h.cfg.ReplicaRF)
	// Collect remote nodes.
	var remoteNodes []hashring.Replica
	for _, replica := range replicas {
		if replica.NodeID != h.NodeID() {
			remoteNodes = append(remoteNodes, replica)
		}
	}
	if w > len(remoteNodes)+1 {
		return fmt.Errorf("quorum not reachable: %d replicas available, need %d", len(remoteNodes)+1, w)
	}

	requestID := uuid.New().String()
	// Build request with correlation ID.
	req := &pb.GossipMessage{
		Payload: &pb.GossipMessage_ReplicateReq{
			ReplicateReq: &pb.ReplicateRequest{
				RequestId: requestID,
				SenderId:  h.NodeID(),
				Entries: []*pb.Entry{{
					Key: key, Value: entry.Value, Version: entry.Version,
					Origin: entry.Origin, Deleted: entry.Deleted,
				}},
			},
		},
	}
	reqBuf, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal replicate request: %w", err)
	}

	ch := make(chan ackResult, len(remoteNodes))
	// Correlate ACKs.
	h.pendingMu.Lock()
	h.pendingReqs[requestID] = ch
	h.pendingMu.Unlock()
	defer func() {
		h.pendingMu.Lock()
		delete(h.pendingReqs, requestID)
		h.pendingMu.Unlock()
	}()

	sent := 0
	// Send to each remote replica.
	expected := make(map[string]struct{}, len(remoteNodes))
	for _, replica := range remoteNodes {
		if err := h.transport.SendReliable(replica.NodeID, reqBuf); err != nil {
			log.Printf("gossip: replicate to %s: %v", replica.NodeID, err)
			continue
		}
		expected[replica.NodeID] = struct{}{}
		sent++
	}
	if sent+1 < w {
		return fmt.Errorf("quorum not reachable: %d replicas reachable, need %d", sent+1, w)
	}

	acks := 1
	// Wait for enough ACKs (including local).
	acked := make(map[string]struct{}, sent)
	deadline := time.After(DefaultReplicateTimeout)
	for acks < w {
		select {
		case res := <-ch:
			if _, valid := expected[res.nodeID]; valid && res.err == nil {
				if _, duplicate := acked[res.nodeID]; !duplicate {
					acked[res.nodeID] = struct{}{}
					acks++
				}
			}
		case <-deadline:
			return fmt.Errorf("quorum not reached: %d/%d ACKs after %v", acks, w, DefaultReplicateTimeout)
		}
	}
	return nil
}

func (h *Handler) Join(address string) error {
	_, err := h.transport.Join([]string{address})
	return err
}

// --- Replication handlers ---

// handleReplicateRequest applies entries from a quorum write request and
// sends an ACK back to the sender.
func (h *Handler) handleReplicateRequest(req *pb.ReplicateRequest) {
	success := true
	for _, entry := range req.Entries {
		replicas := h.cfg.Ring.GetReplicas(entry.Key, h.cfg.ReplicaRF)
		isReplica := false
		for _, replica := range replicas {
			if replica.NodeID == h.NodeID() {
				isReplica = true
				break
			}
		}
		if !isReplica {
			success = false
			break
		}
		h.cfg.Store.SetWithVersion(entry.Key, store.Entry{
			Value: entry.Value, Version: entry.Version, Origin: entry.Origin, Deleted: entry.Deleted,
		})
	}

	resp := &pb.GossipMessage{
		Payload: &pb.GossipMessage_ReplicateResp{
			ReplicateResp: &pb.ReplicateResponse{
				RequestId: req.RequestId, NodeId: h.NodeID(), Success: success,
			},
		},
	}
	respBuf, err := proto.Marshal(resp)
	if err != nil {
		log.Printf("gossip: marshal ack: %v", err)
		return
	}
	if err := h.transport.SendReliable(req.SenderId, respBuf); err != nil {
		log.Printf("gossip: send ack to %s: %v", req.SenderId, err)
	}
}

// handleReplicateResponse routes an ACK to the waiting goroutine.
func (h *Handler) handleReplicateResponse(resp *pb.ReplicateResponse) {
	h.pendingMu.Lock()
	ch, ok := h.pendingReqs[resp.RequestId]
	h.pendingMu.Unlock()
	if !ok {
		return
	}
	var err error
	if !resp.Success {
		err = fmt.Errorf("replica %s rejected write", resp.NodeId)
	}
	select {
	case ch <- ackResult{nodeID: resp.NodeId, err: err}:
	default:
		// channel full (already timed out or duplicate ACK)
	}
}

// HandleReplication applies a batch of entries to the local store if this
// node is in the ring's replica set for the key. Public for test access.
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
		// ponytail: fetch version again to avoid TOCTOU; SetWithVersion
		// internally does the LWW check under the write lock.
		h.cfg.Store.SetWithVersion(e.Key, store.Entry{
			Value: e.Value, Version: e.Version, Origin: e.Origin, Deleted: e.Deleted,
		})
	}
}

// --- Anti-entropy ---

func (h *Handler) antiEntropyLoop() {
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

	var peers []MemberInfo
	for _, m := range members {
		if m.Name != h.NodeID() {
			peers = append(peers, m)
		}
	}
	if len(peers) == 0 {
		return
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	h.antiEntropyMu.Lock()
	index := h.antiEntropyPos % len(peers)
	peer := peers[index]
	h.antiEntropyPos = (index + 1) % len(peers)
	h.antiEntropyMu.Unlock()
	snapshotBuf, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("gossip: marshal snapshot: %v", err)
		return
	}

	if err := h.transport.SendReliable(peer.Name, snapshotBuf); err != nil {
		log.Printf("gossip: anti-entropy send to %s: %v", peer.Name, err)
	}
}

func (h *Handler) serializeStore() *pb.GossipMessage {
	snap := h.cfg.Store.Snapshot()
	if len(snap) == 0 {
		return nil
	}
	entries := make([]*pb.Entry, 0, len(snap))
	for k, e := range snap {
		entries = append(entries, &pb.Entry{
			Key: k, Value: e.Value, Version: e.Version, Origin: e.Origin, Deleted: e.Deleted,
		})
	}
	return &pb.GossipMessage{
		Payload: &pb.GossipMessage_EntryBatch{
			EntryBatch: &pb.EntryBatch{Entries: entries},
		},
	}
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
	var msg pb.GossipMessage
	if err := proto.Unmarshal(buf, &msg); err != nil {
		log.Printf("gossip: unmarshal remote state: %v", err)
		return
	}
	batch := msg.GetEntryBatch()
	if batch == nil {
		return
	}
	h.HandleReplication(batch.Entries)
	log.Printf("gossip: merged %d entries from anti-entropy", len(batch.Entries))
}

// --- retry join ---

func (h *Handler) retryJoin(seeds []string) {
	if len(seeds) == 0 {
		return
	}
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

// NodeID returns this node's ID.
func (h *Handler) NodeID() string {
	return h.transport.LocalNodeID()
}

// Leave gracefully leaves the cluster.
func (h *Handler) Leave(timeout time.Duration) error {
	close(h.stopCh)
	return h.transport.Leave(timeout)
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
