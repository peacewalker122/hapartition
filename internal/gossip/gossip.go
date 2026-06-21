package gossip

import (
	"fmt"
	"log"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/internal/gossip/pb"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// Discoverer finds seed nodes for memberlist to join.
type Discoverer interface {
	// Discover returns gossip addresses (host:port) to join.
	Discover() ([]string, error)
}

// Config for the gossip layer.
type Config struct {
	NodeID         string         // unique node ID (memberlist Name)
	BindAddr       string         // gossip bind address (e.g. "0.0.0.0")
	BindPort       int            // gossip bind port
	AdvertiseAddr  string         // optional, for NAT
	RedisAddr      string         // Redis TCP address, stored in node Meta
	Store          *store.Store   // shared store reference
	Ring           hashring.Hashring // shared hashring reference
	ReplicaRF      int            // replication factor (number of replicas per key)
	Discoverer     Discoverer     // seed discovery
	AntiEntropySec int            // anti-entropy interval in seconds (default 30)
}

// Handler wraps memberlist and implements the Delegate + EventDelegate for
// data replication via gossip.
type Handler struct {
	cfg         Config
	memberlist  *memberlist.Memberlist
	delegate    *gossipDelegate
	broadcasts  *memberlist.TransmitLimitedQueue
	stopCh      chan struct{}
}

// New creates a gossip handler. Call Start() to begin.
func New(cfg Config) *Handler {
	return &Handler{
		cfg: cfg,
		delegate: &gossipDelegate{},
		stopCh: make(chan struct{}),
	}
}

// Start initialises memberlist and joins the cluster.
func (h *Handler) Start() error {
	if h.cfg.AntiEntropySec <= 0 {
		h.cfg.AntiEntropySec = 30
	}

	h.delegate.handler = h

	cfg := memberlist.DefaultLANConfig()
	cfg.Name = h.cfg.NodeID
	cfg.BindAddr = h.cfg.BindAddr
	cfg.BindPort = h.cfg.BindPort
	if h.cfg.AdvertiseAddr != "" {
		cfg.AdvertiseAddr = h.cfg.AdvertiseAddr
	}
	cfg.Delegate = h.delegate
	cfg.Events = h.delegate
	cfg.LogOutput = log.Default().Writer()
	cfg.EnableCompression = true

	list, err := memberlist.Create(cfg)
	if err != nil {
		return fmt.Errorf("gossip: create memberlist: %w", err)
	}
	h.memberlist = list

	// Broadcast queue
	h.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int { return list.NumMembers() },
		RetransmitMult: 3,
	}

	// Join the cluster via discoverer
	if h.cfg.Discoverer != nil {
		seeds, err := h.cfg.Discoverer.Discover()
		if err != nil {
			log.Printf("gossip: discover seeds: %v (continuing without join)", err)
		} else if len(seeds) > 0 {
			joined, err := list.Join(seeds)
			if err != nil {
				log.Printf("gossip: join %v: %v (continuing alone)", seeds, err)
			} else {
				log.Printf("gossip: joined cluster, %d nodes reachable", joined)
			}
		}
	}

	// Start anti-entropy loop
	go h.antiEntropyLoop()

	return nil
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

	// Find how many retransmissions we need (at least 2 for RF=2)
	numRetransmits := h.cfg.ReplicaRF
	if numRetransmits < 2 {
		numRetransmits = 2
	}

	h.broadcasts.QueueBroadcast(&broadcastMessage{
		buf:  buf,
		name: key,
	})
}

// NodeID returns this node's memberlist name.
func (h *Handler) NodeID() string {
	if h.memberlist == nil {
		return h.cfg.NodeID
	}
	return h.memberlist.LocalNode().Name
}

// Members returns all known cluster members.
func (h *Handler) Members() []*memberlist.Node {
	if h.memberlist == nil {
		return nil
	}
	return h.memberlist.Members()
}

// Leave gracefully leaves the cluster.
func (h *Handler) Leave(timeout time.Duration) error {
	close(h.stopCh)
	if h.memberlist == nil {
		return nil
	}
	if err := h.memberlist.Leave(timeout); err != nil {
		return err
	}
	return h.memberlist.Shutdown()
}

// HandleReplication is called by the server when it receives a replication
// message. Public so the server can expose it via tests.
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
		// LWW — SetWithVersion skips if existing version >= incoming
		h.cfg.Store.SetWithVersion(e.Key, e.Value, e.Version)
	}
}

// --- anti-entropy ---

func (h *Handler) antiEntropyLoop() {
	ticker := time.NewTicker(time.Duration(h.cfg.AntiEntropySec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopCh:
			return
		case <-ticker.C:
			h.exchangeState()
		}
	}
}

func (h *Handler) exchangeState() {
	// Serialize entire store
	snapshot := h.serializeStore()
	if snapshot == nil {
		return
	}

	// Pick a random peer
	members := h.memberlist.Members()
	if len(members) <= 1 {
		return // only us
	}

	// Exclude self
	peers := make([]memberlist.Node, 0, len(members)-1)
	for _, m := range members {
		if m.Name != h.NodeID() {
			peers = append(peers, *m)
		}
	}
	if len(peers) == 0 {
		return
	}

	// Pick the first (we could randomize, but for simplicity just sync with
	// one peer each round — over time all nodes converge)
	peer := peers[0]
	snapshotBuf, err := proto.Marshal(snapshot)
	if err != nil {
		log.Printf("gossip: marshal snapshot: %v", err)
		return
	}

	// Use reliable send for anti-entropy
	if err := h.memberlist.SendReliable(&peer, snapshotBuf); err != nil {
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

// --- memberlist Delegate ---

// gossipDelegate implements memberlist.Delegate, memberlist.EventDelegate.
type gossipDelegate struct {
	handler *Handler
}

// NodeMeta encodes our Redis address into memberlist node metadata.
func (d *gossipDelegate) NodeMeta(limit int) []byte {
	meta := &pb.NodeMeta{RedisAddr: d.handler.cfg.RedisAddr}
	buf, err := proto.Marshal(meta)
	if err != nil {
		return nil
	}
	if len(buf) > limit {
		log.Printf("gossip: NodeMeta %d bytes exceeds limit %d", len(buf), limit)
		return buf[:limit]
	}
	return buf
}

// NotifyMsg handles an incoming gossip message (broadcast or anti-entropy).
func (d *gossipDelegate) NotifyMsg(msg []byte) {
	var batch pb.EntryBatch
	if err := proto.Unmarshal(msg, &batch); err != nil {
		log.Printf("gossip: unmarshal message: %v", err)
		return
	}
	d.handler.HandleReplication(batch.Entries)
}

// GetBroadcasts pulls pending broadcasts from the queue.
func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.handler.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState serializes the current store for memberlist's anti-entropy.
func (d *gossipDelegate) LocalState(join bool) []byte {
	snapshot := d.handler.serializeStore()
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

// MergeRemoteState merges incoming anti-entropy state from a peer.
func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {
	var snapshot pb.EntryBatch
	if err := proto.Unmarshal(buf, &snapshot); err != nil {
		log.Printf("gossip: unmarshal remote state: %v", err)
		return
	}
	// LWW merge all entries
	for _, e := range snapshot.Entries {
		d.handler.cfg.Store.SetWithVersion(e.Key, e.Value, e.Version)
	}
	log.Printf("gossip: merged %d entries from anti-entropy", len(snapshot.Entries))
}

// --- EventDelegate ---

// NotifyJoin is called when a new node joins the cluster.
func (d *gossipDelegate) NotifyJoin(node *memberlist.Node) {
	redisAddr := DecodeMetaRedisAddr(node.Meta)
	if redisAddr == "" {
		redisAddr = node.Addr.String()
	}
	d.handler.cfg.Ring.AddNode(node.Name, redisAddr, 256)
	log.Printf("gossip: node joined: %s (%s)", node.Name, redisAddr)
}

// NotifyLeave is called when a node leaves the cluster.
func (d *gossipDelegate) NotifyLeave(node *memberlist.Node) {
	d.handler.cfg.Ring.RemoveNode(node.Name)
	log.Printf("gossip: node left: %s", node.Name)
}

// NotifyUpdate is called when a node's metadata changes.
func (d *gossipDelegate) NotifyUpdate(node *memberlist.Node) {
	// No-op for now — metadata doesn't change after join.
}

// MemberInfo holds a summary of a cluster member for external consumers.
type MemberInfo struct {
	Name      string
	RedisAddr string
	Addr      string
}

// MembersInfo returns all cluster members with their decoded Redis addresses.
func (h *Handler) MembersInfo() []MemberInfo {
	if h.memberlist == nil {
		return nil
	}
	members := h.memberlist.Members()
	info := make([]MemberInfo, len(members))
	for i, m := range members {
		info[i] = MemberInfo{
			Name:      m.Name,
			Addr:      m.Addr.String(),
			RedisAddr: DecodeMetaRedisAddr(m.Meta),
		}
		if info[i].RedisAddr == "" {
			info[i].RedisAddr = m.Addr.String()
		}
	}
	return info
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

// --- broadcastMessage implements memberlist.Broadcast ---

type broadcastMessage struct {
	buf  []byte
	name string
}

func (b *broadcastMessage) Invalidates(other memberlist.Broadcast) bool {
	// Invalidate older broadcast for the same key
	o, ok := other.(*broadcastMessage)
	if !ok {
		return false
	}
	return b.name == o.name
}

func (b *broadcastMessage) Message() []byte {
	return b.buf
}

func (b *broadcastMessage) Finished() {
	// no-op — nothing to clean up
}
