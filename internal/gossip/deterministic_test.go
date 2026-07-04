package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// ─── ManualClock ────────────────────────────────────────────────────────────

type ManualClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []afterEntry
}

type afterEntry struct {
	deadline time.Time
	ch       chan time.Time
}

func NewManualClock() *ManualClock {
	return &ManualClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	c.pending = append(c.pending, afterEntry{deadline, ch})
	return ch
}

func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var remaining []afterEntry
	for _, ae := range c.pending {
		if !ae.deadline.After(c.now) {
			ae.ch <- c.now
		} else {
			remaining = append(remaining, ae)
		}
	}
	c.pending = remaining
	c.mu.Unlock()
}

func (c *ManualClock) WaitForPending(n int, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		count := len(c.pending)
		c.mu.Unlock()
		if count >= n {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

// ─── MessageBus ─────────────────────────────────────────────────────────────

type Message struct {
	From string
	To   string
	Buf  []byte
}

type MessageBus struct {
	mu        sync.Mutex
	nodes     map[string]*simulatedTransport
	pending   []Message
	partition func(from, to string) bool
}

func NewMessageBus() *MessageBus {
	return &MessageBus{
		nodes: make(map[string]*simulatedTransport),
	}
}

func (b *MessageBus) SetPartition(fn func(from, to string) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.partition = fn
}

func (b *MessageBus) ClearPartition() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.partition = nil
}

func (b *MessageBus) DeliverAll() {
	for {
		b.mu.Lock()
		if len(b.pending) == 0 {
			b.mu.Unlock()
			return
		}
		msg := b.pending[0]
		b.pending = b.pending[1:]
		part := b.partition
		b.mu.Unlock()

		b.deliver(msg, part)
	}
}

func (b *MessageBus) PendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

func (b *MessageBus) deliver(msg Message, part func(from, to string) bool) {
	if msg.To == "" {
		for id, n := range b.nodes {
			if id == msg.From {
				continue
			}
			if part != nil && part(msg.From, id) {
				continue
			}
			n.inbox <- msg
		}
	} else {
		if part != nil && part(msg.From, msg.To) {
			return
		}
		if n, ok := b.nodes[msg.To]; ok {
			n.inbox <- msg
		}
	}
}

func (b *MessageBus) queue(msg Message) {
	b.mu.Lock()
	b.pending = append(b.pending, msg)
	b.mu.Unlock()
}

func (b *MessageBus) DrainInbox() {
	for {
		b.mu.Lock()
		var found bool
		var msg Message
		var target *simulatedTransport
		for _, n := range b.nodes {
			select {
			case msg = <-n.inbox:
				target = n
				found = true
			default:
			}
			if found {
				break
			}
		}
		b.mu.Unlock()

		if !found {
			return
		}

		if target.handlers.OnMessage != nil {
			target.handlers.OnMessage(msg.From, msg.Buf)
		}
	}
}

// ─── simulatedTransport ─────────────────────────────────────────────────────

type simulatedTransport struct {
	nodeID    string
	redisAddr string
	bus       *MessageBus
	inbox     chan Message
	handlers  TransportHandlers
	started   bool
	aeFired   chan struct{}
}

func newSimulatedTransport(nodeID, redisAddr string, bus *MessageBus) *simulatedTransport {
	return &simulatedTransport{
		nodeID:    nodeID,
		redisAddr: redisAddr,
		bus:       bus,
		inbox:     make(chan Message, 256),
		aeFired:   make(chan struct{}, 16),
	}
}

func (s *simulatedTransport) Start(cfg TransportConfig) error {
	s.nodeID = cfg.NodeID
	s.redisAddr = cfg.RedisAddr
	s.started = true

	s.bus.mu.Lock()
	s.bus.nodes[cfg.NodeID] = s
	s.bus.mu.Unlock()

	return nil
}

func (s *simulatedTransport) Join(seeds []string) (int, error) {
	return len(seeds), nil
}

func (s *simulatedTransport) Leave(timeout time.Duration) error {
	s.bus.mu.Lock()
	delete(s.bus.nodes, s.nodeID)
	s.bus.mu.Unlock()
	return nil
}

func (s *simulatedTransport) Shutdown() error {
	s.bus.mu.Lock()
	delete(s.bus.nodes, s.nodeID)
	s.bus.mu.Unlock()
	return nil
}

func (s *simulatedTransport) LocalNodeID() string { return s.nodeID }

func (s *simulatedTransport) Members() []MemberInfo {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	info := make([]MemberInfo, 0, len(s.bus.nodes))
	for id, n := range s.bus.nodes {
		info = append(info, MemberInfo{
			Name:      id,
			RedisAddr: n.redisAddr,
			Addr:      id,
			Status:    "alive",
		})
	}
	return info
}

func (s *simulatedTransport) SendReliable(nodeID string, buf []byte) error {
	s.bus.queue(Message{From: s.nodeID, To: nodeID, Buf: cloneBytes(buf)})
	select {
	case s.aeFired <- struct{}{}:
	default:
	}
	return nil
}

func (s *simulatedTransport) Broadcast(buf []byte, dedupKey string) error {
	s.bus.queue(Message{From: s.nodeID, To: "", Buf: cloneBytes(buf)})
	return nil
}

func (s *simulatedTransport) SetHandlers(h TransportHandlers) {
	s.handlers = h
}

func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ─── Test helpers ───────────────────────────────────────────────────────────

type simNode struct {
	Handler   *Handler
	Store     *store.Store
	Ring      hashring.Hashring
	Bus       *MessageBus
	Clock     *ManualClock
	transport *simulatedTransport
}

func startSimNode(t *testing.T, nodeID, redisAddr string, bus *MessageBus, clock *ManualClock) *simNode {
	t.Helper()

	st := store.New()
	ring := hashring.New(nodeID)
	ring.AddNode(nodeID, redisAddr, 256)

	sim := newSimulatedTransport(nodeID, redisAddr, bus)

	h := New(Config{
		NodeID:       nodeID,
		RedisAddr:    redisAddr,
		Store:        st,
		Ring:         ring,
		ReplicaRF:    2,
		WriteConcern: 2,
		Transport:    sim,
		Clock:        clock,
	})

	if err := h.Start(); err != nil {
		t.Fatalf("start %s: %v", nodeID, err)
	}

	return &simNode{
		Handler:   h,
		Store:     st,
		Ring:      ring,
		Bus:       bus,
		Clock:     clock,
		transport: sim,
	}
}

// crossConnect adds all nodes to each other's rings so GetReplicas can find
// remote nodes for point-to-point replication.
func crossConnect(nodes ...*simNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a == b {
				continue
			}
			a.Ring.AddNode(b.Handler.NodeID(), b.Handler.cfg.RedisAddr, 256)
		}
	}
}

// replicateAndDeliver runs ReplicateWithQuorum in a goroutine and delivers
// messages via the bus until the quorum is satisfied or timeout.
func replicateAndDeliver(t *testing.T, h *Handler, key, value string, version int64, w int, bus *MessageBus) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.ReplicateWithQuorum(key, value, version, w)
	}()

	deadline := time.After(3 * time.Second)
	for {
		bus.DeliverAll()
		bus.DrainInbox()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("replicate %s: %v", key, err)
			}
			return
		case <-deadline:
			t.Fatal("replicate: timed out waiting for quorum")
		default:
		}
	}
}

// triggerAE advances the clock past the anti-entropy interval and delivers
// all resulting messages until the bus is quiet.
func triggerAE(t *testing.T, nodes []*simNode, clock *ManualClock, bus *MessageBus) {
	t.Helper()
	// Drain stale aeFired signals from prior quorum writes.
	for _, n := range nodes {
	forDrain:
		for {
			select {
			case <-n.transport.aeFired:
			default:
				break forDrain
			}
		}
	}

	if !clock.WaitForPending(len(nodes), 2*time.Second) {
		t.Fatalf("triggerAE timeout: waiting for AE goroutines to register")
	}
	clock.Advance(31 * time.Second)

	// Deliver and process messages until the bus is quiet.
	deadline := time.After(2 * time.Second)
	for {
		bus.DeliverAll()
		bus.DrainInbox()
		if bus.PendingCount() == 0 {
			// Short sleep to let any in-flight goroutines queue more messages.
			time.Sleep(10 * time.Millisecond)
			if bus.PendingCount() == 0 {
				return
			}
		}
		select {
		case <-deadline:
			return
		default:
		}
	}
}

func assertHasKey(t *testing.T, st *store.Store, key, expectedValue string) {
	t.Helper()
	val, _, ok := st.Get(key)
	if !ok {
		t.Fatalf("key %q not found in store", key)
	}
	if val != expectedValue {
		t.Fatalf("key %q = %q, want %q", key, val, expectedValue)
	}
}

func assertNoKey(t *testing.T, st *store.Store, key string) {
	t.Helper()
	_, _, ok := st.Get(key)
	if ok {
		t.Fatalf("key %q should not exist in store", key)
	}
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestQuorumWriteReplicates(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	replicateAndDeliver(t, n1.Handler, "foo", "v1", 1, 2, bus)

	assertHasKey(t, n1.Store, "foo", "v1")
	assertHasKey(t, n2.Store, "foo", "v1")
}

func TestQuorumWriteW1(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	// W=1: only local write confirmed, no remote replication.
	if err := n1.Handler.ReplicateWithQuorum("key", "val", 1, 1); err != nil {
		t.Fatalf("W=1 write failed: %v", err)
	}

	assertHasKey(t, n1.Store, "key", "val")
	assertNoKey(t, n2.Store, "key")
}

func TestQuorumWriteMissedThenAntiEntropy(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	// Write v1 with W=2 — both get it.
	replicateAndDeliver(t, n1.Handler, "k", "v1", 1, 2, bus)
	assertHasKey(t, n2.Store, "k", "v1")

	// Partition: n2 can't receive from n1.
	bus.SetPartition(func(from, to string) bool {
		return from == "n1" && to == "n2"
	})

	// Write v2 with W=1 (since W=2 would timeout under partition).
	if err := n1.Handler.ReplicateWithQuorum("k", "v2", 2, 1); err != nil {
		t.Fatalf("W=1 write failed: %v", err)
	}
	// n2 still has v1.
	assertHasKey(t, n2.Store, "k", "v1")

	// Heal.
	bus.ClearPartition()

	// Anti-entropy converges n2 to v2.
	triggerAE(t, []*simNode{n1, n2}, clock, bus)
	assertHasKey(t, n2.Store, "k", "v2")
}

func TestQuorumWriteFullSyncAfterPartition(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	// Seed 10 keys via quorum W=2.
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		n1.Store.Set(key, "v1")
		replicateAndDeliver(t, n1.Handler, key, "v1", int64(i+1), 2, bus)
	}

	// Partition: n2 isolated.
	bus.SetPartition(func(from, to string) bool {
		return from == "n1" && to == "n2"
	})

	// Write 5 more keys with W=1 (partitioned).
	for i := 10; i < 15; i++ {
		key := string(rune('a' + i))
		n1.Store.Set(key, "v2")
		if err := n1.Handler.ReplicateWithQuorum(key, "v2", int64(i+1), 1); err != nil {
			t.Fatalf("W=1 write %s: %v", key, err)
		}
	}
	for i := 10; i < 15; i++ {
		key := string(rune('a' + i))
		assertNoKey(t, n2.Store, key)
	}

	// Heal and AE.
	bus.ClearPartition()
	triggerAE(t, []*simNode{n1, n2}, clock, bus)

	for i := 0; i < 15; i++ {
		key := string(rune('a' + i))
		want := "v1"
		if i >= 10 {
			want = "v2"
		}
		assertHasKey(t, n2.Store, key, want)
	}
}

func TestQuorumWriteThreeNodes(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	n3 := startSimNode(t, "n3", "127.0.0.3:6379", bus, clock)
	crossConnect(n1, n2, n3)

	replicateAndDeliver(t, n1.Handler, "shared", "hello", 1, 2, bus)

	// RF=2: exactly 2 of 3 nodes should have the key.
	count := 0
	for _, s := range []*store.Store{n1.Store, n2.Store, n3.Store} {
		v, _, ok := s.Get("shared")
		if ok {
			if v != "hello" {
				t.Errorf("expected 'hello', got %q", v)
			}
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected exactly 2 replicas, got %d", count)
	}
}

func TestQuorumWriteMultipleKeys(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	for i, kv := range []struct{ k, v string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		replicateAndDeliver(t, n1.Handler, kv.k, kv.v, int64(i+1), 2, bus)
	}

	assertHasKey(t, n2.Store, "a", "1")
	assertHasKey(t, n2.Store, "b", "2")
	assertHasKey(t, n2.Store, "c", "3")
}

func TestQuorumWriteConvergenceAfterPartition(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	// Seed initial state.
	replicateAndDeliver(t, n1.Handler, "k", "v1", 1, 2, bus)

	// Partition both directions.
	bus.SetPartition(func(from, to string) bool {
		return (from == "n1" && to == "n2") || (from == "n2" && to == "n1")
	})

	// Both sides write with W=1 during partition.
	if err := n1.Handler.ReplicateWithQuorum("k", "v2", 2, 1); err != nil {
		t.Fatalf("n1 write: %v", err)
	}
	if err := n2.Handler.ReplicateWithQuorum("other", "x", 10, 1); err != nil {
		t.Fatalf("n2 write: %v", err)
	}

	assertHasKey(t, n2.Store, "k", "v1")
	assertNoKey(t, n1.Store, "other")

	// Heal.
	bus.ClearPartition()

	triggerAE(t, []*simNode{n1, n2}, clock, bus)

	assertHasKey(t, n1.Store, "k", "v2")
	assertHasKey(t, n1.Store, "other", "x")
	assertHasKey(t, n2.Store, "k", "v2")
	assertHasKey(t, n2.Store, "other", "x")
}

func TestQuorumWriteDeterministic(t *testing.T) {
	run := func() string {
		clock := NewManualClock()
		bus := NewMessageBus()
		n1 := startSimNode(t, "n1", ":6379", bus, clock)
		n2 := startSimNode(t, "n2", ":6380", bus, clock)
		crossConnect(n1, n2)

		replicateAndDeliver(t, n1.Handler, "k", "val", 1, 2, bus)
		val, _, _ := n2.Store.Get("k")
		return val
	}

	r1 := run()
	r2 := run()
	if r1 != r2 {
		t.Fatalf("non-deterministic: first run=%q second run=%q", r1, r2)
	}
}

func TestQuorumWriteTimeout(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	crossConnect(n1, n2)

	// Partition to prevent ACKs from reaching n1.
	bus.SetPartition(func(from, to string) bool {
		return from == "n2" && to == "n1"
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- n1.Handler.ReplicateWithQuorum("key", "val", 1, 2)
	}()

	// Deliver request to n2; ACK from n2 is blocked by partition.
	// Wait for the quorum to time out.
	deadline := time.After(4 * time.Second)
	for {
		bus.DeliverAll()
		bus.DrainInbox()
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("expected timeout error, got nil")
			}
			t.Logf("got expected error: %v", err)
			return
		case <-deadline:
			t.Fatal("replicate didn't timeout within test deadline")
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
}
