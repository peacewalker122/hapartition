package gossip

import (
	"sync"
	"testing"
	"time"

	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// ─── ManualClock ────────────────────────────────────────────────────────────

// ManualClock implements Clock. Time advances only when the test calls Advance.
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

// After returns a channel that fires when Advance moves time past the
// deadline. Only the most recently created channel fires per deadline —
// the anti-entropy loop creates one, fires, then creates the next.
func (c *ManualClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	c.pending = append(c.pending, afterEntry{deadline, ch})
	return ch
}

// Advance moves the clock forward by d and fires every pending After channel
// whose deadline has been reached or passed.
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

// ─── MessageBus ─────────────────────────────────────────────────────────────

// Message is an in-flight message between simulated transports.
type Message struct {
	From string
	To   string // "" means broadcast to all
	Buf  []byte
}

// MessageBus routes messages between simulated transports. The test
// controls delivery by calling DeliverAll.
type MessageBus struct {
	mu        sync.Mutex
	nodes     map[string]*simulatedTransport
	pending   []Message
	partition func(from, to string) bool // nil = no partition
}

func NewMessageBus() *MessageBus {
	return &MessageBus{
		nodes: make(map[string]*simulatedTransport),
	}
}

// SetPartition installs a function that returns true when messages from→to
// should be dropped.
func (b *MessageBus) SetPartition(fn func(from, to string) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.partition = fn
}

// ClearPartition removes the partition function.
func (b *MessageBus) ClearPartition() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.partition = nil
}

// DeliverAll delivers every pending message in FIFO order. Messages blocked
// by a partition are dropped silently.
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

// PendingCount returns the number of undelivered messages.
func (b *MessageBus) PendingCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

func (b *MessageBus) deliver(msg Message, part func(from, to string) bool) {
	if msg.To == "" {
		// Broadcast to all nodes except sender
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

// DrainInbox drains all pending messages from each node's inbox and
// processes them via the node's OnMessage handler. Call this after each
// DeliverAll to process delivered messages.
func (b *MessageBus) DrainInbox() {
	for {
		b.mu.Lock()
		// Collect one message from any node's inbox
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

// simulatedTransport implements GossipTransport via a shared MessageBus.
type simulatedTransport struct {
	nodeID    string
	redisAddr string
	bus       *MessageBus
	inbox     chan Message
	handlers  TransportHandlers
	started   bool
}

func newSimulatedTransport(nodeID, redisAddr string, bus *MessageBus) *simulatedTransport {
	return &simulatedTransport{
		nodeID:    nodeID,
		redisAddr: redisAddr,
		bus:       bus,
		inbox:     make(chan Message, 256),
	}
}

func (s *simulatedTransport) Start(cfg TransportConfig) error {
	s.nodeID = cfg.NodeID
	s.redisAddr = cfg.RedisAddr
	s.started = true

	// Register with the bus
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
			Addr:      id, // gossip addr = node ID for simulated
			Status:    "alive",
		})
	}
	return info
}

func (s *simulatedTransport) SendReliable(nodeID string, buf []byte) error {
	s.bus.queue(Message{From: s.nodeID, To: nodeID, Buf: cloneBytes(buf)})
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
		NodeID:    nodeID,
		RedisAddr: redisAddr,
		Store:     st,
		Ring:      ring,
		ReplicaRF: 2,
		Transport: sim,
		Clock:     clock,
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

// triggerAE advances the clock past the anti-entropy interval to trigger
// exchangeState on all nodes. Polls until AE goroutines have enqueued at
// least one message per node onto the bus pending queue.
func triggerAE(t *testing.T, nodes []*simNode, clock *ManualClock, bus *MessageBus) {
	t.Helper()
	// Advance past the AE interval (default 30s). The AE goroutines' After
	// channels fire, causing each to call exchangeState → SendReliable → bus.
	clock.Advance(31 * time.Second)

	// Poll until enough messages have been queued (one per node, minus self).
	// The test may share pending with other messages, so we check that at
	// least one new message appeared.
	for i := 0; i < 100; i++ {
		if bus.PendingCount() >= len(nodes) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("warning: triggerAE timed out waiting for AE goroutines (pending=%d)", bus.PendingCount())
}

// assertHasKey fails if the store has no entry for key.
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

// assertNoKey fails if the store has an entry for key.
func assertNoKey(t *testing.T, st *store.Store, key string) {
	t.Helper()
	_, _, ok := st.Get(key)
	if ok {
		t.Fatalf("key %q should not exist in store", key)
	}
}

// ─── Tests ──────────────────────────────────────────────────────────────────

func TestSimBroadcastReplicates(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)

	// n1 writes a key and broadcasts.
	n1.Store.Set("foo", "v1")
	n1.Handler.Broadcast("foo", "v1", 1)

	// Deliver the broadcast message.
	bus.DeliverAll()
	bus.DrainInbox()

	// n2 should have the key via broadcast replication.
	assertHasKey(t, n2.Store, "foo", "v1")
	assertHasKey(t, n1.Store, "foo", "v1")
}

func TestSimBroadcastMissedThenAntiEntropy(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	_ = n2

	// n1 writes v1, broadcasts. Deliver.
	n1.Store.Set("k", "v1")
	n1.Handler.Broadcast("k", "v1", 1)
	bus.DeliverAll()
	bus.DrainInbox()

	// Now write v2 but with a partition so the broadcast never reaches n2.
	bus.SetPartition(func(from, to string) bool {
		return from == "n1" && to == "n2"
	})
	n1.Store.Set("k", "v2") // version 2 — n1 only
	n1.Handler.Broadcast("k", "v2", 2)
	bus.DeliverAll()
	bus.DrainInbox()
	// n2 still has v1 via the first broadcast.
	assertHasKey(t, n2.Store, "k", "v1")

	// Heal partition.
	bus.ClearPartition()

	// Advance clock past AE interval. Each node sends its snapshot to a peer.
	triggerAE(t, []*simNode{n1, n2}, clock, bus)
	bus.DeliverAll()
	bus.DrainInbox()

	// n2 should now have v2 via anti-entropy (LWW picks version 2 > 1).
	assertHasKey(t, n2.Store, "k", "v2")
}

func TestSimBroadcastMissedThenAntiEntropyFullSync(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)

	// n1 writes 10 keys.
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		n1.Store.Set(key, "v1")
	}
	// Broadcast each (so they get replicated).
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		n1.Handler.Broadcast(key, "v1", int64(i+1))
	}
	bus.DeliverAll()
	bus.DrainInbox()

	// n1 writes 5 more keys with partition active, so n2 misses them.
	bus.SetPartition(func(from, to string) bool {
		return from == "n1" && to == "n2"
	})
	for i := 10; i < 15; i++ {
		key := string(rune('a' + i))
		n1.Store.Set(key, "v2")
		n1.Handler.Broadcast(key, "v2", int64(i+1))
	}
	bus.DeliverAll()
	bus.DrainInbox()
	// n2 should not have keys 'k' through 'o'.
	for i := 10; i < 15; i++ {
		key := string(rune('a' + i))
		assertNoKey(t, n2.Store, key)
	}

	// Heal and trigger AE.
	bus.ClearPartition()
	triggerAE(t, []*simNode{n1, n2}, clock, bus)
	bus.DeliverAll()
	bus.DrainInbox()

	// Now n2 should have all 15 keys. Keys 'a'–'j' are v1, keys 'k'–'o' are v2.
	for i := 0; i < 15; i++ {
		key := string(rune('a' + i))
		want := "v1"
		if i >= 10 {
			want = "v2"
		}
		assertHasKey(t, n2.Store, key, want)
	}
}

func TestSimThreeNodeBroadcast(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)
	n3 := startSimNode(t, "n3", "127.0.0.3:6379", bus, clock)

	// n1 writes and broadcasts.
	n1.Store.Set("shared", "hello")
	n1.Handler.Broadcast("shared", "hello", 1)

	bus.DeliverAll()
	bus.DrainInbox()

	// All three nodes should have the key.
	assertHasKey(t, n1.Store, "shared", "hello")
	assertHasKey(t, n2.Store, "shared", "hello")
	assertHasKey(t, n3.Store, "shared", "hello")
}

func TestSimMultipleWritesDeterministic(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)

	// Write three keys in sequence.
	for i, kv := range []struct{ k, v string }{
		{"a", "1"}, {"b", "2"}, {"c", "3"},
	} {
		n1.Store.Set(kv.k, kv.v)
		n1.Handler.Broadcast(kv.k, kv.v, int64(i+1))
	}

	bus.DeliverAll()
	bus.DrainInbox()

	assertHasKey(t, n2.Store, "a", "1")
	assertHasKey(t, n2.Store, "b", "2")
	assertHasKey(t, n2.Store, "c", "3")
}

func TestSimConvergenceAfterPartition(t *testing.T) {
	clock := NewManualClock()
	bus := NewMessageBus()

	n1 := startSimNode(t, "n1", "127.0.0.1:6379", bus, clock)
	n2 := startSimNode(t, "n2", "127.0.0.2:6379", bus, clock)

	// Seed initial state via broadcast.
	n1.Store.Set("k", "v1")
	n1.Handler.Broadcast("k", "v1", 1)
	bus.DeliverAll()
	bus.DrainInbox()

	// Partition: n1 isolated.
	bus.SetPartition(func(from, to string) bool {
		return (from == "n1" && to == "n2") || (from == "n2" && to == "n1")
	})

	// Both sides write during partition.
	n1.Store.Set("k", "v2") // version 2
	n1.Handler.Broadcast("k", "v2", 2)

	n2.Store.Set("other", "x") // different key on n2
	n2.Handler.Broadcast("other", "x", 10)

	bus.DeliverAll()
	bus.DrainInbox()

	// n2 should not see v2.
	assertHasKey(t, n2.Store, "k", "v1")
	// n1 should not see "other".
	assertNoKey(t, n1.Store, "other")

	// Heal.
	bus.ClearPartition()

	// Two AE rounds: first round each node sends its snapshot.
	// n1 → n2 carries k=v2 (v2 > v1, so n2 updates)
	// n2 → n1 carries other=x (n1 didn't have it)
	triggerAE(t, []*simNode{n1, n2}, clock, bus)
	bus.DeliverAll()
	bus.DrainInbox()

	// Both nodes should have both keys after convergence.
	assertHasKey(t, n1.Store, "k", "v2")
	assertHasKey(t, n1.Store, "other", "x")
	assertHasKey(t, n2.Store, "k", "v2")
	assertHasKey(t, n2.Store, "other", "x")
}

// TestSimDeterministicRunTwice verifies that two identical test runs produce
// the same result (the test itself is deterministic).
func TestSimDeterministicRunTwice(t *testing.T) {
	run := func() string {
		clock := NewManualClock()
		bus := NewMessageBus()
		n1 := startSimNode(t, "n1", ":6379", bus, clock)
		n2 := startSimNode(t, "n2", ":6380", bus, clock)

		n1.Store.Set("k", "val")
		n1.Handler.Broadcast("k", "val", 1)
		bus.DeliverAll()
		bus.DrainInbox()

		val, _, _ := n2.Store.Get("k")
		return val
	}

	r1 := run()
	r2 := run()
	if r1 != r2 {
		t.Fatalf("non-deterministic: first run=%q second run=%q", r1, r2)
	}
}
