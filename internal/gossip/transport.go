package gossip

import (
	"crypto/tls"
	"time"
)

// MemberInfo summarizes a cluster member.
type MemberInfo struct {
	Name      string
	RedisAddr string
	Addr      string
	Status    string // alive, suspect, dead, left
}

// GossipTransport abstracts the SWIM membership protocol and message passing.
// The real implementation uses HashiCorp memberlist (UDP/TCP ports).
// The test implementation uses an in-memory message bus for determinism.
type GossipTransport interface {
	// Start initialises the transport (binds to ports, creates memberlist, etc.).
	Start(cfg TransportConfig) error
	// Join attempts to join the cluster through seed addresses.
	Join(seeds []string) (int, error)
	// Leave gracefully leaves the cluster.
	Leave(timeout time.Duration) error
	// Shutdown terminates all network activity.
	Shutdown() error
	// LocalNodeID returns this node's unique ID in the cluster.
	LocalNodeID() string
	// Members returns all known cluster members.
	Members() []MemberInfo
	// SendReliable sends a guaranteed-delivery message to one node.
	SendReliable(nodeID string, buf []byte) error
	// Broadcast enqueues a best-effort message to all cluster members.
	// dedupKey allows coalescing redundant broadcasts for the same key.
	Broadcast(buf []byte, dedupKey string) error
	// SetHandlers registers callbacks that the transport calls when events
	// arrive. Must be called before Start.
	SetHandlers(h TransportHandlers)
}

// TransportConfig holds configuration for starting a GossipTransport.
type TransportConfig struct {
	NodeID    string
	BindAddr  string
	BindPort  int
	RedisAddr string
	TLSConfig *tls.Config
}

// TransportHandlers are callbacks set by gossip.Handler on the transport.
type TransportHandlers struct {
	// OnMessage is called when a gossip message arrives (broadcast or AE sync).
	OnMessage         func(from string, buf []byte)
	// OnJoin is called when a new node joins the cluster.
	OnJoin            func(nodeID string, redisAddr string)
	// OnLeave is called when a node leaves the cluster.
	OnLeave           func(nodeID string)
	// OnUpdate is called when a node's metadata changes.
	OnUpdate          func(nodeID string, redisAddr string)
	// OnLocalState is called when the transport needs a snapshot of local state
	// for push-pull sync (memberlist's LocalState delegate).
	OnLocalState      func() []byte
	// OnMergeRemoteState is called when the transport receives remote state
	// from a push-pull sync (memberlist's MergeRemoteState delegate).
	OnMergeRemoteState func(buf []byte)
}

// Clock abstracts time for deterministic testing of anti-entropy loops.
type Clock interface {
	After(d time.Duration) <-chan time.Time
	Now() time.Time
}

// wallClock is the production implementation of Clock.
type wallClock struct{}

func (wallClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (wallClock) Now() time.Time                         { return time.Now() }
