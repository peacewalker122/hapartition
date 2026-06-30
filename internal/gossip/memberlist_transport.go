package gossip

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"google.golang.org/protobuf/proto"

	"github.com/peacewalker122/hapartition/internal/gossip/pb"
)

// memberlistTransport implements GossipTransport using HashiCorp memberlist.
type memberlistTransport struct {
	cfg       TransportConfig
	mlist     *memberlist.Memberlist
	broadcasts *memberlist.TransmitLimitedQueue
	handlers  TransportHandlers
	delegate  *mlDelegate
	startOnce sync.Once
}

// NewMemberlistTransport creates a transport backed by real memberlist.
// This is the default when gossip.Config.Transport is nil.
func NewMemberlistTransport() GossipTransport {
	return &memberlistTransport{}
}

func (m *memberlistTransport) Start(cfg TransportConfig) error {
	m.cfg = cfg
	m.delegate = &mlDelegate{transport: m}

	mlCfg := memberlist.DefaultLANConfig()
	mlCfg.Name = cfg.NodeID
	mlCfg.BindAddr = cfg.BindAddr
	mlCfg.BindPort = cfg.BindPort
	mlCfg.Delegate = m.delegate
	mlCfg.Events = m.delegate
	mlCfg.LogOutput = log.Default().Writer()
	mlCfg.EnableCompression = true

	if cfg.TLSConfig != nil {
		tlsTransport, err := NewTLSTransport(mlCfg, cfg.TLSConfig, log.Default())
		if err != nil {
			return fmt.Errorf("gossip: create tls transport: %w", err)
		}
		mlCfg.Transport = tlsTransport
	}

	list, err := memberlist.Create(mlCfg)
	if err != nil {
		return fmt.Errorf("gossip: create memberlist: %w", err)
	}
	m.mlist = list

	m.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes:       func() int { return list.NumMembers() },
		RetransmitMult: 3,
	}

	return nil
}

func (m *memberlistTransport) Join(seeds []string) (int, error) {
	return m.mlist.Join(seeds)
}

func (m *memberlistTransport) Leave(timeout time.Duration) error {
	if m.mlist == nil {
		return nil
	}
	if err := m.mlist.Leave(timeout); err != nil {
		return err
	}
	return m.mlist.Shutdown()
}

func (m *memberlistTransport) Shutdown() error {
	if m.mlist == nil {
		return nil
	}
	return m.mlist.Shutdown()
}

func (m *memberlistTransport) LocalNodeID() string {
	if m.mlist == nil {
		return m.cfg.NodeID
	}
	return m.mlist.LocalNode().Name
}

func (m *memberlistTransport) Members() []MemberInfo {
	if m.mlist == nil {
		return nil
	}
	members := m.mlist.Members()
	info := make([]MemberInfo, len(members))
	for i, nd := range members {
		status := "alive"
		switch nd.State {
		case memberlist.StateSuspect:
			status = "suspect"
		case memberlist.StateDead:
			status = "dead"
		case memberlist.StateLeft:
			status = "left"
		}
		redisAddr := DecodeMetaRedisAddr(nd.Meta)
		if redisAddr == "" {
			redisAddr = nd.Addr.String()
		}
		info[i] = MemberInfo{
			Name:      nd.Name,
			Addr:      nd.Addr.String(),
			RedisAddr: redisAddr,
			Status:    status,
		}
	}
	return info
}

func (m *memberlistTransport) SendReliable(nodeID string, buf []byte) error {
	// Find the memberlist node with this ID
	for _, nd := range m.mlist.Members() {
		if nd.Name == nodeID {
			return m.mlist.SendReliable(nd, buf)
		}
	}
	return fmt.Errorf("gossip: peer %s not found", nodeID)
}

func (m *memberlistTransport) Broadcast(buf []byte, dedupKey string) error {
	m.broadcasts.QueueBroadcast(&mlBroadcast{buf: buf, name: dedupKey})
	return nil
}

func (m *memberlistTransport) SetHandlers(h TransportHandlers) {
	m.handlers = h
}

// --- memberlist Delegate ---

// mlDelegate implements memberlist.Delegate + memberlist.EventDelegate.
type mlDelegate struct {
	transport *memberlistTransport
}

func (d *mlDelegate) NodeMeta(limit int) []byte {
	meta := &pb.NodeMeta{RedisAddr: d.transport.cfg.RedisAddr}
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

func (d *mlDelegate) NotifyMsg(msg []byte) {
	if d.transport.handlers.OnMessage != nil {
		d.transport.handlers.OnMessage("", msg)
	}
}

func (d *mlDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.transport.broadcasts.GetBroadcasts(overhead, limit)
}

func (d *mlDelegate) LocalState(join bool) []byte {
	if d.transport.handlers.OnLocalState != nil {
		return d.transport.handlers.OnLocalState()
	}
	return nil
}

func (d *mlDelegate) MergeRemoteState(buf []byte, join bool) {
	if d.transport.handlers.OnMergeRemoteState != nil {
		d.transport.handlers.OnMergeRemoteState(buf)
	}
}

func (d *mlDelegate) NotifyJoin(node *memberlist.Node) {
	redisAddr := DecodeMetaRedisAddr(node.Meta)
	if redisAddr == "" {
		redisAddr = node.Addr.String()
	}
	if d.transport.handlers.OnJoin != nil {
		d.transport.handlers.OnJoin(node.Name, redisAddr)
	}
}

func (d *mlDelegate) NotifyLeave(node *memberlist.Node) {
	if d.transport.handlers.OnLeave != nil {
		d.transport.handlers.OnLeave(node.Name)
	}
}

func (d *mlDelegate) NotifyUpdate(node *memberlist.Node) {
	redisAddr := DecodeMetaRedisAddr(node.Meta)
	if d.transport.handlers.OnUpdate != nil {
		d.transport.handlers.OnUpdate(node.Name, redisAddr)
	}
}

// --- mlBroadcast implements memberlist.Broadcast ---

type mlBroadcast struct {
	buf  []byte
	name string
}

func (b *mlBroadcast) Invalidates(other memberlist.Broadcast) bool {
	o, ok := other.(*mlBroadcast)
	if !ok {
		return false
	}
	return b.name == o.name
}

func (b *mlBroadcast) Message() []byte { return b.buf }
func (b *mlBroadcast) Finished()       {}
