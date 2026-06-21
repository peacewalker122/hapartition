package server

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/peacewalker122/hapartition/internal/hashring"
	"github.com/peacewalker122/hapartition/internal/membership"
	"github.com/peacewalker122/hapartition/pkg/api"
	"github.com/peacewalker122/hapartition/pkg/store"
)

// Server is a Redis-compatible TCP server with cluster membership tracking
// and consistent hash ring routing.
type Server struct {
	addr       string
	store      *store.Store
	membership *membership.Membership
	ring       hashring.Hashring
	ln         net.Listener
	wg         sync.WaitGroup
	quit       chan struct{}
	done       chan struct{}
}

// New creates a new Server with the given address and local node ID.
func New(addr, nodeID string) *Server {
	ring := hashring.New(nodeID)
	// Add the local node to the ring so the node's own keys are routable.
	// The address is the local bind address; MOVED redirects for these keys
	// will resolve to a different node, not back to self.
	ring.AddNode(nodeID, addr, 256)

	return &Server{
		addr:       addr,
		store:      store.New(),
		membership: membership.New(nodeID),
		ring:       ring,
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// ListenAndServe starts the TCP listener and accept loop.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.addr, err)
	}
	s.ln = ln
	log.Printf("server: listening on %s [node: %s]", s.addr, s.membership.NodeID())

	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	defer close(s.done)
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
			default:
				log.Printf("server: accept error: %v", err)
				continue
			}
		}
		s.wg.Add(1)
		go s.handleConn(conn)
	}
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	close(s.quit)
	if s.ln != nil {
		s.ln.Close()
	}

	doneCh := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until the accept loop exits.
func (s *Server) Wait() {
	<-s.done
}

// Membership returns the server's cluster membership tracker.
func (s *Server) Membership() *membership.Membership {
	return s.membership
}

// Ring returns the server's consistent hash ring.
func (s *Server) Ring() hashring.Hashring {
	return s.ring
}

// Addr returns the listener's address (useful when using :0 for tests).
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	defer s.wg.Done()

	rd := resp.NewReader(conn)
	wr := resp.NewWriter(conn)

	for {
		v, err := rd.ReadValue()
		if err != nil {
			return
		}

		cmd, ok := v.(resp.Array)
		if !ok {
			wr.WriteValue(resp.Error("ERR protocol error: expected array"))
			continue
		}
		if len(cmd) == 0 {
			wr.WriteValue(resp.Error("ERR empty command"))
			continue
		}

		name, ok := cmd[0].(resp.BulkString)
		if !ok || name.Data == nil {
			wr.WriteValue(resp.Error("ERR protocol error: command name must be a bulk string"))
			continue
		}

		args := cmd[1:]
		s.dispatch(wr, strings.ToUpper(string(name.Data)), args)
	}
}

func (s *Server) dispatch(wr *resp.Writer, cmd string, args []resp.Value) {
	switch cmd {
	case "PING":
		wr.WriteValue(resp.PONG)

	case "SET":
		s.handleKeyCommand(wr, args, func(key, value string) {
			s.store.Set(key, value)
			wr.WriteValue(resp.OK)
		})

	case "GET":
		s.handleKeyCommand(wr, args, func(key, _ string) {
			val, ok := s.store.Get(key)
			if !ok {
				wr.WriteValue(resp.NullBulkString())
			} else {
				wr.WriteValue(resp.NewBulkString(val))
			}
		})

	case "DEL":
		if len(args) < 1 {
			wr.WriteValue(resp.Error("ERR wrong number of arguments for 'DEL' command"))
			return
		}
		keys := make([]string, 0, len(args))
		for _, a := range args {
			key, ok := getBulkString(a)
			if !ok {
				wr.WriteValue(resp.Error("ERR invalid key"))
				return
			}
			keys = append(keys, key)
		}
		n := s.store.Del(keys...)
		wr.WriteValue(resp.Integer(n))

	case "NODE.JOIN":
		if len(args) < 2 {
			wr.WriteValue(resp.Error("ERR wrong number of arguments for 'NODE.JOIN' command"))
			return
		}
		nodeID, ok := getBulkString(args[0])
		if !ok {
			wr.WriteValue(resp.Error("ERR invalid node-id"))
			return
		}
		address, ok := getBulkString(args[1])
		if !ok {
			wr.WriteValue(resp.Error("ERR invalid address"))
			return
		}
		s.membership.Join(nodeID, address)
		s.ring.AddNode(nodeID, address, 256)
		wr.WriteValue(resp.OK)

	case "NODE.LIST":
		peers := s.membership.Peers()
		arr := make(resp.Array, len(peers))
		for i, p := range peers {
			arr[i] = resp.Array{
				resp.NewBulkString(p.NodeID),
				resp.NewBulkString(p.Address),
				resp.NewBulkString(p.Status.String()),
				resp.NewBulkString(p.LastSeen.Format(time.RFC3339Nano)),
			}
		}
		wr.WriteValue(arr)

	case "NODE.PING":
		if len(args) < 1 {
			wr.WriteValue(resp.Error("ERR wrong number of arguments for 'NODE.PING' command"))
			return
		}
		nodeID, ok := getBulkString(args[0])
		if !ok {
			wr.WriteValue(resp.Error("ERR invalid node-id"))
			return
		}
		if s.membership.Ping(nodeID) {
			wr.WriteValue(resp.OK)
		} else {
			wr.WriteValue(resp.Error(fmt.Sprintf("ERR unknown node '%s'", nodeID)))
		}

	case "NODE.LEAVE":
		if len(args) < 1 {
			wr.WriteValue(resp.Error("ERR wrong number of arguments for 'NODE.LEAVE' command"))
			return
		}
		nodeID, ok := getBulkString(args[0])
		if !ok {
			wr.WriteValue(resp.Error("ERR invalid node-id"))
			return
		}
		if s.membership.Remove(nodeID) {
			s.ring.RemoveNode(nodeID)
			wr.WriteValue(resp.Integer(1))
		} else {
			wr.WriteValue(resp.Integer(0))
		}

	default:
		wr.WriteValue(resp.Error(fmt.Sprintf("ERR unknown command '%s'", cmd)))
	}
}

// handleKeyCommand checks the hashring for the first key argument. If the key
// belongs to a different node, it sends a MOVED error. Otherwise it executes
// the provided fn with the key (and value for SET).
func (s *Server) handleKeyCommand(wr *resp.Writer, args []resp.Value, fn func(key, value string)) {
	if len(args) < 1 {
		wr.WriteValue(resp.Error("ERR wrong number of arguments for 'SET' command"))
		return
	}
	key, ok := getBulkString(args[0])
	if !ok {
		wr.WriteValue(resp.Error("ERR invalid key"))
		return
	}

	// Check hashring for ownership
	ownerNode, ownerAddr := s.ring.GetNode(key)
	if ownerNode != "" && ownerNode != s.membership.NodeID() {
		// Key belongs to a different node — MOVED redirect
		wr.WriteValue(resp.Error(fmt.Sprintf("MOVED %s", ownerAddr)))
		return
	}

	var value string
	if len(args) >= 2 {
		v, ok := getBulkString(args[1])
		if !ok {
			wr.WriteValue(resp.Error("ERR invalid value"))
			return
		}
		value = v
	}

	fn(key, value)
}

// getBulkString extracts a string from a RESP BulkString value.
func getBulkString(v resp.Value) (string, bool) {
	b, ok := v.(resp.BulkString)
	if !ok || b.Data == nil {
		return "", false
	}
	return string(b.Data), true
}
