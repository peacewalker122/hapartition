package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/peacewalker122/hapartition/internal/gossip"
)

// Server is an HTTP management server for cluster node operations.
type Server struct {
	addr   string
	gossip *gossip.Handler
	srv    *http.Server
	ln     net.Listener
}

// joinRequest is the JSON body for POST /join.
type joinRequest struct {
	Address string `json:"address"`
}

// infoResponse is the JSON body for GET /info.
type infoResponse struct {
	NodeID  string       `json:"node_id"`
	Members []memberInfo `json:"members"`
}

type memberInfo struct {
	Name       string `json:"name"`
	RedisAddr  string `json:"redis_addr"`
	GossipAddr string `json:"gossip_addr"`
}

// New creates an HTTP management server sharing the given gossip handler.
func New(addr string, g *gossip.Handler) *Server {
	return &Server{
		addr:   addr,
		gossip: g,
	}
}

// ListenAndServe starts the HTTP listener.
func (s *Server) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/info", s.handleInfo)

	s.srv = &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("mgmt: listen %s: %w", s.addr, err)
	}

	s.ln = ln
	log.Printf("mgmt: listening on %s", ln.Addr())
	go s.srv.Serve(ln)
	return nil
}

// Addr returns the listener's bound address.
func (s *Server) Addr() string {
	if s.ln != nil {
		return s.ln.Addr().String()
	}
	return s.addr
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req joinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	if req.Address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	log.Printf("mgmt: joining %s via gossip", req.Address)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	members := s.gossip.MembersInfo()
	memberInfos := make([]memberInfo, len(members))
	for i, m := range members {
		memberInfos[i] = memberInfo{
			Name:       m.Name,
			RedisAddr:  m.RedisAddr,
			GossipAddr: m.Addr,
		}
	}

	resp := infoResponse{
		NodeID:  s.gossip.NodeID(),
		Members: memberInfos,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
