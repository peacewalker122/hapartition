package mgmt

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/peacewalker122/hapartition/internal/membership"
)

// Server is an HTTP management server for cluster node operations.
type Server struct {
	addr       string
	membership *membership.Membership
	srv        *http.Server
	ln         net.Listener
}

// joinRequest is the JSON body for POST /join.
type joinRequest struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

// heartbeatRequest is the JSON body for POST /heartbeat.
type heartbeatRequest struct {
	NodeID string `json:"node_id"`
}

// infoResponse is the JSON body for GET /info.
type infoResponse struct {
	NodeID string     `json:"node_id"`
	Peers  []peerInfo `json:"peers"`
}

type peerInfo struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Status   string `json:"status"`
	LastSeen string `json:"last_seen"`
}

// New creates an HTTP management server sharing the given membership state.
func New(addr string, m *membership.Membership) *Server {
	return &Server{
		addr:       addr,
		membership: m,
	}
}

// ListenAndServe starts the HTTP listener.
func (s *Server) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/heartbeat", s.handleHeartbeat)
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
	if req.NodeID == "" || req.Address == "" {
		http.Error(w, "node_id and address are required", http.StatusBadRequest)
		return
	}

	s.membership.Join(req.NodeID, req.Address)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "OK")
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req heartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("bad request: %v", err), http.StatusBadRequest)
		return
	}
	if req.NodeID == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}

	if s.membership.Ping(req.NodeID) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	} else {
		http.Error(w, fmt.Sprintf("unknown node '%s'", req.NodeID), http.StatusNotFound)
	}
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	peers := s.membership.Peers()
	peerInfos := make([]peerInfo, len(peers))
	for i, p := range peers {
		peerInfos[i] = peerInfo{
			NodeID:   p.NodeID,
			Address:  p.Address,
			Status:   p.Status.String(),
			LastSeen: p.LastSeen.Format(time.RFC3339Nano),
		}
	}

	resp := infoResponse{
		NodeID: s.membership.NodeID(),
		Peers:  peerInfos,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
