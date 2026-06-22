package mgmt

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/peacewalker122/hapartition/internal/gossip"
	"github.com/peacewalker122/hapartition/internal/hashring"
)

//go:embed dashboard.html
var dashboardHTML string

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
	NodeID   string       `json:"node_id"`
	KeyCount int          `json:"key_count"`
	Members  []memberInfo `json:"members"`
}

type memberInfo struct {
	Name       string `json:"name"`
	RedisAddr  string `json:"redis_addr"`
	GossipAddr string `json:"gossip_addr"`
	Status     string `json:"status"`
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
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/info", s.handleInfo)
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/events", s.handleEvents)
	mux.HandleFunc("/events/stream", s.handleEventStream)
	mux.HandleFunc("/ring", s.handleRing)

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

// ── Dashboard ──

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

// ── POST /join ──

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

// ── GET /info ──

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
			Status:     m.Status,
		}
	}

	resp := infoResponse{
		NodeID:   s.gossip.NodeID(),
		KeyCount: s.gossip.KeyCount(),
		Members:  memberInfos,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── GET /events ──

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	n := 50 // default
	if s := r.URL.Query().Get("n"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 200 {
			n = v
		}
	}

	events := s.gossip.RecentEvents(n)
	if events == nil {
		events = []gossip.ClusterEvent{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// ── GET /events/stream (SSE) ──

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := make(chan gossip.ClusterEvent, 32)
	unsub := s.gossip.Subscribe(ch)
	defer unsub()

	// Send initial keepalive
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	ctx := r.Context()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		case ev := <-ch:
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
			flusher.Flush()
		}
	}
}

// ── GET /ring ──

func (s *Server) handleRing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entries := s.gossip.Ring().RingSnapshot()
	if entries == nil {
		entries = []hashring.RingEntryInfo{}
	}

	// Return as a JSON object with metadata
	type ringResponse struct {
		LocalNode  string               `json:"local_node"`
		NodeCount  int                  `json:"node_count"`
		TotalVNodes int                 `json:"total_vnodes"`
		Entries    []hashringAPIEntry   `json:"entries"`
	}

	// Deduplicate nodes
	seen := make(map[string]struct{})
	for _, e := range entries {
		seen[e.NodeID] = struct{}{}
	}

	resp := ringResponse{
		LocalNode:   s.gossip.NodeID(),
		NodeCount:   len(seen),
		TotalVNodes: len(entries),
		Entries:     toAPIRingEntries(entries),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ── Ring JSON types (avoid exposing internal hashring types in API) ──

type hashringAPIEntry struct {
	Hash    string `json:"hash"`     // hex string for JS bigint safety
	HashU64 uint64 `json:"hash_u64"` // raw value for sorting
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
}

func toAPIRingEntries(in []hashring.RingEntryInfo) []hashringAPIEntry {
	out := make([]hashringAPIEntry, len(in))
	for i, e := range in {
		// Format as 16-char hex for JS (max 2^53 safe integer in JS)
		out[i] = hashringAPIEntry{
			Hash:    fmt.Sprintf("%016x", e.Hash),
			HashU64: e.Hash,
			NodeID:  e.NodeID,
			Address: e.Address,
		}
	}
	return out
}
