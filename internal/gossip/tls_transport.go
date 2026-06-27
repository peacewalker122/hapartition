package gossip

import (
	"crypto/tls"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// TLSTransport wraps memberlist's NetTransport to add mutual TLS on TCP
// streams. UDP packets are passed through unmodified — they carry only
// lightweight probe/ack messages; all actual data replication (state sync,
// anti-entropy, reliable sends) happens over the TLS-wrapped TCP
// connections.
type TLSTransport struct {
	inner      memberlist.NodeAwareTransport
	tlsConfig  *tls.Config
	streamCh   chan net.Conn
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

var _ memberlist.NodeAwareTransport = (*TLSTransport)(nil)

// NewTLSTransport creates a NetTransport for UDP and wraps its TCP
// connections with the given TLS config for mutual TLS.
func NewTLSTransport(config *memberlist.Config, tlsCfg *tls.Config, logger *log.Logger) (*TLSTransport, error) {
	nc := &memberlist.NetTransportConfig{
		BindAddrs: []string{config.BindAddr},
		BindPort:  config.BindPort,
		Logger:    logger,
	}
	nt, err := memberlist.NewNetTransport(nc)
	if err != nil {
		return nil, err
	}
	if config.BindPort == 0 {
		port := nt.GetAutoBindPort()
		config.BindPort = port
		config.AdvertisePort = port
	}

	t := &TLSTransport{
		inner:      nt,
		tlsConfig:  tlsCfg,
		streamCh:   make(chan net.Conn),
		shutdownCh: make(chan struct{}),
	}

	t.wg.Add(1)
	go t.streamInterceptor()

	return t, nil
}

// streamInterceptor reads raw TCP connections from the inner transport's
// StreamCh, performs a TLS server-side handshake (verifying the client
// certificate), and forwards the TLS connection to our own streamCh.
func (t *TLSTransport) streamInterceptor() {
	defer t.wg.Done()
	rawCh := t.inner.StreamCh()
	for {
		select {
		case <-t.shutdownCh:
			return
		case conn, ok := <-rawCh:
			if !ok {
				return
			}
			tlsConn := tls.Server(conn, t.tlsConfig)
			// Perform handshake to verify client cert at connect time rather
			// than letting it lazy-handshake on the first read/write.
			if err := tlsConn.Handshake(); err != nil {
				log.Printf("tls: server handshake from %s: %v", conn.RemoteAddr(), err)
				conn.Close()
				continue
			}
			// Send or drop if shutting down.
			select {
			case t.streamCh <- tlsConn:
			case <-t.shutdownCh:
				conn.Close()
				return
			}
		}
	}
}

// --- Transport interface ---

func (t *TLSTransport) FinalAdvertiseAddr(ip string, port int) (net.IP, int, error) {
	return t.inner.FinalAdvertiseAddr(ip, port)
}

func (t *TLSTransport) WriteTo(b []byte, addr string) (time.Time, error) {
	return t.inner.WriteTo(b, addr)
}

func (t *TLSTransport) PacketCh() <-chan *memberlist.Packet {
	return t.inner.PacketCh()
}

// DialTimeout dials a raw TCP connection, then wraps it with TLS client-side.
// The host part of addr is used as the TLS ServerName for certificate
// verification when the peer's node name is not available.
func (t *TLSTransport) DialTimeout(addr string, timeout time.Duration) (net.Conn, error) {
	// Extract hostname from addr for ServerName fallback.
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return t.DialAddressTimeout(memberlist.Address{Addr: addr, Name: host}, timeout)
}

// StreamCh returns a channel of incoming TLS-wrapped stream connections.
func (t *TLSTransport) StreamCh() <-chan net.Conn {
	return t.streamCh
}

func (t *TLSTransport) Shutdown() error {
	close(t.shutdownCh)
	t.wg.Wait()
	return t.inner.Shutdown()
}

// --- NodeAwareTransport interface ---

func (t *TLSTransport) WriteToAddress(b []byte, addr memberlist.Address) (time.Time, error) {
	return t.inner.WriteToAddress(b, addr)
}

// dialTLSConfig returns a shallow clone of t.tlsConfig with ServerName set
// to the given name. Go's crypto/tls requires ServerName for certificate
// hostname verification unless InsecureSkipVerify is true.
func (t *TLSTransport) dialTLSConfig(serverName string) *tls.Config {
	cfg := t.tlsConfig.Clone()
	// Only override ServerName if the base config didn't set one.
	// In k8s, memberlist node names (e.g. "hapartition-0") aren't valid DNS
	// SANs, so the caller should set a ServerName that matches the cert.
	if serverName != "" && cfg.ServerName == "" {
		cfg.ServerName = serverName
	}
	return cfg
}

// DialAddressTimeout dials a raw TCP connection, wraps it with TLS
// client-side, and performs the TLS handshake before returning.
// The peer's node name (addr.Name) is used as the TLS ServerName for
// certificate hostname verification.
func (t *TLSTransport) DialAddressTimeout(addr memberlist.Address, timeout time.Duration) (net.Conn, error) {
	raw, err := t.inner.DialAddressTimeout(addr, timeout)
	if err != nil {
		return nil, err
	}
	tlsCfg := t.dialTLSConfig(addr.Name)
	tlsConn := tls.Client(raw, tlsCfg)
	if err := tlsConn.Handshake(); err != nil {
		raw.Close()
		return nil, err
	}
	return tlsConn, nil
}
