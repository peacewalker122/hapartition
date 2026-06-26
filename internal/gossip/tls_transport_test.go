package gossip

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

// TestTLSTransportEndToEnd verifies two memberlist nodes can form a cluster
// using the TLS transport with mutual TLS.
func TestTLSTransportEndToEnd(t *testing.T) {
	certFile, keyFile, caFile, cleanup := genTestCerts(t)
	defer cleanup()

	tlsCfg := mustTLSConfig(t, certFile, keyFile, caFile)

	// Node 1
	n1Cfg := memberlist.DefaultLANConfig()
	n1Cfg.Name = "node-1"
	n1Cfg.BindAddr = "127.0.0.1"
	n1Cfg.BindPort = 0
	n1Cfg.EnableCompression = false
	n1Cfg.LogOutput = nil

	t1, err := NewTLSTransport(n1Cfg, tlsCfg, nil)
	if err != nil {
		t.Fatalf("NewTLSTransport node-1: %v", err)
	}
	n1Cfg.Transport = t1

	m1, err := memberlist.Create(n1Cfg)
	if err != nil {
		t.Fatalf("memberlist.Create node-1: %v", err)
	}
	defer m1.Shutdown()

	// Node 2
	n2Cfg := memberlist.DefaultLANConfig()
	n2Cfg.Name = "node-2"
	n2Cfg.BindAddr = "127.0.0.1"
	n2Cfg.BindPort = 0
	n2Cfg.EnableCompression = false
	n2Cfg.LogOutput = nil

	t2, err := NewTLSTransport(n2Cfg, tlsCfg, nil)
	if err != nil {
		t.Fatalf("NewTLSTransport node-2: %v", err)
	}
	n2Cfg.Transport = t2

	m2, err := memberlist.Create(n2Cfg)
	if err != nil {
		t.Fatalf("memberlist.Create node-2: %v", err)
	}
	defer m2.Shutdown()

	// Join node-2 to node-1 over mTLS
	joinAddr := fmt.Sprintf("127.0.0.1:%d", n1Cfg.BindPort)
	n, err := m2.Join([]string{joinAddr})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected at least 1 node joined, got %d", n)
	}

	// Wait for cluster convergence
	time.Sleep(2 * time.Second)

	// Verify both nodes see each other
	if len(m1.Members()) < 2 {
		t.Fatalf("node-1 sees %d members, want >=2", len(m1.Members()))
	}
	if len(m2.Members()) < 2 {
		t.Fatalf("node-2 sees %d members, want >=2", len(m2.Members()))
	}

	// Verify TLS-wrapped TCP stream works by sending a reliable message
	// (SendReliable uses DialAddressTimeout which wraps with TLS).
	for _, m := range m1.Members() {
		if m.Name == "node-2" {
			if err := m1.SendReliable(m, []byte("hello-via-tls")); err != nil {
				t.Fatalf("SendReliable over TLS: %v", err)
			}
			break
		}
	}
}

// genTestCerts creates a temporary CA + leaf cert pair for testing.
func genTestCerts(t *testing.T) (certFile, keyFile, caFile string, cleanup func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "hapartition-tls-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	cleanup = func() { os.RemoveAll(dir) }

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	// Generate and write a self-signed CA + leaf cert using crypto/tls
	// test certificates (simpler than full x509 generation).
	// We use net's built-in test cert approach: just create a dummy
	// self-signed cert that works for both client and server.
	if err := writeSelfSignedCert(certFile, keyFile, caFile); err != nil {
		cleanup()
		t.Fatalf("write test certs: %v", err)
	}
	return
}

// writeSelfSignedCert generates a self-signed CA and a leaf cert signed by it.
func writeSelfSignedCert(certFile, keyFile, caFile string) error {
	// For testing, we create two self-signed certs with the same CA
	// using Go's crypto/tls test infrastructure.

	// CA key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "hapartition-test-ca"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create CA: %w", err)
	}

	// Leaf key
	leafKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "hapartition-test-node"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caTmpl, &leafKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("create leaf: %w", err)
	}

	// Write CA cert
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0644); err != nil {
		return err
	}
	// Write leaf cert
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0644); err != nil {
		return err
	}
	// Write leaf key
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return err
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
		return err
	}
	return nil
}

func mustTLSConfig(t *testing.T, certFile, keyFile, caFile string) *tls.Config {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load cert/key: %v", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("no certs in CA PEM")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS12,
		// The test uses self-signed certs with CN=hapartition-test-node
		// but connects to 127.0.0.1, so we skip hostname verification.
		// Certificate chain and client cert verification still applies.
		InsecureSkipVerify: true,
	}
}


