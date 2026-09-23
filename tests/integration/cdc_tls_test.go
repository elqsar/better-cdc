//go:build integration

package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"better-cdc/internal/publisher"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPublisherMutualTLSAndAuthentication(t *testing.T) {
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "CDC test"}, NotBefore: time.Now().Add(-7 * 24 * time.Hour), NotAfter: time.Now().Add(7 * 24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, configPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "nats.conf")
	for path, data := range map[string][]byte{certPath: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), keyPath: pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), configPath: []byte(`jetstream { store_dir: "/data" }
 tls { cert_file: "/cert.pem", key_file: "/key.pem", ca_file: "/cert.pem", verify: true }
 authorization { user: "cdc", password: "test-secret" }
 `)} {
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "nats:2.10-alpine", ExposedPorts: []string{"4222/tcp"}, Cmd: []string{"-c", "/nats.conf"}, Files: []testcontainers.ContainerFile{{HostFilePath: certPath, ContainerFilePath: "/cert.pem", FileMode: 0644}, {HostFilePath: keyPath, ContainerFilePath: "/key.pem", FileMode: 0600}, {HostFilePath: configPath, ContainerFilePath: "/nats.conf", FileMode: 0644}}, WaitingFor: wait.ForListeningPort("4222/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(context.Background(), "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	options := publisher.JetStreamOptions{URLs: []string{fmt.Sprintf("tls://%s:%s", host, port.Port())}, Username: "cdc", Password: "test-secret", TLSCA: certPath, TLSCert: certPath, TLSKey: keyPath, StreamName: "TLS", ConnectTimeout: 2 * time.Second, PublishTimeout: 2 * time.Second}
	p := publisher.NewJetStreamPublisher(options, nil)
	if err = p.Connect(); err != nil {
		t.Fatal(err)
	}
	if err = p.Publish(context.Background(), "cdc.tls.public.t", []byte("{}"), "tls-event"); err != nil {
		t.Fatal(err)
	}
	_ = p.Close()
	options.Password = "wrong"
	bad := publisher.NewJetStreamPublisher(options, nil)
	if err = bad.Connect(); err == nil {
		_ = bad.Close()
		t.Fatal("invalid authentication succeeded")
	}
	options.Password = "test-secret"
	options.TLSCA = ""
	untrusted := publisher.NewJetStreamPublisher(options, nil)
	if err = untrusted.Connect(); err == nil {
		_ = untrusted.Close()
		t.Fatal("untrusted certificate accepted")
	}
}
