//go:build integration

// Integration tests that exercise the client against a real ClickHouse server
// started with testcontainers-go. They replace the manual Makefile workflow:
// a single node is configured with HTTPS and a two-disk storage policy so we can
// verify node discovery, MOVE PARTITION between disks, and idempotency for real.
//
// Run with:
//
//	go test -tags=integration ./...
//
// Requires a running Docker daemon.
package clickhouse

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	chImage    = "clickhouse/clickhouse-server:26.3"
	chPassword = "secret"
)

// serverConfig enables the HTTPS interface on 8443 and defines a "tiered"
// storage policy with two local disks (the built-in "default" plus "cold"), so
// a partition can be moved from one disk to the other.
const serverConfig = `<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
        </server>
    </openSSL>
    <storage_configuration>
        <disks>
            <cold>
                <type>local</type>
                <path>/var/lib/clickhouse/cold/</path>
            </cold>
        </disks>
        <policies>
            <tiered>
                <volumes>
                    <hot><disk>default</disk></hot>
                    <cold><disk>cold</disk></cold>
                </volumes>
                <move_factor>0.0</move_factor>
            </tiered>
        </policies>
    </storage_configuration>
</clickhouse>`

// startClickHouse boots a single ClickHouse node with HTTPS + the tiered storage
// policy and returns a Client pointed at it. The container is terminated when the
// test finishes.
func startClickHouse(t *testing.T) *Client {
	t.Helper()
	ctx := context.Background()

	crt, key := selfSignedCert(t)

	req := testcontainers.ContainerRequest{
		Image:        chImage,
		ExposedPorts: []string{"8443/tcp"},
		// Setting a password enables network access for the default user; the
		// entrypoint otherwise locks it down when no credentials are provided.
		Env: map[string]string{"CLICKHOUSE_PASSWORD": chPassword},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(serverConfig),
				ContainerFilePath: "/etc/clickhouse-server/config.d/test-overrides.xml",
				FileMode:          0o644,
			},
			{
				Reader:            strings.NewReader(crt),
				ContainerFilePath: "/etc/clickhouse-server/certs/server.crt",
				FileMode:          0o644,
			},
			{
				Reader:            strings.NewReader(key),
				ContainerFilePath: "/etc/clickhouse-server/certs/server.key",
				FileMode:          0o644,
			},
		},
		// The image logs to files, not stdout, so a log-based wait never matches;
		// wait for the HTTPS listener instead, then poll Ping below.
		WaitingFor: wait.ForListeningPort("8443/tcp").WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start clickhouse container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	mapped, err := container.MappedPort(ctx, "8443/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	client := New(Options{
		Host:               host,
		Port:               int(mapped.Num()),
		User:               "default",
		Password:           chPassword,
		InsecureSkipVerify: true,
		Timeout:            30 * time.Second,
	})

	// The log line fires slightly before the HTTPS listener accepts requests;
	// poll Ping until it succeeds.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := client.Ping(ctx); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("clickhouse never became ready: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return client
}

func TestIntegrationClusterNodes(t *testing.T) {
	client := startClickHouse(t)
	ctx := context.Background()

	// A single node still exposes at least one entry in system.clusters.
	nodes, err := client.ClusterNodes(ctx, "")
	if err != nil {
		t.Fatalf("ClusterNodes: %v", err)
	}
	if len(nodes) == 0 {
		t.Fatal("ClusterNodes returned no nodes")
	}
}

func TestIntegrationMovePartition(t *testing.T) {
	client := startClickHouse(t)
	ctx := context.Background()

	exec := func(q string) {
		t.Helper()
		if _, err := client.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}

	exec(`CREATE TABLE default.events
		(event_date Date, id UInt64)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(event_date)
		ORDER BY id
		SETTINGS storage_policy = 'tiered'`)
	exec(`INSERT INTO default.events
		SELECT toDate('2024-01-01') + (number % 28), number FROM numbers(100)`)

	// Data lands on the first volume ("hot" = default disk).
	if disk := partitionDisk(t, client, "202401"); disk != "default" {
		t.Fatalf("partition should start on 'default', got %q", disk)
	}

	// Move it to the cold disk.
	if err := client.MovePartition(ctx, "default", "events", "202401", "cold", true); err != nil {
		t.Fatalf("MovePartition: %v", err)
	}
	if disk := partitionDisk(t, client, "202401"); disk != "cold" {
		t.Fatalf("partition should be on 'cold' after move, got %q", disk)
	}

	// Verify no rows were lost in the move.
	if n := scalarInt(t, client, "SELECT count() FROM default.events"); n != 100 {
		t.Fatalf("expected 100 rows after move, got %d", n)
	}

	// Moving again is a no-op: ClickHouse error 479 -> ErrAlreadyOnTarget.
	err := client.MovePartition(ctx, "default", "events", "202401", "cold", true)
	if !errors.Is(err, ErrAlreadyOnTarget) {
		t.Fatalf("second move should return ErrAlreadyOnTarget, got %v", err)
	}
}

// partitionDisk returns the disk holding the single active part of the partition.
func partitionDisk(t *testing.T, c *Client, partitionID string) string {
	t.Helper()
	q := fmt.Sprintf(
		"SELECT DISTINCT disk_name FROM system.parts WHERE database='default' AND table='events' AND partition_id=%s AND active",
		quoteLiteral(partitionID),
	)
	vals, err := c.QueryColumn(context.Background(), q)
	if err != nil {
		t.Fatalf("query disk_name: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("expected exactly one active disk for partition %s, got %v", partitionID, vals)
	}
	return vals[0]
}

// scalarInt runs a query returning a single integer value.
func scalarInt(t *testing.T, c *Client, query string) int {
	t.Helper()
	vals, err := c.QueryColumn(context.Background(), query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	if len(vals) != 1 {
		t.Fatalf("expected one value from %q, got %v", query, vals)
	}
	n, err := strconv.Atoi(vals[0])
	if err != nil {
		t.Fatalf("parse int from %q: %v", vals[0], err)
	}
	return n
}

// selfSignedCert returns a PEM-encoded certificate and key for the HTTPS
// listener. The client uses InsecureSkipVerify, so the contents only need to be
// a structurally valid cert the server will load.
func selfSignedCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	return certPEM, keyPEM
}
