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
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	chImage      = "clickhouse/clickhouse-server:26.3"
	seaweedImage = "chrislusf/seaweedfs:3.73"
	chPassword   = "secret"
)

// httpsConfig enables the HTTPS interface on 8443 with the self-signed cert.
const httpsConfig = `<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
        </server>
    </openSSL>
</clickhouse>`

// localStorageConfig defines a "tiered" policy over two local disks (the built-in
// "default" plus "cold"), so a partition can be moved from one disk to the other.
const localStorageConfig = `<clickhouse>
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

// s3StorageConfig defines a "tiered" policy where new parts land on the local
// "default" disk and can be moved to the "object_storage" S3 disk backed by the
// SeaweedFS gateway reachable at the "seaweedfs" network alias.
const s3StorageConfig = `<clickhouse>
    <storage_configuration>
        <disks>
            <object_storage>
                <type>s3</type>
                <endpoint>http://seaweedfs:8333/clickhouse/data/</endpoint>
                <access_key_id>clickhouse</access_key_id>
                <secret_access_key>clickhouse</secret_access_key>
                <region>us-east-1</region>
                <support_batch_delete>false</support_batch_delete>
            </object_storage>
        </disks>
        <policies>
            <tiered>
                <volumes>
                    <local><disk>default</disk></local>
                    <s3><disk>object_storage</disk></s3>
                </volumes>
                <move_factor>0.0</move_factor>
            </tiered>
        </policies>
    </storage_configuration>
</clickhouse>`

// startClickHouse boots a single ClickHouse node with the two-local-disk policy.
func startClickHouse(t *testing.T) *Client {
	return startClickHouseNode(t, localStorageConfig, "")
}

// startClickHouseWithS3 boots SeaweedFS (S3) and a ClickHouse node wired to it on
// a shared network, using the local+S3 storage policy. It returns a client for a
// realistic local -> object_storage move.
func startClickHouseWithS3(t *testing.T) *Client {
	t.Helper()
	nw, err := network.New(context.Background())
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	startSeaweedFS(t, nw.Name)
	return startClickHouseNode(t, s3StorageConfig, nw.Name)
}

// startClickHouseNode boots a ClickHouse node with HTTPS and the given storage
// configuration, optionally joining networkName (needed to reach SeaweedFS).
func startClickHouseNode(t *testing.T, storageXML, networkName string) *Client {
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
			{Reader: strings.NewReader(httpsConfig), ContainerFilePath: "/etc/clickhouse-server/config.d/https.xml", FileMode: 0o644},
			{Reader: strings.NewReader(storageXML), ContainerFilePath: "/etc/clickhouse-server/config.d/storage.xml", FileMode: 0o644},
			{Reader: strings.NewReader(crt), ContainerFilePath: "/etc/clickhouse-server/certs/server.crt", FileMode: 0o644},
			{Reader: strings.NewReader(key), ContainerFilePath: "/etc/clickhouse-server/certs/server.key", FileMode: 0o644},
		},
		// The image logs to files, not stdout, so a log-based wait never matches;
		// wait for the HTTPS listener instead, then poll Ping below.
		WaitingFor: wait.ForListeningPort("8443/tcp").WithStartupTimeout(2 * time.Minute),
	}
	if networkName != "" {
		req.Networks = []string{networkName}
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
		Timeout:            2 * time.Minute,
	})

	// The listener opens slightly before requests are accepted; poll Ping.
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

// startSeaweedFS boots a SeaweedFS all-in-one server exposing the S3 gateway on
// the given network (alias "seaweedfs") and creates the "clickhouse" bucket.
func startSeaweedFS(t *testing.T, networkName string) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:          seaweedImage,
		Cmd:            []string{"server", "-s3", "-dir=/data", "-master.volumeSizeLimitMB=1024", "-volume.max=0"},
		ExposedPorts:   []string{"8333/tcp", "9333/tcp"},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"seaweedfs"}},
		WaitingFor:     wait.ForHTTP("/cluster/status").WithPort("9333/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start seaweedfs: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(context.Background()); err != nil {
			t.Logf("terminate seaweedfs: %v", err)
		}
	})

	// Create the bucket the object_storage disk writes into; retry until the
	// filer is ready to serve weed shell commands.
	deadline := time.Now().Add(60 * time.Second)
	for {
		code, _, err := c.Exec(ctx, []string{"sh", "-c", "echo 's3.bucket.create -name clickhouse' | weed shell -master localhost:9333"})
		if err == nil && code == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create bucket: code=%d err=%v", code, err)
		}
		time.Sleep(time.Second)
	}
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

// move relocates partition 202401 of default.events to disk.
func move(ctx context.Context, c *Client, disk string) error {
	return c.MovePartition(ctx, "default", "events", "202401", disk, true,
		MoveOptions{PollInterval: 200 * time.Millisecond})
}

func TestIntegrationMoveLocalToS3(t *testing.T) {
	c := startClickHouseWithS3(t)
	ctx := context.Background()

	// withPayload=true gives each row an incompressible String, so the part has
	// real size on disk (compressible data would upload to S3 almost instantly).
	seedEvents(t, c, 3_000_000, true)
	requirePartitionOn(t, c, "202401", "default")
	t.Logf("partition on-disk size: %d MB",
		scalarInt(t, c, "SELECT sum(bytes_on_disk) FROM system.parts WHERE table='events' AND active")/1024/1024)

	// Move to the S3-backed object_storage disk. Unlike local->local, this
	// transfers real bytes to SeaweedFS, so the watcher goroutine ticks.
	start := time.Now()
	if err := move(ctx, c, "object_storage"); err != nil {
		t.Fatalf("MovePartition to S3: %v", err)
	}
	t.Logf("local->S3 MOVE took %s", time.Since(start).Round(time.Millisecond))

	requirePartitionOn(t, c, "202401", "object_storage")
	requireRowCount(t, c, 3_000_000) // data fully readable from S3

	// Idempotent re-run.
	if err := move(ctx, c, "object_storage"); !errors.Is(err, ErrAlreadyOnTarget) {
		t.Fatalf("second move to S3 should return ErrAlreadyOnTarget, got %v", err)
	}
}

// mustExec runs a statement and fails the test on error.
func mustExec(t *testing.T, c *Client, query string) {
	t.Helper()
	if _, err := c.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// seedEvents creates default.events on the 'tiered' policy and inserts rows for
// partition 202401. When withPayload is set, each row carries an incompressible
// String so the part has meaningful on-disk size.
func seedEvents(t *testing.T, c *Client, rows int, withPayload bool) {
	t.Helper()
	cols, sel := "(event_date Date, id UInt64)", "toDate('2024-01-01') + (number % 28), number"
	if withPayload {
		cols = "(event_date Date, id UInt64, payload String)"
		sel = "toDate('2024-01-01') + (number % 28), number, randomPrintableASCII(200)"
	}
	mustExec(t, c, "CREATE TABLE default.events "+cols+`
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(event_date)
		ORDER BY id
		SETTINGS storage_policy = 'tiered'`)
	mustExec(t, c, fmt.Sprintf("INSERT INTO default.events SELECT %s FROM numbers(%d)", sel, rows))
}

// requirePartitionOn asserts the partition's single active disk equals want.
func requirePartitionOn(t *testing.T, c *Client, partitionID, want string) {
	t.Helper()
	if got := partitionDisk(t, c, partitionID); got != want {
		t.Fatalf("partition %s: expected disk %q, got %q", partitionID, want, got)
	}
}

// requireRowCount asserts default.events holds exactly want rows.
func requireRowCount(t *testing.T, c *Client, want int) {
	t.Helper()
	if got := scalarInt(t, c, "SELECT count() FROM default.events"); got != want {
		t.Fatalf("expected %d rows, got %d", want, got)
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
