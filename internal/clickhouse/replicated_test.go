//go:build integration

// Research: how the mover behaves on a single-shard cluster whose table is a
// ReplicatedMergeTree. The central question is whether ALTER ... MOVE PARTITION
// TO DISK propagates between replicas (if it did, running it on one node would be
// enough; if it does not, the mover MUST run it on every replica). This test
// spins up ClickHouse Keeper + two replicas of one shard and checks it for real.
package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const keeperImage = "clickhouse/clickhouse-keeper:26.3"

const keeperConfig = `<clickhouse>
    <logger><level>warning</level><console>true</console></logger>
    <listen_host>0.0.0.0</listen_host>
    <keeper_server>
        <tcp_port>9181</tcp_port>
        <server_id>1</server_id>
        <log_storage_path>/var/lib/clickhouse-keeper/coordination/log</log_storage_path>
        <snapshot_storage_path>/var/lib/clickhouse-keeper/coordination/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <session_timeout_ms>30000</session_timeout_ms>
        </coordination_settings>
        <raft_configuration>
            <server><id>1</id><hostname>keeper</hostname><port>9234</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>`

const zookeeperConfig = `<clickhouse>
    <zookeeper><node><host>keeper</host><port>9181</port></node></zookeeper>
</clickhouse>`

const singleShardCluster = `<clickhouse>
    <remote_servers>
        <single_shard>
            <shard>
                <internal_replication>true</internal_replication>
                <replica><host>ch1</host><port>9000</port></replica>
                <replica><host>ch2</host><port>9000</port></replica>
            </shard>
        </single_shard>
    </remote_servers>
</clickhouse>`

func macrosConfig(replica string) string {
	return `<clickhouse><macros><shard>01</shard><replica>` + replica + `</replica></macros></clickhouse>`
}

// startReplicatedPair boots Keeper and two ClickHouse replicas of a single shard
// on a shared network, returning a client for each replica.
func startReplicatedPair(t *testing.T) (*Client, *Client) {
	t.Helper()
	ctx := context.Background()

	nw, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	startKeeper(t, nw.Name)
	c1 := startReplica(t, nw.Name, "ch1", "r1")
	c2 := startReplica(t, nw.Name, "ch2", "r2")
	return c1, c2
}

func startKeeper(t *testing.T, networkName string) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:          keeperImage,
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {"keeper"}},
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(keeperConfig), ContainerFilePath: "/etc/clickhouse-keeper/keeper_config.xml", FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort("9181/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start keeper: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
}

func startReplica(t *testing.T, networkName, alias, replica string) *Client {
	t.Helper()
	ctx := context.Background()
	crt, key := selfSignedCert(t)

	req := testcontainers.ContainerRequest{
		Image:          chImage,
		Hostname:       alias,
		ExposedPorts:   []string{"8443/tcp"},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {alias}},
		Env:            map[string]string{"CLICKHOUSE_PASSWORD": chPassword},
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(httpsConfig), ContainerFilePath: "/etc/clickhouse-server/config.d/https.xml", FileMode: 0o644},
			{Reader: strings.NewReader(localStorageConfig), ContainerFilePath: "/etc/clickhouse-server/config.d/storage.xml", FileMode: 0o644},
			{Reader: strings.NewReader(zookeeperConfig), ContainerFilePath: "/etc/clickhouse-server/config.d/zookeeper.xml", FileMode: 0o644},
			{Reader: strings.NewReader(singleShardCluster), ContainerFilePath: "/etc/clickhouse-server/config.d/cluster.xml", FileMode: 0o644},
			{Reader: strings.NewReader(macrosConfig(replica)), ContainerFilePath: "/etc/clickhouse-server/config.d/macros.xml", FileMode: 0o644},
			{Reader: strings.NewReader(crt), ContainerFilePath: "/etc/clickhouse-server/certs/server.crt", FileMode: 0o644},
			{Reader: strings.NewReader(key), ContainerFilePath: "/etc/clickhouse-server/certs/server.key", FileMode: 0o644},
		},
		WaitingFor: wait.ForListeningPort("8443/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start replica %s: %v", alias, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	mapped, err := c.MappedPort(ctx, "8443/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}
	client := New(Options{Host: host, Port: int(mapped.Num()), User: "default", Password: chPassword, InsecureSkipVerify: true, Timeout: time.Minute})

	deadline := time.Now().Add(30 * time.Second)
	for {
		if err := client.Ping(ctx); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("replica %s never became ready: %v", alias, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return client
}

func TestIntegrationReplicatedMoveIsNotReplicated(t *testing.T) {
	c1, c2 := startReplicatedPair(t)
	ctx := context.Background()

	exec1 := func(q string) {
		t.Helper()
		if _, err := c1.Exec(ctx, q); err != nil {
			t.Fatalf("exec on ch1 %q: %v", q, err)
		}
	}

	// One shard, two replicas of the same ReplicatedMergeTree table.
	exec1(`CREATE TABLE default.events ON CLUSTER single_shard
		(event_date Date, id UInt64)
		ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/events', '{replica}')
		PARTITION BY toYYYYMM(event_date)
		ORDER BY id
		SETTINGS storage_policy = 'tiered'`)
	exec1(`INSERT INTO default.events
		SELECT toDate('2024-01-01') + (number % 28), number FROM numbers(1000)`)

	// Wait until the second replica has fetched the data.
	if _, err := c2.Exec(ctx, "SYSTEM SYNC REPLICA default.events"); err != nil {
		t.Fatalf("sync replica on ch2: %v", err)
	}
	if n := scalarInt(t, c2, "SELECT count() FROM default.events"); n != 1000 {
		t.Fatalf("ch2 should have replicated 1000 rows, got %d", n)
	}

	// Both replicas start with the partition on the local "default" disk.
	requirePartitionOn(t, c1, "202401", "default")
	requirePartitionOn(t, c2, "202401", "default")

	// Move the partition to 'cold' on ch1 ONLY.
	if err := c1.MovePartition(ctx, "default", "events", "202401", "cold", true, MoveOptions{PollInterval: 200 * time.Millisecond}); err != nil {
		t.Fatalf("move on ch1: %v", err)
	}

	// Give any (hypothetical) replication of the move time to propagate, then
	// confirm ch2 is UNAFFECTED — proving MOVE ... TO DISK is a local, non-
	// replicated operation.
	time.Sleep(3 * time.Second)
	_, _ = c2.Exec(ctx, "SYSTEM SYNC REPLICA default.events")
	requirePartitionOn(t, c1, "202401", "cold")
	if disk := partitionDisk(t, c2, "202401"); disk != "default" {
		t.Fatalf("EXPECTED ch2 still on 'default' (move not replicated), but it is on %q", disk)
	}
	t.Log("confirmed: MOVE ... TO DISK on ch1 did NOT move the partition on ch2 (not replicated)")

	// Running it on ch2 too puts the partition on 'cold' everywhere.
	if err := c2.MovePartition(ctx, "default", "events", "202401", "cold", true, MoveOptions{PollInterval: 200 * time.Millisecond}); err != nil {
		t.Fatalf("move on ch2: %v", err)
	}
	requirePartitionOn(t, c2, "202401", "cold")

	// Data is intact on both replicas.
	if n := scalarInt(t, c1, "SELECT count() FROM default.events"); n != 1000 {
		t.Fatalf("ch1 rows after move: %d", n)
	}
	if n := scalarInt(t, c2, "SELECT count() FROM default.events"); n != 1000 {
		t.Fatalf("ch2 rows after move: %d", n)
	}
}
