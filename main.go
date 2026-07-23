// Command clickhouse-sync moves a table partition to a destination disk on every
// node of a ClickHouse cluster (shards and their replicas).
//
// It discovers the cluster topology by querying system.clusters on an entry-point
// node, then connects to each node in turn over HTTPS and issues
// ALTER TABLE ... MOVE PARTITION ... TO DISK.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"clickhouse-sync/internal/clickhouse"
)

type config struct {
	user            string
	password        string
	hostname        string
	port            int
	database        string
	table           string
	partition       string
	disk            string
	cluster         string
	partitionIsID   bool
	insecure        bool
	dryRun          bool
	continueOnError bool
	timeout         time.Duration
}

func parseFlags() (config, error) {
	var c config
	flag.StringVar(&c.user, "user", "", "ClickHouse username (required)")
	flag.StringVar(&c.password, "password", "", "ClickHouse password (defaults to $CLICKHOUSE_PASSWORD)")
	flag.StringVar(&c.hostname, "hostname", "", "Entry-point node hostname used to discover the cluster (required)")
	flag.IntVar(&c.port, "port", 8443, "ClickHouse HTTPS port")
	flag.StringVar(&c.database, "database", "", "Database name (required)")
	flag.StringVar(&c.table, "table", "", "Table name (required)")
	flag.StringVar(&c.partition, "partition", "", "Partition value (or id, see -partition-id) to move (required)")
	flag.StringVar(&c.disk, "destination-disk", "", "Destination disk name (required)")
	flag.StringVar(&c.cluster, "cluster", "", "Cluster name in system.clusters; empty means all known nodes")
	flag.BoolVar(&c.partitionIsID, "partition-id", false, "Treat -partition as a partition id (MOVE PARTITION ID)")
	flag.BoolVar(&c.insecure, "insecure", false, "Skip TLS certificate verification")
	flag.BoolVar(&c.dryRun, "dry-run", false, "Print the statements without executing the moves")
	flag.BoolVar(&c.continueOnError, "continue-on-error", false, "Keep going if a node fails instead of stopping")
	flag.DurationVar(&c.timeout, "timeout", 5*time.Minute, "Per-request timeout")
	flag.Parse()

	if c.password == "" {
		c.password = os.Getenv("CLICKHOUSE_PASSWORD")
	}

	var missing []string
	for name, val := range map[string]string{
		"user":             c.user,
		"hostname":         c.hostname,
		"database":         c.database,
		"table":            c.table,
		"partition":        c.partition,
		"destination-disk": c.disk,
	} {
		if val == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing required flag(s): %v", missing)
	}
	return c, nil
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config) error {
	newClient := func(host string) *clickhouse.Client {
		return clickhouse.New(clickhouse.Options{
			Host:               host,
			Port:               cfg.port,
			User:               cfg.user,
			Password:           cfg.password,
			InsecureSkipVerify: cfg.insecure,
			Timeout:            cfg.timeout,
		})
	}

	// 1. Discover the cluster nodes via the system table on the entry point.
	entry := newClient(cfg.hostname)
	fmt.Printf("Discovering cluster nodes via %s ...\n", cfg.hostname)
	nodes, err := entry.ClusterNodes(ctx, cfg.cluster)
	if err != nil {
		return fmt.Errorf("discovering cluster nodes: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes found in system.clusters (cluster=%q)", cfg.cluster)
	}
	fmt.Printf("Found %d node(s): %v\n", len(nodes), nodes)

	sql := clickhouse.MovePartitionSQL(cfg.database, cfg.table, cfg.partition, cfg.disk, cfg.partitionIsID)
	fmt.Printf("Statement: %s\n\n", sql)

	// 2. Run the move on every node.
	var failures, skipped int
	for i, host := range nodes {
		label := fmt.Sprintf("[%d/%d] %s", i+1, len(nodes), host)
		if cfg.dryRun {
			fmt.Printf("%s: (dry-run) would execute\n", label)
			continue
		}
		err := newClient(host).MovePartition(ctx, cfg.database, cfg.table, cfg.partition, cfg.disk, cfg.partitionIsID)
		switch {
		case err == nil:
			fmt.Printf("%s: OK\n", label)
		case errors.Is(err, clickhouse.ErrAlreadyOnTarget):
			// Idempotent no-op: the partition is already on the destination disk.
			skipped++
			fmt.Printf("%s: SKIP (already on disk %q)\n", label, cfg.disk)
		default:
			failures++
			fmt.Printf("%s: FAILED: %v\n", label, err)
			if !cfg.continueOnError {
				return fmt.Errorf("aborting after failure on %s (use -continue-on-error to keep going)", host)
			}
		}
	}

	if failures > 0 {
		return fmt.Errorf("%d of %d node(s) failed", failures, len(nodes))
	}
	fmt.Printf("\nDone: partition present on disk %q on all %d node(s) (%d moved, %d already there).\n",
		cfg.disk, len(nodes), len(nodes)-skipped, skipped)
	return nil
}
