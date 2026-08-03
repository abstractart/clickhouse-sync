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
	"strings"
	"syscall"
	"time"

	"clickhouse-sync/internal/clickhouse"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

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
	connectTimeout  time.Duration
	retries         int
	retryDelay      time.Duration
	nodesList       string
}

func parseFlags() (config, error) {
	var c config
	flag.StringVar(&c.user, "user", "", "ClickHouse username (required)")
	flag.StringVar(&c.password, "password", "", "ClickHouse password (defaults to $CLICKHOUSE_PASSWORD)")
	flag.StringVar(&c.hostname, "hostname", "", "Entry-point node hostname used to discover the cluster (required unless -nodes is given)")
	flag.StringVar(&c.nodesList, "nodes", "", "Comma-separated explicit node list to operate on; skips discovery (use to re-run against previously failed nodes)")
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
	flag.DurationVar(&c.timeout, "timeout", 5*time.Minute, "Overall per-request timeout, covering the MOVE operation")
	flag.DurationVar(&c.connectTimeout, "connect-timeout", 10*time.Second, "Timeout for establishing the connection; unreachable nodes fail fast")
	flag.IntVar(&c.retries, "retries", 2, "Number of retries on transient (network) errors, with exponential backoff")
	flag.DurationVar(&c.retryDelay, "retry-delay", 2*time.Second, "Base backoff before the first retry (doubles each attempt)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("clickhouse-sync %s\n", version)
		os.Exit(0)
	}

	if c.password == "" {
		c.password = os.Getenv("CLICKHOUSE_PASSWORD")
	}

	var missing []string
	for name, val := range map[string]string{
		"user":             c.user,
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
	// Node source: either discover via -hostname, or an explicit -nodes list.
	if c.hostname == "" && c.nodesList == "" {
		return c, fmt.Errorf("either -hostname (for discovery) or -nodes (explicit list) is required")
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
			ConnectTimeout:     cfg.connectTimeout,
		})
	}
	policy := clickhouse.RetryPolicy{Retries: cfg.retries, Delay: cfg.retryDelay}

	// 1. Determine the target nodes: an explicit -nodes list, or discovery via
	//    the entry point. Transient failures during discovery are retried.
	nodes, err := targetNodes(ctx, cfg, newClient, policy)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes to operate on")
	}
	fmt.Printf("Operating on %d node(s): %v\n", len(nodes), nodes)

	sql := clickhouse.MovePartitionSQL(cfg.database, cfg.table, cfg.partition, cfg.disk, cfg.partitionIsID)
	fmt.Printf("Statement: %s\n\n", sql)

	// 2. Run the move on every node. Transient (network) errors are retried per
	//    node; ErrAlreadyOnTarget and query errors are final.
	var failedNodes []string
	var skipped int
	for i, host := range nodes {
		label := fmt.Sprintf("[%d/%d] %s", i+1, len(nodes), host)
		if cfg.dryRun {
			fmt.Printf("%s: (dry-run) would execute\n", label)
			continue
		}
		client := newClient(host)
		err := clickhouse.Retry(ctx, policy, func() error {
			return client.MovePartition(ctx, cfg.database, cfg.table, cfg.partition, cfg.disk, cfg.partitionIsID)
		})
		switch {
		case err == nil:
			fmt.Printf("%s: OK\n", label)
		case errors.Is(err, clickhouse.ErrAlreadyOnTarget):
			// Idempotent no-op: the partition is already on the destination disk.
			skipped++
			fmt.Printf("%s: SKIP (already on disk %q)\n", label, cfg.disk)
		default:
			failedNodes = append(failedNodes, host)
			fmt.Printf("%s: FAILED: %v\n", label, err)
			if !cfg.continueOnError {
				fmt.Fprintf(os.Stderr, "\nFailed node(s): %s\nRe-run against just these with: -nodes %s\n",
					strings.Join(failedNodes, ","), strings.Join(failedNodes, ","))
				return fmt.Errorf("aborting after failure on %s (use -continue-on-error to keep going)", host)
			}
		}
	}

	if len(failedNodes) > 0 {
		// Surface the failed nodes so the operator can re-drive just those once
		// they recover; the move is idempotent, so a full re-run is also safe.
		fmt.Fprintf(os.Stderr, "\nFailed node(s): %s\nRe-run against just these with: -nodes %s\n",
			strings.Join(failedNodes, ","), strings.Join(failedNodes, ","))
		return fmt.Errorf("%d of %d node(s) failed", len(failedNodes), len(nodes))
	}
	fmt.Printf("\nDone: partition present on disk %q on all %d node(s) (%d moved, %d already there).\n",
		cfg.disk, len(nodes), len(nodes)-skipped, skipped)
	return nil
}

// targetNodes returns the nodes to operate on: the explicit -nodes list when
// given, otherwise the cluster topology discovered via the entry point.
func targetNodes(ctx context.Context, cfg config, newClient func(string) *clickhouse.Client, policy clickhouse.RetryPolicy) ([]string, error) {
	if cfg.nodesList != "" {
		nodes := splitNodes(cfg.nodesList)
		fmt.Printf("Using %d explicitly provided node(s), skipping discovery.\n", len(nodes))
		return nodes, nil
	}

	entry := newClient(cfg.hostname)
	fmt.Printf("Discovering cluster nodes via %s ...\n", cfg.hostname)
	var nodes []string
	err := clickhouse.Retry(ctx, policy, func() error {
		var e error
		nodes, e = entry.ClusterNodes(ctx, cfg.cluster)
		return e
	})
	if err != nil {
		return nil, fmt.Errorf("discovering cluster nodes: %w", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes found in system.clusters (cluster=%q)", cfg.cluster)
	}
	return nodes, nil
}

// splitNodes parses a comma-separated node list, trimming spaces and dropping
// empty entries.
func splitNodes(s string) []string {
	var nodes []string
	for _, part := range strings.Split(s, ",") {
		if host := strings.TrimSpace(part); host != "" {
			nodes = append(nodes, host)
		}
	}
	return nodes
}
