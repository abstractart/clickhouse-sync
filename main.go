// Command clickhouse-sync moves a table partition to a destination disk on every
// node of a ClickHouse cluster (shards and their replicas).
//
// It discovers the cluster topology by querying system.clusters on an entry-point
// node, then connects to each node in turn over HTTPS and issues
// ALTER TABLE ... MOVE PARTITION ... TO DISK.
package main

import (
	"bufio"
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
	assumeYes       bool
	continueOnError bool
	timeout         time.Duration
	connectTimeout  time.Duration
	retries         int
	retryDelay      time.Duration
	nodesList       string
	pollInterval    time.Duration
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
	flag.BoolVar(&c.assumeYes, "yes", false, "Skip the interactive confirmation prompt (required for non-interactive runs)")
	flag.BoolVar(&c.continueOnError, "continue-on-error", false, "Keep going if a node fails instead of stopping")
	flag.DurationVar(&c.timeout, "timeout", 5*time.Minute, "Overall deadline for the whole MOVE on a node (kick-off + polling)")
	flag.DurationVar(&c.connectTimeout, "connect-timeout", 10*time.Second, "Timeout for establishing the connection; unreachable nodes fail fast")
	flag.IntVar(&c.retries, "retries", 2, "Number of retries on transient (network) errors, with exponential backoff")
	flag.DurationVar(&c.retryDelay, "retry-delay", 2*time.Second, "Base backoff before the first retry (doubles each attempt)")
	flag.DurationVar(&c.pollInterval, "poll-interval", 5*time.Second, "How often to poll MOVE progress on the server")
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

	// 2. Run the move on every node. Before each node we show that node's
	//    destination-disk capacity and (unless -dry-run or -yes) ask to confirm.
	//    Transient (network) errors are retried per node; ErrAlreadyOnTarget and
	//    query errors are final.
	stdin := bufio.NewReader(os.Stdin)
	assumeYes := cfg.assumeYes
	var failedNodes, declinedNodes []string
	var skipped int
	for i, host := range nodes {
		label := fmt.Sprintf("[%d/%d] %s", i+1, len(nodes), host)
		client := newClient(host)

		// Show this node's destination-disk capacity now and projected after the
		// move, so the decision to proceed is informed by that specific node.
		printNodeCapacity(ctx, cfg, client, host, policy)

		if cfg.dryRun {
			fmt.Printf("%s: (dry-run) would execute\n", label)
			continue
		}
		if !assumeYes {
			proceed, all, cerr := confirmNodeMove(host, stdin)
			if cerr != nil {
				return cerr // no readable stdin and not -yes: abort with guidance
			}
			if all {
				assumeYes = true // "yes to all": stop asking for the remaining nodes
			}
			if !proceed {
				declinedNodes = append(declinedNodes, host)
				fmt.Printf("%s: SKIPPED (declined)\n", label)
				continue
			}
		}

		err := clickhouse.Retry(ctx, policy, func() error {
			return moveOnNode(ctx, client, cfg)
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
	if len(declinedNodes) > 0 {
		// The operator skipped some nodes, so the partition is not on the target
		// disk everywhere. Report which, so they can re-run against just those.
		fmt.Fprintf(os.Stderr, "\nSkipped by user: %s\nRe-run against just these with: -nodes %s\n",
			strings.Join(declinedNodes, ","), strings.Join(declinedNodes, ","))
		return fmt.Errorf("%d of %d node(s) skipped by user; partition not relocated everywhere", len(declinedNodes), len(nodes))
	}
	fmt.Printf("\nDone: partition present on disk %q on all %d node(s) (%d moved, %d already there).\n",
		cfg.disk, len(nodes), len(nodes)-skipped, skipped)
	return nil
}

// moveOnNode performs the partition move on a single node. -timeout bounds the
// whole operation (kick-off + polling), not a single request.
func moveOnNode(ctx context.Context, client *clickhouse.Client, cfg config) error {
	opCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	return client.MovePartition(opCtx, cfg.database, cfg.table, cfg.partition, cfg.disk, cfg.partitionIsID,
		clickhouse.MoveOptions{PollInterval: cfg.pollInterval})
}

// printNodeCapacity reports one node's destination-disk capacity: free space,
// current usage %, and the usage % projected once the partition lands, plus the
// bytes that will actually move. It is best-effort: read failures are printed
// inline and do not abort (the move loop handles unreachable nodes), so the
// operator still sees a line for every node before deciding.
func printNodeCapacity(ctx context.Context, cfg config, client *clickhouse.Client, host string, policy clickhouse.RetryPolicy) {
	var di clickhouse.DiskInfo
	if err := clickhouse.Retry(ctx, policy, func() error {
		var e error
		di, e = client.DiskInfo(ctx, cfg.disk)
		return e
	}); err != nil {
		fmt.Printf("  %s: disk %q — could not read capacity: %v\n", host, cfg.disk, err)
		return
	}

	var moving uint64
	if err := clickhouse.Retry(ctx, policy, func() error {
		var e error
		moving, e = client.PartitionBytesToMove(ctx, cfg.database, cfg.table, cfg.partition, cfg.partitionIsID, cfg.disk)
		return e
	}); err != nil {
		fmt.Printf("  %s: disk %q — free %s / %s, used %.1f%% (partition size unknown: %v)\n",
			host, cfg.disk, humanBytes(di.Free), humanBytes(di.Total), percent(di.Used(), di.Total), err)
		return
	}

	used := di.Used()
	line := fmt.Sprintf("  %s: disk %q — free %s / %s, used %.1f%% -> %.1f%% after moving %s",
		host, cfg.disk, humanBytes(di.Free), humanBytes(di.Total),
		percent(used, di.Total), percent(used+moving, di.Total), humanBytes(moving))
	if moving > di.Free {
		line += "  !! MOVE needs more than the free space"
	}
	fmt.Println(line)
}

// confirmNodeMove asks whether to move on a single node. It returns proceed
// (move this node), all (proceed on this and every remaining node without asking
// again), and an error only when stdin cannot be read — e.g. a non-interactive
// run without -yes — so the caller aborts with guidance instead of hanging or
// assuming yes. Answers: y/yes -> proceed; a/all -> proceed on all; anything
// else (including empty) -> skip this node.
func confirmNodeMove(host string, r *bufio.Reader) (proceed, all bool, err error) {
	fmt.Printf("  Proceed on %s? [y/N/a=yes to all]: ", host)
	line, rerr := r.ReadString('\n')
	if rerr != nil && line == "" {
		return false, false, fmt.Errorf("aborted: could not read confirmation (%v); pass -yes for non-interactive runs", rerr)
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true, false, nil
	case "a", "all":
		return true, true, nil
	default:
		return false, false, nil
	}
}

// percent returns part/total as a percentage, guarding against a zero total.
func percent(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// humanBytes formats a byte count with a binary (KiB/MiB/...) suffix.
func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
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
