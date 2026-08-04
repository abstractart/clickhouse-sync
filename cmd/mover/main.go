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

	fmt.Printf("Moving partition %q of %s.%s to disk %q, part by part.\n\n", cfg.partition, cfg.database, cfg.table, cfg.disk)

	// 2. On every node, enumerate the partition's parts and move them one at a
	//    time. Before each part we show the node's destination-disk capacity and
	//    (unless -dry-run or -yes) ask to confirm that specific part. Transient
	//    (network) errors are retried; ErrAlreadyOnTarget and ErrPartGone are
	//    treated as skips; other query errors are final.
	stdin := bufio.NewReader(os.Stdin)
	assumeYes := cfg.assumeYes
	var failedNodes, declinedNodes []string
	var moved, skipped int
	for i, host := range nodes {
		nodeLabel := fmt.Sprintf("[node %d/%d] %s", i+1, len(nodes), host)
		client := newClient(host)

		var parts []clickhouse.Part
		if err := clickhouse.Retry(ctx, policy, func() error {
			var e error
			parts, e = client.PartitionParts(ctx, cfg.database, cfg.table, cfg.partition, cfg.partitionIsID, cfg.disk)
			return e
		}); err != nil {
			failedNodes = appendUnique(failedNodes, host)
			fmt.Printf("%s: FAILED to list parts: %v\n", nodeLabel, err)
			if !cfg.continueOnError {
				return abort(failedNodes, "listing parts on "+host)
			}
			continue
		}
		if len(parts) == 0 {
			fmt.Printf("%s: nothing to move (no parts outside disk %q)\n", nodeLabel, cfg.disk)
			continue
		}
		fmt.Printf("%s: %d part(s), %s to move to disk %q\n", nodeLabel, len(parts), humanBytes(totalBytes(parts)), cfg.disk)

		for j, part := range parts {
			partLabel := fmt.Sprintf("  [part %d/%d] %s", j+1, len(parts), part.Name)

			// Re-read the disk before each part so free space reflects the parts
			// already moved on this node.
			printPartCapacity(ctx, cfg, client, host, part, policy)

			// Show the exact statement that will run for this part, so it is
			// visible before the confirmation (and in dry-run).
			sql := clickhouse.MovePartSQL(cfg.database, cfg.table, part.Name, cfg.disk)
			fmt.Printf("      SQL: %s\n", sql)

			if cfg.dryRun {
				fmt.Printf("%s: (dry-run) would run the SQL above\n", partLabel)
				continue
			}
			if !assumeYes {
				proceed, all, cerr := confirmPartMove(host, part.Name, stdin)
				if cerr != nil {
					return cerr // no readable stdin and not -yes: abort with guidance
				}
				if all {
					assumeYes = true // "yes to all": stop asking for the rest
				}
				if !proceed {
					declinedNodes = appendUnique(declinedNodes, host)
					fmt.Printf("%s: SKIPPED (declined)\n", partLabel)
					continue
				}
			}

			err := clickhouse.Retry(ctx, policy, func() error {
				return movePartOnNode(ctx, client, cfg, part.Name)
			})
			switch {
			case err == nil:
				moved++
				fmt.Printf("%s: OK (moved from disk %q to %q)\n", partLabel, part.Disk, cfg.disk)
			case errors.Is(err, clickhouse.ErrAlreadyOnTarget):
				skipped++
				fmt.Printf("%s: SKIP (already on disk %q)\n", partLabel, cfg.disk)
			case errors.Is(err, clickhouse.ErrPartGone):
				// Merged away between listing and moving; a re-run moves the merged
				// result. Not a failure.
				skipped++
				fmt.Printf("%s: SKIP (part gone, likely merged; re-run to move its data)\n", partLabel)
			default:
				failedNodes = appendUnique(failedNodes, host)
				fmt.Printf("%s: FAILED: %v\n", partLabel, err)
				if !cfg.continueOnError {
					return abort(failedNodes, "part "+part.Name+" on "+host)
				}
			}
		}
	}

	if cfg.dryRun {
		fmt.Printf("\nDry-run complete: no changes made.\n")
		return nil
	}
	if len(failedNodes) > 0 {
		// Surface the affected nodes so the operator can re-drive just those once
		// they recover; per-part moves are idempotent and re-enumerate, so a
		// re-run is safe and picks up exactly what remains.
		fmt.Fprintf(os.Stderr, "\nNode(s) with failed part move(s): %s\nRe-run against just these with: -nodes %s\n",
			strings.Join(failedNodes, ","), strings.Join(failedNodes, ","))
		return fmt.Errorf("part move(s) failed on %d node(s) (%d moved, %d skipped)", len(failedNodes), moved, skipped)
	}
	if len(declinedNodes) > 0 {
		fmt.Fprintf(os.Stderr, "\nNode(s) with skipped part(s): %s\nRe-run against just these with: -nodes %s\n",
			strings.Join(declinedNodes, ","), strings.Join(declinedNodes, ","))
		return fmt.Errorf("some part(s) skipped by user on %d node(s); partition not relocated everywhere", len(declinedNodes))
	}
	fmt.Printf("\nDone: %d part(s) moved to disk %q, %d already there, across %d node(s).\n",
		moved, cfg.disk, skipped, len(nodes))
	return nil
}

// abort prints the failed-node hint and returns the aborting error used when
// -continue-on-error is not set.
func abort(failedNodes []string, what string) error {
	fmt.Fprintf(os.Stderr, "\nNode(s) with failed part move(s): %s\nRe-run against just these with: -nodes %s\n",
		strings.Join(failedNodes, ","), strings.Join(failedNodes, ","))
	return fmt.Errorf("aborting after failure (%s); use -continue-on-error to keep going", what)
}

// totalBytes sums the on-disk size of the given parts.
func totalBytes(parts []clickhouse.Part) uint64 {
	var n uint64
	for _, p := range parts {
		n += p.Bytes
	}
	return n
}

// appendUnique appends v to list only if not already present, preserving order.
func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}

// movePartOnNode moves a single named part on one node. -timeout bounds the whole
// operation (kick-off + polling), not a single request.
func movePartOnNode(ctx context.Context, client *clickhouse.Client, cfg config, part string) error {
	opCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	return client.MovePart(opCtx, cfg.database, cfg.table, part, cfg.disk,
		clickhouse.MoveOptions{PollInterval: cfg.pollInterval})
}

// printPartCapacity reports the destination-disk capacity on this node just
// before a part is moved: free space, current usage %, and the usage % projected
// once this part lands, alongside the part's size. It is best-effort: a read
// failure is printed inline and does not abort (the move itself will surface a
// real problem), so the operator still sees a line before each decision.
func printPartCapacity(ctx context.Context, cfg config, client *clickhouse.Client, host string, part clickhouse.Part, policy clickhouse.RetryPolicy) {
	var di clickhouse.DiskInfo
	if err := clickhouse.Retry(ctx, policy, func() error {
		var e error
		di, e = client.DiskInfo(ctx, cfg.disk)
		return e
	}); err != nil {
		fmt.Printf("    %s (%s): move from disk %q to %q — could not read capacity: %v\n", part.Name, humanBytes(part.Bytes), part.Disk, cfg.disk, err)
		return
	}

	used := di.Used()
	line := fmt.Sprintf("    %s (%s): move from disk %q to %q — free %s / %s, used %.1f%% -> %.1f%% after this part",
		part.Name, humanBytes(part.Bytes), part.Disk, cfg.disk, humanBytes(di.Free), humanBytes(di.Total),
		percent(used, di.Total), percent(used+part.Bytes, di.Total))
	if part.Bytes > di.Free {
		line += "  !! part is larger than the free space"
	}
	fmt.Println(line)
}

// confirmPartMove asks whether to move a single part. It returns proceed (move
// this part), all (proceed on this and every remaining part without asking
// again), and an error only when stdin cannot be read — e.g. a non-interactive
// run without -yes — so the caller aborts with guidance instead of hanging or
// assuming yes. Answers: y/yes -> proceed; a/all -> proceed on all; anything
// else (including empty) -> skip this part.
func confirmPartMove(host, part string, r *bufio.Reader) (proceed, all bool, err error) {
	fmt.Printf("    Move part %s on %s? [y/N/a=yes to all]: ", part, host)
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
