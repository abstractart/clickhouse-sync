// Command fscache demonstrates ClickHouse's filesystem_cache as an alternative to
// physically moving cold parts back onto a hot disk.
//
// The idea: a table lives on an S3-backed disk that is wrapped by a local `cache`
// disk (see the `cached` storage policy in deploy). Data stays on S3, but the
// first read pulls file segments over the network and caches them on the local
// fast disk; every later read of those segments is served locally. So instead of
// `ALTER TABLE ... MOVE PART`, hot data ends up on a fast disk automatically after
// it is first queried.
//
// This program makes that observable: it creates a table on the cached policy,
// drops the filesystem cache, runs the same scan twice, and reports — from
// system.query_log ProfileEvents — how the first (cold) read comes from S3 while
// the second (warm) read comes from the local cache.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"clickhouse-sync/internal/clickhouse"
)

var version = "dev"

type config struct {
	user     string
	password string
	hostname string
	port     int
	database string
	table    string
	policy   string
	rows     int
	insecure bool
	reuse    bool
	timeout  time.Duration
}

func parseFlags() (config, error) {
	var c config
	flag.StringVar(&c.user, "user", "default", "ClickHouse username")
	flag.StringVar(&c.password, "password", "", "ClickHouse password (defaults to $CLICKHOUSE_PASSWORD)")
	flag.StringVar(&c.hostname, "hostname", "clickhouse-s1r1", "ClickHouse node to run the demo against")
	flag.IntVar(&c.port, "port", 8443, "ClickHouse HTTPS port")
	flag.StringVar(&c.database, "database", "demo", "Database for the demo table")
	flag.StringVar(&c.table, "table", "fscache_demo", "Demo table name")
	flag.StringVar(&c.policy, "policy", "cached", "Storage policy backed by an S3 + cache disk")
	flag.IntVar(&c.rows, "rows", 1_000_000, "Rows to insert (each ~100 bytes of payload)")
	flag.BoolVar(&c.insecure, "insecure", true, "Skip TLS certificate verification")
	flag.BoolVar(&c.reuse, "reuse", false, "Reuse existing table data instead of recreating it")
	flag.DurationVar(&c.timeout, "timeout", 10*time.Minute, "Overall request timeout")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("fscache %s\n", version)
		os.Exit(0)
	}
	if c.password == "" {
		c.password = os.Getenv("CLICKHOUSE_PASSWORD")
	}
	return c, nil
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
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
	client := clickhouse.New(clickhouse.Options{
		Host:               cfg.hostname,
		Port:               cfg.port,
		User:               cfg.user,
		Password:           cfg.password,
		InsecureSkipVerify: cfg.insecure,
		Timeout:            cfg.timeout,
	})
	if err := client.Ping(ctx); err != nil {
		return fmt.Errorf("connecting to %s:%d: %w", cfg.hostname, cfg.port, err)
	}
	qt := qualified(cfg.database, cfg.table)

	// 1. Make sure the demo data exists on the cached (S3-backed) policy.
	if err := ensureData(ctx, client, cfg, qt); err != nil {
		return err
	}

	// 2. Start from an empty cache so the first scan is a genuine cold read.
	fmt.Println("\nDropping the filesystem cache to force a cold read ...")
	if _, err := client.Exec(ctx, "SYSTEM DROP FILESYSTEM CACHE"); err != nil {
		return fmt.Errorf("dropping filesystem cache: %w", err)
	}

	// 3. Same scan twice: cold (served from S3) then warm (served from cache).
	token := strconv.FormatInt(time.Now().UnixNano(), 36)
	coldComment := "fscache_cold_" + token
	warmComment := "fscache_warm_" + token

	fmt.Println("\nRunning cold scan (cache empty, reads from S3) ...")
	coldWall, err := scan(ctx, client, qt, coldComment)
	if err != nil {
		return fmt.Errorf("cold scan: %w", err)
	}
	fmt.Printf("  wall time: %s\n", coldWall.Round(time.Millisecond))

	fmt.Println("Running warm scan (same query, reads from local cache) ...")
	warmWall, err := scan(ctx, client, qt, warmComment)
	if err != nil {
		return fmt.Errorf("warm scan: %w", err)
	}
	fmt.Printf("  wall time: %s\n", warmWall.Round(time.Millisecond))

	// 4. Pull the authoritative per-query stats from query_log.
	if _, err := client.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return fmt.Errorf("flushing logs: %w", err)
	}
	cold, err := readStats(ctx, client, coldComment)
	if err != nil {
		return fmt.Errorf("reading cold stats: %w", err)
	}
	warm, err := readStats(ctx, client, warmComment)
	if err != nil {
		return fmt.Errorf("reading warm stats: %w", err)
	}

	report(cold, warm)
	return printCacheOccupancy(ctx, client)
}

// ensureData creates the demo table on the cached policy and fills it, unless
// -reuse was given and the table already holds rows.
func ensureData(ctx context.Context, client *clickhouse.Client, cfg config, qt string) error {
	if cfg.reuse {
		if n, err := scalarInt(ctx, client, "SELECT count() FROM "+qt); err == nil && n > 0 {
			fmt.Printf("Reusing existing %s (%d rows).\n", qt, n)
			return nil
		}
	}

	fmt.Printf("Creating %s on storage policy %q and inserting %d rows ...\n", qt, cfg.policy, cfg.rows)
	if _, err := client.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(cfg.database)); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	if _, err := client.Exec(ctx, "DROP TABLE IF EXISTS "+qt+" SYNC"); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	create := fmt.Sprintf(`CREATE TABLE %s (id UInt64, d Date, payload String)
		ENGINE = MergeTree ORDER BY id
		SETTINGS storage_policy = %s`, qt, literal(cfg.policy))
	if _, err := client.Exec(ctx, create); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	insert := fmt.Sprintf(`INSERT INTO %s
		SELECT number, toDate('2024-01-01') + (number %% 365), randomPrintableASCII(100)
		FROM numbers(%d)`, qt, cfg.rows)
	if _, err := client.Exec(ctx, insert); err != nil {
		return fmt.Errorf("inserting rows: %w", err)
	}
	size, _ := scalar(ctx, client,
		"SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE active AND database="+
			literalFromQualified(cfg.database, cfg.table))
	fmt.Printf("Inserted; on-disk size ~%s (on S3, read through the cache).\n", size)
	return nil
}

// scan runs the full-column scan tagged with a log_comment so it can be located
// in query_log, and returns the client-side wall time.
func scan(ctx context.Context, client *clickhouse.Client, qt, comment string) (time.Duration, error) {
	// cityHash64 over the payload forces reading the whole column data (unlike
	// length(), which only needs the String size substream), so the cold read
	// actually pulls the bytes from S3 and warms the cache.
	q := fmt.Sprintf("SELECT count(), sum(cityHash64(payload)) FROM %s SETTINGS log_comment = %s", qt, literal(comment))
	start := time.Now()
	_, err := client.Exec(ctx, q)
	return time.Since(start), err
}

type stats struct {
	durationMs  string
	cacheBytes  string
	sourceBytes string
	s3Gets      string
}

// readStats fetches the filesystem-cache ProfileEvents for the query identified
// by its log_comment.
func readStats(ctx context.Context, client *clickhouse.Client, comment string) (stats, error) {
	q := `SELECT
			toString(query_duration_ms),
			formatReadableSize(ProfileEvents['CachedReadBufferReadFromCacheBytes']),
			formatReadableSize(ProfileEvents['CachedReadBufferReadFromSourceBytes']),
			toString(ProfileEvents['S3GetObject'])
		FROM system.query_log
		WHERE type = 'QueryFinish' AND log_comment = ` + literal(comment) + `
		ORDER BY event_time_microseconds DESC
		LIMIT 1`
	row, err := queryRow(ctx, client, q)
	if err != nil {
		return stats{}, err
	}
	if len(row) < 4 {
		return stats{}, fmt.Errorf("query_log row not found for %q (got %v)", comment, row)
	}
	return stats{durationMs: row[0], cacheBytes: row[1], sourceBytes: row[2], s3Gets: row[3]}, nil
}

func report(cold, warm stats) {
	fmt.Println("\nResult — same scan, cache dropped in between:")
	fmt.Printf("  %-22s %18s %18s\n", "", "COLD (1st read)", "WARM (2nd read)")
	fmt.Printf("  %-22s %18s %18s\n", "server duration (ms)", cold.durationMs, warm.durationMs)
	fmt.Printf("  %-22s %18s %18s\n", "read from S3 (source)", cold.sourceBytes, warm.sourceBytes)
	fmt.Printf("  %-22s %18s %18s\n", "read from cache", cold.cacheBytes, warm.cacheBytes)
	fmt.Printf("  %-22s %18s %18s\n", "S3 GET requests", cold.s3Gets, warm.s3Gets)
	fmt.Println("\nThe cold scan pulls bytes from S3 and fills the cache; the warm scan")
	fmt.Println("serves the same data from the local cache with (near) zero S3 traffic —")
	fmt.Println("hot data ends up on a fast local disk without moving any part.")
}

func printCacheOccupancy(ctx context.Context, client *clickhouse.Client) error {
	row, err := queryRow(ctx, client,
		"SELECT toString(count()), formatReadableSize(sum(size)) FROM system.filesystem_cache")
	if err != nil {
		return fmt.Errorf("reading system.filesystem_cache: %w", err)
	}
	if len(row) >= 2 {
		fmt.Printf("\nFilesystem cache now holds %s segment(s), %s.\n", row[0], row[1])
	}
	return nil
}

// --- small query helpers ---------------------------------------------------

func queryRow(ctx context.Context, client *clickhouse.Client, query string) ([]string, error) {
	out, err := client.Exec(ctx, query+"\nFORMAT TabSeparated")
	if err != nil {
		return nil, err
	}
	line := strings.SplitN(strings.TrimRight(out, "\n"), "\n", 2)[0]
	if line == "" {
		return nil, nil
	}
	return strings.Split(line, "\t"), nil
}

func scalar(ctx context.Context, client *clickhouse.Client, query string) (string, error) {
	row, err := queryRow(ctx, client, query)
	if err != nil || len(row) == 0 {
		return "", err
	}
	return row[0], nil
}

func scalarInt(ctx context.Context, client *clickhouse.Client, query string) (int64, error) {
	s, err := scalar(ctx, client, query)
	if err != nil || s == "" {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func qualified(database, table string) string { return ident(database) + "." + ident(table) }

func ident(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func literal(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// literalFromQualified builds the "database=... AND table=..." predicate values.
func literalFromQualified(database, table string) string {
	return literal(database) + " AND table = " + literal(table)
}
