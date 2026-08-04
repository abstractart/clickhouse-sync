package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// MoveOptions tunes MovePartition.
type MoveOptions struct {
	// PollInterval is how often the status-watching goroutine checks progress.
	PollInterval time.Duration
	// StatusTimeout bounds each individual status query.
	StatusTimeout time.Duration
}

func (o MoveOptions) withDefaults() MoveOptions {
	if o.PollInterval <= 0 {
		o.PollInterval = 5 * time.Second
	}
	if o.StatusTimeout <= 0 {
		o.StatusTimeout = 15 * time.Second
	}
	return o
}

// MovePartition runs the ALTER ... MOVE PARTITION in a dedicated goroutine and,
// concurrently, watches its progress from the calling goroutine by polling
// system.processes. This keeps a long-running move (e.g. to object storage) off a
// single long-lived request while still reporting liveness; the authoritative
// result comes straight from the MOVE request itself, so no log/parts inspection
// is needed.
//
// The overall deadline is taken from ctx (set it with context.WithTimeout).
// Behaviour:
//   - node unreachable       -> TransportError (transient; the caller may retry);
//   - MOVE succeeds          -> nil;
//   - already on target      -> ErrAlreadyOnTarget;
//   - MOVE fails on server   -> the ClickHouse error;
//   - ctx deadline hit       -> error noting the MOVE may still be running
//     server-side (NOT transient, so it is not blindly retried).
func (c *Client) MovePartition(ctx context.Context, database, table, partition, disk string, partitionIsID bool, opts MoveOptions) error {
	opts = opts.withDefaults()

	queryID, err := newQueryID()
	if err != nil {
		return fmt.Errorf("generating query id: %w", err)
	}
	sql := MovePartitionSQL(database, table, partition, disk, partitionIsID)

	// Goroutine 1: execute the MOVE and report its definitive result. The channel
	// is buffered so this goroutine never blocks, even if we have already returned.
	resultCh := make(chan error, 1)
	go func() {
		_, execErr := c.execWithID(ctx, sql, queryID)
		resultCh <- execErr
	}()

	// Goroutine 2 (this one): watch progress until the MOVE goroutine finishes.
	ticker := time.NewTicker(opts.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case err := <-resultCh:
			switch {
			case err == nil:
				return nil
			case isAlreadyOnTarget(err):
				return ErrAlreadyOnTarget
			case ctx.Err() != nil:
				// The request ended because our deadline/cancel fired; the ALTER
				// itself is not cancelled server-side and likely keeps running.
				return fmt.Errorf("MOVE did not finish within the deadline and may still be running server-side on %s (query_id=%s): %w", c.addr, queryID, ctx.Err())
			default:
				return err
			}
		case <-ticker.C:
			if running, err := c.queryRunning(ctx, queryID, opts.StatusTimeout); err == nil && running {
				msg := fmt.Sprintf("    MOVE in progress on %s (query_id=%s)", c.addr, queryID)
				if detail := c.moveProgress(ctx, database, table, opts.StatusTimeout); detail != "" {
					msg += " — " + detail
				}
				fmt.Println(msg)
			}
		}
	}
}

// queryRunning reports whether a query with the given id is currently executing.
func (c *Client) queryRunning(ctx context.Context, queryID string, timeout time.Duration) (bool, error) {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	vals, err := c.QueryColumn(qctx, "SELECT count() FROM system.processes WHERE query_id = "+quoteLiteral(queryID))
	if err != nil {
		return false, err
	}
	return len(vals) == 1 && vals[0] != "0", nil
}

// moveProgress returns a best-effort, human-readable summary of the part moves
// currently active for the given table, taken from system.moves. Unlike the
// liveness probe (which matches our exact query_id in system.processes),
// system.moves has no query_id column, so this is matched by database/table
// only: it may occasionally reflect an unrelated concurrent move of the same
// table. It is used purely to enrich the progress log, never to decide the
// operation's outcome, so that imprecision is acceptable. Any error (including
// older servers without system.moves) yields an empty string, and the caller
// simply logs the plain liveness line.
func (c *Client) moveProgress(ctx context.Context, database, table string, timeout time.Duration) string {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	q := "SELECT count(), round(max(elapsed), 1), arrayStringConcat(groupUniqArray(target_disk_name), ',') " +
		"FROM system.moves WHERE database = " + quoteLiteral(database) +
		" AND table = " + quoteLiteral(table)
	out, err := c.Exec(qctx, q+"\nFORMAT TabSeparated")
	if err != nil {
		return ""
	}
	fields := strings.Split(strings.TrimSpace(out), "\t")
	if len(fields) < 3 || fields[0] == "0" || fields[0] == "" {
		return ""
	}
	return fmt.Sprintf("%s part(s) → disk %s, elapsed %ss", fields[0], fields[2], fields[1])
}

// newQueryID returns a random 128-bit hex identifier.
func newQueryID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
