package clickhouse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
				return fmt.Errorf("MOVE did not finish within the deadline and may still be running server-side (query_id=%s): %w", queryID, ctx.Err())
			default:
				return err
			}
		case <-ticker.C:
			if running, err := c.queryRunning(ctx, queryID, opts.StatusTimeout); err == nil && running {
				fmt.Printf("    MOVE in progress (query_id=%s)\n", queryID)
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

// newQueryID returns a random 128-bit hex identifier.
func newQueryID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
