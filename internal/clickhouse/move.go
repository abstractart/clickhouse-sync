package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrAlreadyOnTarget is returned by a move when the part (or every part of the
// partition) already resides on the destination disk. It makes the move
// idempotent: re-running it is a no-op instead of an error.
var ErrAlreadyOnTarget = errors.New("already on destination disk")

// ErrPartGone is returned by MovePart when the part no longer exists on the
// server — typically because a background merge combined it into a larger part
// between listing and moving. The data is not lost; a re-run re-enumerates the
// active parts and moves the merged result.
var ErrPartGone = errors.New("part no longer exists (likely merged)")

// ClusterNodes returns the distinct host names of every node in the cluster.
// If cluster is empty, nodes from all clusters known to the server are returned.
func (c *Client) ClusterNodes(ctx context.Context, cluster string) ([]string, error) {
	query := "SELECT DISTINCT host_name FROM system.clusters"
	if cluster != "" {
		query += " WHERE cluster = " + quoteLiteral(cluster)
	}
	query += " ORDER BY host_name"
	return c.QueryColumn(ctx, query)
}

// MovePartitionSQL builds the ALTER TABLE ... MOVE PARTITION ... TO DISK statement.
//
// partitionIsID controls how the partition is referenced: when true it is
// treated as a partition id (MOVE PARTITION ID '...'), otherwise as a partition
// expression value (MOVE PARTITION '...').
func MovePartitionSQL(database, table, partition, disk string, partitionIsID bool) string {
	target := quoteIdentifier(database) + "." + quoteIdentifier(table)
	clause := "PARTITION " + quoteLiteral(partition)
	if partitionIsID {
		clause = "PARTITION ID " + quoteLiteral(partition)
	}
	return fmt.Sprintf("ALTER TABLE %s MOVE %s TO DISK %s", target, clause, quoteLiteral(disk))
}

// MovePartSQL builds the ALTER TABLE ... MOVE PART ... TO DISK statement for a
// single, named data part.
func MovePartSQL(database, table, part, disk string) string {
	target := quoteIdentifier(database) + "." + quoteIdentifier(table)
	return fmt.Sprintf("ALTER TABLE %s MOVE PART %s TO DISK %s", target, quoteLiteral(part), quoteLiteral(disk))
}

// isAlreadyOnTarget reports whether err is ClickHouse's "already on disk/volume"
// response, which means there is nothing to move.
func isAlreadyOnTarget(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already on disk") || strings.Contains(msg, "already on volume")
}

// isPartGone reports whether err indicates the target part no longer exists,
// which happens when a background merge replaces it between listing and moving.
// The check is best-effort against ClickHouse's message text.
func isPartGone(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no part") ||
		strings.Contains(msg, "no such part") ||
		strings.Contains(msg, "no such data part") ||
		strings.Contains(msg, "cannot find part")
}
