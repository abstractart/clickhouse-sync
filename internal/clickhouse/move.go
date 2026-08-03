package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrAlreadyOnTarget is returned by MovePartition when every part of the
// partition already resides on the destination disk. It makes the move
// idempotent: re-running it is a no-op instead of an error.
var ErrAlreadyOnTarget = errors.New("partition already on destination disk")

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

// isAlreadyOnTarget reports whether err is ClickHouse's "already on disk/volume"
// response, which means there is nothing to move.
func isAlreadyOnTarget(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already on disk") || strings.Contains(msg, "already on volume")
}
