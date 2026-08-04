package clickhouse

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// DiskInfo describes the capacity of a ClickHouse disk on a single node.
type DiskInfo struct {
	Name  string
	Total uint64 // total_space, bytes
	Free  uint64 // free_space, bytes
}

// Used returns the bytes currently in use on the disk (Total - Free).
func (d DiskInfo) Used() uint64 {
	if d.Free > d.Total {
		return 0
	}
	return d.Total - d.Free
}

// DiskInfo returns the capacity of the named disk on this node. It errors if the
// disk is not configured on the node, so a missing destination disk surfaces
// during pre-flight rather than as a late MOVE failure.
func (c *Client) DiskInfo(ctx context.Context, disk string) (DiskInfo, error) {
	q := "SELECT total_space, free_space FROM system.disks WHERE name = " + quoteLiteral(disk)
	out, err := c.Exec(ctx, q+"\nFORMAT TabSeparated")
	if err != nil {
		return DiskInfo{}, err
	}
	line := strings.TrimSpace(out)
	if line == "" {
		return DiskInfo{}, fmt.Errorf("disk %q not found on node", disk)
	}
	fields := strings.Split(line, "\t")
	if len(fields) < 2 {
		return DiskInfo{}, fmt.Errorf("unexpected system.disks output: %q", line)
	}
	total, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return DiskInfo{}, fmt.Errorf("parsing total_space: %w", err)
	}
	free, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return DiskInfo{}, fmt.Errorf("parsing free_space: %w", err)
	}
	return DiskInfo{Name: disk, Total: total, Free: free}, nil
}

// PartitionBytesToMove returns the on-disk size of the active parts of the
// partition that are NOT already on destDisk — i.e. the bytes the move will add
// to destDisk on this node. Parts already on destDisk are excluded, so the figure
// matches the idempotent MOVE (which skips them).
func (c *Client) PartitionBytesToMove(ctx context.Context, database, table, partition string, partitionIsID bool, destDisk string) (uint64, error) {
	col := "partition"
	if partitionIsID {
		col = "partition_id"
	}
	q := "SELECT sum(bytes_on_disk) FROM system.parts WHERE active" +
		" AND database = " + quoteLiteral(database) +
		" AND table = " + quoteLiteral(table) +
		" AND " + col + " = " + quoteLiteral(partition) +
		" AND disk_name != " + quoteLiteral(destDisk)
	out, err := c.Exec(ctx, q+"\nFORMAT TabSeparated")
	if err != nil {
		return 0, err
	}
	line := strings.TrimSpace(out)
	// sum() over no matching rows yields 0; guard against an empty/NULL body too.
	if line == "" || line == `\N` {
		return 0, nil
	}
	n, err := strconv.ParseUint(line, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing partition size: %w", err)
	}
	return n, nil
}
