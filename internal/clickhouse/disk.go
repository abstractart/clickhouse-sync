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

// Part identifies one active data part and its on-disk footprint.
type Part struct {
	Name  string
	Bytes uint64 // bytes_on_disk
	Disk  string // disk_name it currently resides on
}

// PartitionParts returns the active parts of the partition that are NOT already
// on destDisk — i.e. the parts a per-part move would relocate on this node. Parts
// already on destDisk are excluded, so the list matches the idempotent moves
// (which skip them). Results are ordered by part name for deterministic output.
func (c *Client) PartitionParts(ctx context.Context, database, table, partition string, partitionIsID bool, destDisk string) ([]Part, error) {
	col := "partition"
	if partitionIsID {
		col = "partition_id"
	}
	q := "SELECT name, bytes_on_disk, disk_name FROM system.parts WHERE active" +
		" AND database = " + quoteLiteral(database) +
		" AND table = " + quoteLiteral(table) +
		" AND " + col + " = " + quoteLiteral(partition) +
		" AND disk_name != " + quoteLiteral(destDisk) +
		" ORDER BY name"
	out, err := c.Exec(ctx, q+"\nFORMAT TabSeparated")
	if err != nil {
		return nil, err
	}

	var parts []Part
	for line := range strings.SplitSeq(strings.TrimRight(out, "\n"), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 3 {
			return nil, fmt.Errorf("unexpected system.parts row: %q", line)
		}
		b, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing bytes_on_disk for part %q: %w", fields[0], err)
		}
		parts = append(parts, Part{Name: fields[0], Bytes: b, Disk: fields[2]})
	}
	return parts, nil
}
