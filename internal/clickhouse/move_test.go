package clickhouse

import (
	"errors"
	"testing"
)

func TestIsAlreadyOnTarget(t *testing.T) {
	cases := map[string]bool{
		"clickhouse returned 500: Code: 479. DB::Exception: All parts of partition '202401' are already on disk 'local'.": true,
		"Code: 60. DB::Exception: Table demo.events_local does not exist.":                                                 false,
		"clickhouse returned 500: parts are already on volume 'local'":                                                     true,
	}
	for msg, want := range cases {
		if got := isAlreadyOnTarget(errors.New(msg)); got != want {
			t.Errorf("isAlreadyOnTarget(%q) = %v, want %v", msg, got, want)
		}
	}
}

func TestMovePartitionSQL(t *testing.T) {
	tests := []struct {
		name          string
		db, tbl       string
		partition     string
		disk          string
		partitionIsID bool
		want          string
	}{
		{
			name: "value partition",
			db:   "db", tbl: "events", partition: "2024-01", disk: "cold",
			want: "ALTER TABLE `db`.`events` MOVE PARTITION '2024-01' TO DISK 'cold'",
		},
		{
			name: "partition id",
			db:   "db", tbl: "events", partition: "202401", disk: "cold", partitionIsID: true,
			want: "ALTER TABLE `db`.`events` MOVE PARTITION ID '202401' TO DISK 'cold'",
		},
		{
			name: "escapes quotes and identifiers",
			db:   "d`b", tbl: "t'bl", partition: "o'clock", disk: "co'ld",
			want: "ALTER TABLE `d``b`.`t'bl` MOVE PARTITION 'o\\'clock' TO DISK 'co\\'ld'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MovePartitionSQL(tt.db, tt.tbl, tt.partition, tt.disk, tt.partitionIsID)
			if got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := quoteLiteral(`a\b'c`); got != `'a\\b\'c'` {
		t.Errorf("quoteLiteral = %q", got)
	}
}
