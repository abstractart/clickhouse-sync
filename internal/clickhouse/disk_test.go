package clickhouse

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// diskFakeServer answers the two pre-flight queries: system.disks capacity and
// the sum(bytes_on_disk) of the parts to move.
type diskFakeServer struct {
	disksBody string
	partsBody string
}

func (f *diskFakeServer) serve(t *testing.T) *Client {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		q := string(body)
		switch {
		case strings.Contains(q, "system.disks"):
			io.WriteString(w, f.disksBody)
		case strings.Contains(q, "system.parts"):
			io.WriteString(w, f.partsBody)
		default:
			io.WriteString(w, "\n")
		}
	}))
	t.Cleanup(srv.Close)
	return newClientFor(t, srv)
}

func TestDiskInfo(t *testing.T) {
	c := (&diskFakeServer{disksBody: "1000\t400\n"}).serve(t)
	di, err := c.DiskInfo(context.Background(), "cold")
	if err != nil {
		t.Fatalf("DiskInfo: %v", err)
	}
	if di.Total != 1000 || di.Free != 400 || di.Used() != 600 {
		t.Fatalf("got total=%d free=%d used=%d", di.Total, di.Free, di.Used())
	}
}

func TestDiskInfoMissingDisk(t *testing.T) {
	c := (&diskFakeServer{disksBody: "\n"}).serve(t)
	if _, err := c.DiskInfo(context.Background(), "nope"); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

func TestPartitionBytesToMove(t *testing.T) {
	c := (&diskFakeServer{partsBody: "12345\n"}).serve(t)
	n, err := c.PartitionBytesToMove(context.Background(), "db", "t", "202401", true, "cold")
	if err != nil {
		t.Fatalf("PartitionBytesToMove: %v", err)
	}
	if n != 12345 {
		t.Fatalf("got %d, want 12345", n)
	}
}

func TestPartitionBytesToMoveEmpty(t *testing.T) {
	// sum() over no rows can come back empty; treat it as zero.
	c := (&diskFakeServer{partsBody: "\n"}).serve(t)
	n, err := c.PartitionBytesToMove(context.Background(), "db", "t", "202401", false, "cold")
	if err != nil || n != 0 {
		t.Fatalf("got n=%d err=%v, want 0,nil", n, err)
	}
}
