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

func TestPartitionParts(t *testing.T) {
	c := (&diskFakeServer{partsBody: "all_1_1_0\t1024\tdefault\nall_2_2_0\t2048\tdefault\n"}).serve(t)
	parts, err := c.PartitionParts(context.Background(), "db", "t", "202401", true, "cold")
	if err != nil {
		t.Fatalf("PartitionParts: %v", err)
	}
	want := []Part{
		{Name: "all_1_1_0", Bytes: 1024, Disk: "default"},
		{Name: "all_2_2_0", Bytes: 2048, Disk: "default"},
	}
	if len(parts) != 2 || parts[0] != want[0] || parts[1] != want[1] {
		t.Fatalf("got %+v, want %+v", parts, want)
	}
}

func TestPartitionPartsEmpty(t *testing.T) {
	// No parts to move (all already on target or empty partition).
	c := (&diskFakeServer{partsBody: "\n"}).serve(t)
	parts, err := c.PartitionParts(context.Background(), "db", "t", "202401", false, "cold")
	if err != nil || len(parts) != 0 {
		t.Fatalf("got parts=%v err=%v, want empty,nil", parts, err)
	}
}
