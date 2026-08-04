package clickhouse

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeServer emulates the ClickHouse HTTP endpoints MovePartition uses: the
// ALTER (executed in the MOVE goroutine) and system.processes (hit by the
// status-watching goroutine).
type fakeServer struct {
	alterDelay  time.Duration // how long the ALTER "runs" before responding
	alterHang   bool          // if set, the ALTER blocks until the client cancels
	alterStatus int           // HTTP status for the ALTER; 0 means 200
	alterBody   string        // response body for the ALTER
	polls       atomic.Int32  // number of system.processes checks observed
	moveProbes  atomic.Int32  // number of system.moves detail checks observed
}

func (f *fakeServer) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	q := string(body)
	switch {
	case strings.Contains(q, "system.moves"):
		f.moveProbes.Add(1)
		io.WriteString(w, "2\t1.5\tcold\n") // 2 parts moving to 'cold', 1.5s elapsed
	case strings.Contains(q, "system.processes"):
		f.polls.Add(1)
		io.WriteString(w, "1\n") // report the MOVE as running
	case strings.HasPrefix(q, "ALTER TABLE"):
		if f.alterHang {
			<-r.Context().Done()
			return
		}
		select {
		case <-time.After(f.alterDelay):
		case <-r.Context().Done():
			return
		}
		if f.alterStatus != 0 {
			w.WriteHeader(f.alterStatus)
			io.WriteString(w, f.alterBody)
			return
		}
	default:
		io.WriteString(w, "\n")
	}
}

func newClientFor(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(u.Port())
	return New(Options{Host: u.Hostname(), Port: port, InsecureSkipVerify: true})
}

func TestMovePartitionSuccessWhileWatched(t *testing.T) {
	fs := &fakeServer{alterDelay: 120 * time.Millisecond}
	srv := httptest.NewTLSServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := newClientFor(t, srv).MovePartition(ctx, "db", "t", "202401", "cold", true,
		MoveOptions{PollInterval: 20 * time.Millisecond, StatusTimeout: time.Second})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if fs.polls.Load() == 0 {
		t.Fatal("status-watching goroutine never polled while the MOVE ran")
	}
	if fs.moveProbes.Load() == 0 {
		t.Fatal("status-watching goroutine never enriched progress from system.moves")
	}
}

func TestMovePartitionServerError(t *testing.T) {
	fs := &fakeServer{alterDelay: 60 * time.Millisecond, alterStatus: http.StatusInternalServerError, alterBody: "Code: 243. Not enough space"}
	srv := httptest.NewTLSServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := newClientFor(t, srv).MovePartition(ctx, "db", "t", "202401", "cold", true,
		MoveOptions{PollInterval: 20 * time.Millisecond, StatusTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "Not enough space") {
		t.Fatalf("expected server failure to surface, got %v", err)
	}
}

func TestMovePartitionAlreadyOnTarget(t *testing.T) {
	fs := &fakeServer{alterStatus: http.StatusInternalServerError, alterBody: "Code: 479. DB::Exception: All parts of partition '202401' are already on disk 'cold'."}
	srv := httptest.NewTLSServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	err := newClientFor(t, srv).MovePartition(context.Background(), "db", "t", "202401", "cold", true,
		MoveOptions{PollInterval: time.Second, StatusTimeout: time.Second})
	if !errors.Is(err, ErrAlreadyOnTarget) {
		t.Fatalf("expected ErrAlreadyOnTarget, got %v", err)
	}
}

func TestMovePartitionDeadline(t *testing.T) {
	fs := &fakeServer{alterHang: true} // never returns; the deadline must fire
	srv := httptest.NewTLSServer(http.HandlerFunc(fs.handler))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := newClientFor(t, srv).MovePartition(ctx, "db", "t", "202401", "cold", true,
		MoveOptions{PollInterval: 20 * time.Millisecond, StatusTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "may still be running") {
		t.Fatalf("expected deadline error mentioning the MOVE may still be running, got %v", err)
	}
}
