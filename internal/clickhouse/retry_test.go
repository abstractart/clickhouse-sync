package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIsTransient(t *testing.T) {
	if !IsTransient(&TransportError{Err: errors.New("dial tcp: connection refused")}) {
		t.Error("TransportError should be transient")
	}
	if IsTransient(errors.New("Code: 60. DB::Exception: table does not exist")) {
		t.Error("a plain ClickHouse error must not be transient")
	}
	if IsTransient(ErrAlreadyOnTarget) {
		t.Error("ErrAlreadyOnTarget must not be transient")
	}
}

func TestRetryRetriesTransientThenSucceeds(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), RetryPolicy{Retries: 3, Delay: 0}, func() error {
		calls++
		if calls < 3 {
			return &TransportError{Err: errors.New("timeout")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestRetryStopsOnNonTransient(t *testing.T) {
	calls := 0
	sentinel := errors.New("query error")
	err := Retry(context.Background(), RetryPolicy{Retries: 5, Delay: 0}, func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("non-transient error must not be retried, got %d calls", calls)
	}
}

func TestRetryExhaustsAttempts(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), RetryPolicy{Retries: 2, Delay: 0}, func() error {
		calls++
		return &TransportError{Err: errors.New("unreachable")}
	})
	if !IsTransient(err) {
		t.Fatalf("expected transient error to surface, got %v", err)
	}
	if calls != 3 { // 1 initial + 2 retries
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestRetryHonoursContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := Retry(ctx, RetryPolicy{Retries: 5, Delay: time.Hour}, func() error {
		calls++
		return &TransportError{Err: errors.New("unreachable")}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 attempt before cancellation, got %d", calls)
	}
}
