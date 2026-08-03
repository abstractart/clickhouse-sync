package clickhouse

import (
	"context"
	"time"
)

// RetryPolicy controls how transient (transport-level) failures are retried.
type RetryPolicy struct {
	// Retries is the number of extra attempts after the first one. Zero disables
	// retrying.
	Retries int
	// Delay is the backoff before the first retry; it doubles after each attempt
	// (exponential backoff).
	Delay time.Duration
}

// Retry runs fn, retrying while it returns a transient error (see IsTransient)
// and attempts remain. Non-transient errors (e.g. a ClickHouse query error or
// ErrAlreadyOnTarget) are returned immediately without retrying. Backoff honours
// ctx cancellation.
func Retry(ctx context.Context, p RetryPolicy, fn func() error) error {
	delay := p.Delay
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil || !IsTransient(err) || attempt >= p.Retries {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}
