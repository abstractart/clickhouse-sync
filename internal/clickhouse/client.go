// Package clickhouse implements a minimal ClickHouse client that talks to the
// server over its HTTP(S) interface using only the Go standard library.
package clickhouse

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// Client executes queries against a single ClickHouse node over HTTP(S).
type Client struct {
	baseURL  string
	user     string
	password string
	http     *http.Client
}

// Options configures a Client.
type Options struct {
	// Host is the node hostname or IP (without scheme or port).
	Host string
	// Port is the ClickHouse HTTP(S) port (typically 8443 for HTTPS).
	Port int
	// User and Password are the ClickHouse credentials.
	User     string
	Password string
	// InsecureSkipVerify disables TLS certificate verification.
	InsecureSkipVerify bool
	// Timeout bounds the whole request, including the (potentially slow) MOVE
	// operation. Zero means a sensible default.
	Timeout time.Duration
	// ConnectTimeout bounds only establishing the TCP+TLS connection, so an
	// unreachable node fails fast instead of hanging until Timeout. Zero means a
	// sensible default.
	ConnectTimeout time.Duration
}

// New builds a Client for the given node. TLS (HTTPS) is always used.
func New(o Options) *Client {
	timeout := o.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	connectTimeout := o.ConnectTimeout
	if connectTimeout == 0 {
		connectTimeout = 10 * time.Second
	}
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: connectTimeout}).DialContext,
		TLSHandshakeTimeout: connectTimeout,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: o.InsecureSkipVerify},
	}
	return &Client{
		baseURL:  fmt.Sprintf("https://%s:%d/", o.Host, o.Port),
		user:     o.User,
		password: o.Password,
		http:     &http.Client{Timeout: timeout, Transport: transport},
	}
}

// Exec sends a query and returns the raw response body. It returns an error if
// ClickHouse answers with a non-2xx status.
func (c *Client) Exec(ctx context.Context, query string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewBufferString(query))
	if err != nil {
		return "", err
	}
	// Authenticate via dedicated headers so credentials never end up in the URL.
	req.Header.Set("X-ClickHouse-User", c.user)
	req.Header.Set("X-ClickHouse-Key", c.password)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.http.Do(req)
	if err != nil {
		// Do returns an error only when no valid HTTP response was received
		// (dial/TLS failure, timeout, connection reset) — i.e. a transport-level
		// problem that is typically transient and safe to retry.
		return "", &TransportError{Err: err}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("clickhouse returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// QueryColumn runs a query that is expected to yield a single column and returns
// the non-empty values. The query is wrapped with FORMAT TabSeparated so the
// output is trivial to parse.
func (c *Client) QueryColumn(ctx context.Context, query string) ([]string, error) {
	out, err := c.Exec(ctx, query+"\nFORMAT TabSeparated")
	if err != nil {
		return nil, err
	}
	var values []string
	for line := range strings.SplitSeq(out, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			values = append(values, line)
		}
	}
	return values, nil
}

// Ping verifies the node is reachable and credentials are accepted.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Exec(ctx, "SELECT 1")
	return err
}

// TransportError wraps a transport-level failure: the request never received a
// valid HTTP response (dial/TLS failure, timeout, connection reset). A non-2xx
// answer from ClickHouse is NOT a TransportError — that is a definitive reply.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

// IsTransient reports whether err is a transport-level failure worth retrying.
func IsTransient(err error) bool {
	var te *TransportError
	return errors.As(err, &te)
}

// quoteLiteral escapes a value for safe use as a ClickHouse string literal.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// quoteIdentifier escapes a value for safe use as a ClickHouse identifier.
func quoteIdentifier(s string) string {
	s = strings.ReplaceAll(s, "`", "``")
	return "`" + s + "`"
}
