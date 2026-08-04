package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"clickhouse-sync/internal/clickhouse"
)

func TestParseFlags(t *testing.T) {
	origArgs, origCmd := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args, flag.CommandLine = origArgs, origCmd })
	t.Setenv("CLICKHOUSE_PASSWORD", "") // deterministic base

	parse := func(args ...string) (config, error) {
		flag.CommandLine = flag.NewFlagSet("clickhouse-sync", flag.ContinueOnError)
		flag.CommandLine.SetOutput(io.Discard)
		os.Args = append([]string{"clickhouse-sync"}, args...)
		return parseFlags()
	}

	t.Run("valid", func(t *testing.T) {
		c, err := parse("-user", "u", "-hostname", "h", "-database", "db",
			"-table", "t", "-partition", "202401", "-destination-disk", "cold")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.user != "u" || c.hostname != "h" || c.database != "db" || c.table != "t" ||
			c.partition != "202401" || c.disk != "cold" {
			t.Fatalf("bad config: %+v", c)
		}
		if c.port != 8443 || c.timeout != 5*time.Minute {
			t.Fatalf("defaults not applied: %+v", c)
		}
	})

	t.Run("missing required", func(t *testing.T) {
		if _, err := parse("-hostname", "h"); err == nil || !strings.Contains(err.Error(), "missing required") {
			t.Fatalf("want missing-required error, got %v", err)
		}
	})

	t.Run("neither hostname nor nodes", func(t *testing.T) {
		_, err := parse("-user", "u", "-database", "db", "-table", "t",
			"-partition", "p", "-destination-disk", "cold")
		if err == nil || !strings.Contains(err.Error(), "-hostname") {
			t.Fatalf("want hostname/nodes error, got %v", err)
		}
	})

	t.Run("nodes instead of hostname", func(t *testing.T) {
		c, err := parse("-user", "u", "-nodes", "a,b", "-database", "db", "-table", "t",
			"-partition", "p", "-destination-disk", "cold")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.nodesList != "a,b" {
			t.Fatalf("nodesList = %q", c.nodesList)
		}
	})

	t.Run("password from env", func(t *testing.T) {
		t.Setenv("CLICKHOUSE_PASSWORD", "secret")
		c, err := parse("-user", "u", "-hostname", "h", "-database", "db",
			"-table", "t", "-partition", "p", "-destination-disk", "cold")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.password != "secret" {
			t.Fatalf("password = %q, want it from the env", c.password)
		}
	})

	t.Run("source equals destination", func(t *testing.T) {
		_, err := parse("-user", "u", "-hostname", "h", "-database", "db", "-table", "t",
			"-partition", "p", "-destination-disk", "cold", "-source-disk", "cold")
		if err == nil || !strings.Contains(err.Error(), "must differ") {
			t.Fatalf("want source!=destination error, got %v", err)
		}
	})

	t.Run("source disk set", func(t *testing.T) {
		c, err := parse("-user", "u", "-hostname", "h", "-database", "db", "-table", "t",
			"-partition", "p", "-destination-disk", "cold", "-source-disk", "warm")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if c.sourceDisk != "warm" {
			t.Fatalf("sourceDisk = %q", c.sourceDisk)
		}
	})
}

func TestConfirmPartMove(t *testing.T) {
	cases := []struct {
		in                       string
		proceed, all, wantErrNil bool
	}{
		{"y\n", true, false, true},
		{"yes\n", true, false, true},
		{"Y\n", true, false, true},
		{"a\n", true, true, true},
		{"all\n", true, true, true},
		{"n\n", false, false, true},
		{"\n", false, false, true}, // empty -> default No
		{"", false, false, false},  // EOF, no data -> error
	}
	for _, c := range cases {
		proceed, all, err := confirmPartMove("node", "all_1_1_0", bufio.NewReader(strings.NewReader(c.in)))
		if proceed != c.proceed || all != c.all || (err == nil) != c.wantErrNil {
			t.Errorf("confirmPartMove(%q) = (%v, %v, err=%v), want (%v, %v, errNil=%v)",
				c.in, proceed, all, err, c.proceed, c.all, c.wantErrNil)
		}
	}
}

func TestMoveHeadline(t *testing.T) {
	base := config{partition: "202401", database: "demo", table: "events", disk: "cold"}
	if got := moveHeadline(base); !strings.Contains(got, `to disk "cold"`) || strings.Contains(got, "from disk") {
		t.Errorf("without source: %q", got)
	}
	withSrc := base
	withSrc.sourceDisk = "warm"
	if got := moveHeadline(withSrc); !strings.Contains(got, `from disk "warm" to disk "cold"`) {
		t.Errorf("with source: %q", got)
	}
}

func TestNothingToMoveReason(t *testing.T) {
	base := config{disk: "cold"}
	if got := nothingToMoveReason(base); got != `no parts outside disk "cold"` {
		t.Errorf("without source: %q", got)
	}
	withSrc := config{disk: "cold", sourceDisk: "warm"}
	if got := nothingToMoveReason(withSrc); got != `no parts on disk "warm"` {
		t.Errorf("with source: %q", got)
	}
}

func TestTotalBytes(t *testing.T) {
	if got := totalBytes(nil); got != 0 {
		t.Errorf("totalBytes(nil) = %d, want 0", got)
	}
	parts := []clickhouse.Part{{Bytes: 100}, {Bytes: 250}, {Bytes: 1}}
	if got := totalBytes(parts); got != 351 {
		t.Errorf("totalBytes = %d, want 351", got)
	}
}

func TestAppendUnique(t *testing.T) {
	got := appendUnique(appendUnique(appendUnique(nil, "a"), "b"), "a")
	want := []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("appendUnique = %v, want %v", got, want)
	}
}

func TestHumanBytes(t *testing.T) {
	tests := map[uint64]string{
		0:                      "0 B",
		512:                    "512 B",
		1024:                   "1.0 KiB",
		1536:                   "1.5 KiB",
		1024 * 1024:            "1.0 MiB",
		5 * 1024 * 1024 * 1024: "5.0 GiB",
	}
	for in, want := range tests {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestPercent(t *testing.T) {
	cases := []struct {
		part, total uint64
		want        float64
	}{
		{0, 0, 0},
		{1, 0, 0}, // guard against divide-by-zero
		{50, 200, 25},
		{200, 200, 100},
	}
	for _, c := range cases {
		if got := percent(c.part, c.total); got != c.want {
			t.Errorf("percent(%d, %d) = %v, want %v", c.part, c.total, got, c.want)
		}
	}
}

func TestSplitNodes(t *testing.T) {
	tests := map[string][]string{
		"a,b,c":            {"a", "b", "c"},
		" a , b ,c ":       {"a", "b", "c"},
		"a,,b,":            {"a", "b"},
		"":                 nil,
		" , ,":             nil,
		"single":           {"single"},
		"host1.local,h2:8": {"host1.local", "h2:8"},
	}
	for in, want := range tests {
		if got := splitNodes(in); !reflect.DeepEqual(got, want) {
			t.Errorf("splitNodes(%q) = %v, want %v", in, got, want)
		}
	}
}
