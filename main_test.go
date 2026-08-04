package main

import (
	"bufio"
	"reflect"
	"strings"
	"testing"

	"clickhouse-sync/internal/clickhouse"
)

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
