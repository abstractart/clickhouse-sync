package main

import (
	"reflect"
	"testing"
)

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
