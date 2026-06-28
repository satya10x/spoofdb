package fidelity

import (
	"strings"
	"testing"
)

func TestReportDedupAndCount(t *testing.T) {
	r := New()
	if !r.Empty() {
		t.Fatal("new report should be empty")
	}
	r.Add("amount", "DECIMAL shown as float8")
	r.Add("amount", "DECIMAL shown as float8") // duplicate -> count 2
	r.Add("big", "value exceeds int64")

	if r.Empty() {
		t.Fatal("report should not be empty after Add")
	}
	ws := r.Warnings()
	if len(ws) != 2 {
		t.Fatalf("warnings = %d, want 2 (deduped): %v", len(ws), ws)
	}
	// Sorted by column: "amount" before "big".
	if !strings.Contains(ws[0], `"amount"`) || !strings.Contains(ws[0], "(x2)") {
		t.Errorf("first warning = %q, want amount with (x2)", ws[0])
	}
	if !strings.Contains(ws[1], `"big"`) || strings.Contains(ws[1], "(x") {
		t.Errorf("second warning = %q, want big without a count", ws[1])
	}
	if !strings.HasPrefix(r.Summary(), "served with approximations:") {
		t.Errorf("summary = %q", r.Summary())
	}
}

func TestNilReportIsSafe(t *testing.T) {
	var r *Report
	r.Add("x", "y") // must not panic
	if !r.Empty() {
		t.Error("nil report should be empty")
	}
	if r.Summary() != "" || r.Warnings() != nil {
		t.Error("nil report should yield empty summary/warnings")
	}
}
