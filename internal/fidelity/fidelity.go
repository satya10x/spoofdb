// Package fidelity records where spoofdb returns approximate data while
// spoofing — NULLs rendered as zero values, integers that overflow the target
// type, decimals shown as floats, and so on — so the loss is visible to the
// operator and the client instead of silently corrupting results.
package fidelity

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
)

// ExceedsInt64 reports whether v's integer value does not fit in int64. It
// handles the wide-integer values DuckDB surfaces as uint64, *big.Int, or a
// numeric string (HUGEINT / large UBIGINT); other values report false.
func ExceedsInt64(v any) bool {
	switch x := v.(type) {
	case uint64:
		return x > math.MaxInt64
	case *big.Int:
		return !x.IsInt64()
	default:
		bi, ok := new(big.Int).SetString(strings.TrimSpace(fmt.Sprint(x)), 10)
		if !ok {
			return false
		}
		return !bi.IsInt64()
	}
}

// Report accumulates approximation notes for a single query, deduped by
// (column, reason) with an occurrence count. The zero value is not usable; use
// New. A nil *Report is safe to call methods on (they no-op), so callers can
// pass nil to disable reporting.
type Report struct {
	notes map[string]*note
}

type note struct {
	column string
	reason string
	count  int
}

// New returns an empty Report.
func New() *Report { return &Report{notes: map[string]*note{}} }

// Add records one occurrence of reason for column.
func (r *Report) Add(column, reason string) {
	if r == nil {
		return
	}
	key := column + "\x00" + reason
	n := r.notes[key]
	if n == nil {
		n = &note{column: column, reason: reason}
		r.notes[key] = n
	}
	n.count++
}

// Empty reports whether nothing was recorded.
func (r *Report) Empty() bool { return r == nil || len(r.notes) == 0 }

func (r *Report) sorted() []*note {
	out := make([]*note, 0, len(r.notes))
	for _, n := range r.notes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].column != out[j].column {
			return out[i].column < out[j].column
		}
		return out[i].reason < out[j].reason
	})
	return out
}

// Warnings returns one human-readable line per distinct (column, reason),
// suitable for a client warning channel. A count >1 is appended.
func (r *Report) Warnings() []string {
	if r.Empty() {
		return nil
	}
	notes := r.sorted()
	out := make([]string, len(notes))
	for i, n := range notes {
		if n.count > 1 {
			out[i] = fmt.Sprintf("column %q: %s (x%d)", n.column, n.reason, n.count)
		} else {
			out[i] = fmt.Sprintf("column %q: %s", n.column, n.reason)
		}
	}
	return out
}

// Summary returns a single-line summary for the server log, or "" if empty.
func (r *Report) Summary() string {
	if r.Empty() {
		return ""
	}
	return "served with approximations: " + strings.Join(r.Warnings(), "; ")
}
