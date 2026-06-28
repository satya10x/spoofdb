package mysql

import "testing"

func TestSubstituteArgs(t *testing.T) {
	cases := []struct {
		name  string
		query string
		args  []any
		want  string
	}{
		{"no args", "SELECT 1", nil, "SELECT 1"},
		{"one int", "SELECT * FROM t WHERE id = ?", []any{int64(5)}, "SELECT * FROM t WHERE id = 5"},
		{"string quoted", "SELECT ? ", []any{"a'b"}, "SELECT 'a''b' "},
		{
			"question mark inside literal is not a placeholder",
			"SELECT note FROM t WHERE note = 'a?b' AND id = ?",
			[]any{int64(7)},
			"SELECT note FROM t WHERE note = 'a?b' AND id = 7",
		},
		{
			"placeholder then literal question mark",
			"SELECT ?, '? literal'",
			[]any{int64(1)},
			"SELECT 1, '? literal'",
		},
		{"null arg", "SELECT ?", []any{nil}, "SELECT NULL"},
		{
			"backslash preserved (DuckDB standard strings)",
			"SELECT ?",
			[]any{`C:\tmp\x`},
			`SELECT 'C:\tmp\x'`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := substituteArgs(c.query, c.args); got != c.want {
				t.Errorf("substituteArgs(%q, %v) = %q, want %q", c.query, c.args, got, c.want)
			}
		})
	}
}

func TestIsSessionNoop(t *testing.T) {
	for _, w := range []string{"SET", "USE", "BEGIN", "START", "COMMIT", "ROLLBACK"} {
		if !isSessionNoop(w) {
			t.Errorf("isSessionNoop(%q) = false, want true", w)
		}
	}
	for _, w := range []string{"SELECT", "INSERT", "SHOW", ""} {
		if isSessionNoop(w) {
			t.Errorf("isSessionNoop(%q) = true, want false", w)
		}
	}
}
