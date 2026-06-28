package engine

import (
	"database/sql"
	"path/filepath"
	"testing"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	dir, err := filepath.Abs("../../testdata")
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func TestDiscover(t *testing.T) {
	e := testEngine(t)
	got := e.Tables()
	want := map[string]bool{"users": true, "orders": true, "events": true}
	for _, tbl := range got {
		delete(want, tbl)
	}
	if len(want) != 0 {
		t.Errorf("missing discovered tables %v; got %v", want, got)
	}
}

func TestQueryParquet(t *testing.T) {
	e := testEngine(t)
	rows, err := e.Query("SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	var n int
	var firstName string
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			firstName = name
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("users row count = %d, want 5", n)
	}
	if firstName != "alice" {
		t.Errorf("first user name = %q, want alice", firstName)
	}
}

func TestQueryCSV(t *testing.T) {
	e := testEngine(t)
	var count int
	if err := e.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 3 {
		t.Errorf("events count = %d, want 3", count)
	}
}

func TestDescribe(t *testing.T) {
	e := testEngine(t)
	cols, err := e.Describe("users")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if len(cols) != 5 {
		t.Fatalf("users columns = %d, want 5", len(cols))
	}
	if cols[0].Name != "id" {
		t.Errorf("first column = %q, want id", cols[0].Name)
	}
}

func TestNewFromSourcesFileTypes(t *testing.T) {
	dir, _ := filepath.Abs("../../testdata")
	e, err := NewFromSources([]Source{
		{Name: "u", Type: "parquet", Path: filepath.Join(dir, "users.parquet")},
		{Name: "e", Type: "csv", Path: filepath.Join(dir, "events.csv")},
	})
	if err != nil {
		t.Fatalf("NewFromSources: %v", err)
	}
	t.Cleanup(func() { e.Close() })

	got := map[string]bool{}
	for _, tbl := range e.Tables() {
		got[tbl] = true
	}
	if !got["users"] || !got["events"] {
		t.Fatalf("tables = %v, want users+events", e.Tables())
	}
	var n int
	if err := e.QueryRow("SELECT count(*) FROM users").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Errorf("users count = %d, want 5", n)
	}
}

func TestUnknownSourceType(t *testing.T) {
	if _, err := NewFromSources([]Source{{Name: "x", Type: "mongodb"}}); err == nil {
		t.Fatal("expected error for unknown source type")
	}
}

// makeSourceDB writes a DuckDB file with two tables, standing in for a live
// external database so the attach-snapshot path can be tested without a network.
func makeSourceDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.db")
	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"CREATE TABLE widgets(id INTEGER, name VARCHAR)",
		"INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')",
		"CREATE TABLE gadgets(sku VARCHAR)",
		"INSERT INTO gadgets VALUES ('x'),('y')",
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAttachSnapshot(t *testing.T) {
	src := makeSourceDB(t)
	e, err := NewFromSources(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })

	// extension "" => skip INSTALL/LOAD; attach the DuckDB file directly. The
	// postgres/mysql paths differ only in the extension + ATTACH type.
	if err := e.attachSnapshot(Source{Name: "src", DSN: src}, "", "duckdb"); err != nil {
		t.Fatalf("attachSnapshot: %v", err)
	}

	for tbl, want := range map[string]int{"widgets": 3, "gadgets": 2} {
		var n int
		if err := e.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if n != want {
			t.Errorf("%s count = %d, want %d", tbl, n, want)
		}
	}

	// The source must be detached after snapshotting (offline copy).
	var attached int
	if err := e.QueryRow(
		"SELECT count(*) FROM duckdb_databases() WHERE database_name = '_src_src'").Scan(&attached); err != nil {
		t.Fatal(err)
	}
	if attached != 0 {
		t.Errorf("source still attached after snapshot (%d)", attached)
	}
}

func TestAttachSnapshotTableFilter(t *testing.T) {
	src := makeSourceDB(t)
	e, err := NewFromSources(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })

	if err := e.attachSnapshot(Source{Name: "f", DSN: src, Tables: []string{"widgets"}}, "", "duckdb"); err != nil {
		t.Fatalf("attachSnapshot: %v", err)
	}
	if got := e.Tables(); len(got) != 1 || got[0] != "widgets" {
		t.Errorf("tables = %v, want [widgets]", got)
	}
}

func TestStubMissingSchema(t *testing.T) {
	e := testEngine(t)

	// Without stub mode, a query on an unknown table is an error.
	if _, err := e.Run("SELECT * FROM no_such_table"); err == nil {
		t.Fatal("expected error for missing table without stub mode")
	}

	e.SetStubMissing(true)

	// Missing table -> empty result, no error.
	res, err := e.Run("SELECT quantity FROM no_such_table WHERE id = 1")
	if err != nil {
		t.Fatalf("stub Run (missing table): %v", err)
	}
	if len(res.Cols) != 0 || len(res.Rows) != 0 {
		t.Errorf("missing table: want empty result, got %d cols / %d rows", len(res.Cols), len(res.Rows))
	}

	// A missing COLUMN on a real table is an app error (likely a typo), NOT a
	// missing relation -- it must still surface even in stub mode, so real bugs
	// aren't masked.
	if _, err = e.Run("SELECT nope FROM users"); err == nil {
		t.Error("missing column must surface as an error even in stub mode")
	}

	// A syntax error is the app's fault too -- never stubbed.
	if _, err = e.Run("SELECT * FROM users WHERE id IN (SELECT)"); err == nil {
		t.Error("syntax error must surface even in stub mode")
	}

	// A valid query against a known table is unaffected by stub mode.
	if res, err = e.Run("SELECT id FROM users"); err != nil {
		t.Fatalf("stub Run (valid query): %v", err)
	}
	if len(res.Rows) == 0 {
		t.Error("valid query returned no rows under stub mode")
	}

	e.SetStubMissing(false)
	if _, err = e.Run("SELECT * FROM no_such_table"); err == nil {
		t.Error("with stub mode off, a missing-table error must surface")
	}
}

func TestPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "persist.duckdb")

	// First "run": create a table and a row, then close (checkpoints to file).
	e1, err := NewFromSourcesAt(path, nil)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if _, err := e1.Run("CREATE TABLE t(id INT, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := e1.Run("INSERT INTO t VALUES (1,'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Second "run" against the same file: the data is still there.
	e2, err := NewFromSourcesAt(path, nil)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	t.Cleanup(func() { e2.Close() })
	res, err := e2.Run("SELECT name FROM t WHERE id = 1")
	if err != nil {
		t.Fatalf("select after restart: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "alice" {
		t.Errorf("after restart got %v, want one row [alice]", res.Rows)
	}

	// In-memory engines do not persist (sanity check the empty-path path).
	mem, err := NewFromSourcesAt("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	if _, err := mem.Run("SELECT 1 FROM t"); err == nil {
		t.Error("in-memory engine unexpectedly saw the persisted table")
	}
}

func TestClassifyErr(t *testing.T) {
	e := testEngine(t)
	cases := []struct {
		query string
		want  errClass
	}{
		{"SELECT * FROM no_such_table", errMissingRelation},
		{"SELECT nope FROM users", errApp},           // binder: column not found
		{"SELECT * FROM users WHERE", errApp},        // parser: syntax error
		{"WITH x AS (SELECT 1) SELECT * FROM x", -1}, // valid CTE -> no error
	}
	for _, c := range cases {
		_, err := e.db.Query(c.query)
		if c.want == -1 {
			if err != nil {
				t.Errorf("%q: unexpected error: %v", c.query, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%q: expected an error", c.query)
			continue
		}
		if got := classifyErr(err); got != c.want {
			t.Errorf("%q: classifyErr = %d, want %d (err: %v)", c.query, got, c.want, err)
		}
	}
}
