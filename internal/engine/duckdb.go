// Package engine wraps an in-process DuckDB instance that exposes the files in
// a data directory as SQL views. A single *sql.DB is shared across every
// protocol listener; DuckDB handles concurrent reads fine.
package engine

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Engine is a shared DuckDB connection plus the set of loaded tables.
type Engine struct {
	db     *sql.DB
	tables []string
	seen   map[string]string // table name -> source that registered it

	// stub, when true, makes a query that references a relation the engine doesn't
	// have -- or uses a dialect feature DuckDB can't execute -- resolve to an
	// empty result instead of an error, so an app can boot and prepare/run all
	// its queries against a spoofdb with no (or partial) schema. It deliberately
	// does NOT swallow app errors (a typo'd column, a syntax error): those still
	// surface, so real query bugs aren't masked.
	stub   bool
	logged map[string]bool // distinct messages already logged, for one-time logging
}

// SetStubMissing toggles stub mode (see Engine.stub).
func (e *Engine) SetStubMissing(on bool) {
	e.stub = on
	if e.logged == nil {
		e.logged = map[string]bool{}
	}
}

// logOnce logs a formatted message at most once per distinct key, so a query
// repeated across many prepares/requests doesn't spam the log.
func (e *Engine) logOnce(key, format string, args ...any) {
	if e.logged == nil {
		e.logged = map[string]bool{}
	}
	if e.logged[key] {
		return
	}
	e.logged[key] = true
	log.Printf(format, args...)
}

// Source describes one place SQL data is loaded from. Type is "dir", "parquet",
// "csv", "postgres", "mysql", or "clickhouse". Path is for the file types; DSN
// (already resolved) and Tables for the database types.
type Source struct {
	Name   string
	Type   string
	Path   string
	DSN    string
	Tables []string
}

// Column describes a single column of a table.
type Column struct {
	Name string
	Type string
}

// New opens an in-memory DuckDB instance and loads a single directory of
// parquet/csv files. It is shorthand for NewFromSources with one "dir" source.
func New(dataDir string) (*Engine, error) {
	return NewFromSources([]Source{{Name: "data", Type: "dir", Path: dataDir}})
}

// NewFromSources opens an in-memory DuckDB instance and loads every source into
// it. A table name claimed by an earlier source is not overwritten.
func NewFromSources(sources []Source) (*Engine, error) {
	return NewFromSourcesAt("", sources)
}

// NewFromSourcesAt is NewFromSources backed by a DuckDB database file at path, so
// data created at runtime (CREATE TABLE / INSERT over the wire) survives a
// restart. An empty path means an in-memory database (nothing persists). File
// sources are (re)registered as views on every start (cheap, idempotent), so the
// persisted file mainly carries the tables/rows a client created at runtime.
func NewFromSourcesAt(path string, sources []Source) (*Engine, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("engine: open duckdb: %w", err)
	}
	e := &Engine{db: db, seen: map[string]string{}}
	for _, src := range sources {
		if err := e.load(src); err != nil {
			db.Close()
			return nil, err
		}
	}
	sort.Strings(e.tables)
	return e, nil
}

// load dispatches a source to the loader for its type.
func (e *Engine) load(src Source) error {
	switch src.Type {
	case "dir":
		return e.loadDir(src)
	case "parquet":
		return e.register(fileTable(src.Path), fmt.Sprintf("read_parquet('%s')", escapeSQL(src.Path)), src.Path)
	case "csv":
		return e.register(fileTable(src.Path), fmt.Sprintf("read_csv_auto('%s')", escapeSQL(src.Path)), src.Path)
	case "postgres":
		return e.attachSnapshot(src, "postgres", "postgres")
	case "mysql":
		return e.attachSnapshot(src, "mysql", "mysql")
	case "clickhouse":
		return e.clickhousePull(src)
	default:
		return fmt.Errorf("engine: source %q: unknown type %q", src.Name, src.Type)
	}
}

// loadDir registers a view per parquet/csv file in a directory.
func (e *Engine) loadDir(src Source) error {
	entries, err := os.ReadDir(src.Path)
	if err != nil {
		return fmt.Errorf("engine: read data dir %s: %w", src.Path, err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(src.Path, name)
		var reader string
		switch strings.ToLower(filepath.Ext(name)) {
		case ".parquet":
			reader = fmt.Sprintf("read_parquet('%s')", escapeSQL(path))
		case ".csv":
			reader = fmt.Sprintf("read_csv_auto('%s')", escapeSQL(path))
		default:
			continue
		}
		if err := e.register(fileTable(name), reader, path); err != nil {
			return err
		}
	}
	return nil
}

// register creates a view named table over a DuckDB table-function expression,
// skipping (with a log) a name already claimed by another source. Two files
// mapping to the same name (users.parquet + users.csv) would otherwise shadow
// each other and double-list the table.
func (e *Engine) register(table, reader, origin string) error {
	if prev, dup := e.seen[table]; dup {
		log.Printf("engine: table %q already registered from %q; skipping %q", table, prev, origin)
		return nil
	}
	stmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", quoteIdent(table), reader)
	if _, err := e.db.Exec(stmt); err != nil {
		return fmt.Errorf("engine: register view %q: %w", table, err)
	}
	e.seen[table] = origin
	e.tables = append(e.tables, table)
	return nil
}

// qualTable is a schema-qualified table name in an attached database.
type qualTable struct{ schema, name string }

// attachSnapshot loads a live SQL database by attaching it via a DuckDB
// extension and copying each selected table into a local DuckDB table, then
// detaching — so the source can be disconnected afterward (an offline snapshot).
// extension is INSTALL/LOADed first (empty means skip, for built-in types);
// attachType is the ATTACH TYPE. They are equal for real use ("postgres",
// "mysql") and differ only for test stand-ins (e.g. "", "duckdb").
func (e *Engine) attachSnapshot(src Source, extension, attachType string) error {
	if src.DSN == "" {
		return fmt.Errorf("engine: source %q: empty DSN", src.Name)
	}
	if extension != "" {
		if _, err := e.db.Exec(fmt.Sprintf("INSTALL %s; LOAD %s;", extension, extension)); err != nil {
			return fmt.Errorf("engine: load %s extension: %w", extension, err)
		}
	}
	alias := "_src_" + src.Name
	attach := fmt.Sprintf("ATTACH '%s' AS %s (TYPE %s, READ_ONLY)", escapeSQL(src.DSN), quoteIdent(alias), attachType)
	if _, err := e.db.Exec(attach); err != nil {
		return fmt.Errorf("engine: attach source %q: %w", src.Name, err)
	}
	defer e.db.Exec(fmt.Sprintf("DETACH %s", quoteIdent(alias)))

	tables, err := e.attachedTables(alias, src.Tables)
	if err != nil {
		return err
	}
	for _, qt := range tables {
		if prev, dup := e.seen[qt.name]; dup {
			log.Printf("engine: table %q already registered from %q; skipping %q", qt.name, prev, src.Name)
			continue
		}
		stmt := fmt.Sprintf("CREATE OR REPLACE TABLE %s AS SELECT * FROM %s.%s.%s",
			quoteIdent(qt.name), quoteIdent(alias), quoteIdent(qt.schema), quoteIdent(qt.name))
		if _, err := e.db.Exec(stmt); err != nil {
			return fmt.Errorf("engine: snapshot table %q from %q: %w", qt.name, src.Name, err)
		}
		e.seen[qt.name] = src.Name
		e.tables = append(e.tables, qt.name)
	}
	return nil
}

// attachedTables lists the tables of an attached database. When allow is
// non-empty it keeps only tables matching a bare name or a schema.name entry.
func (e *Engine) attachedTables(alias string, allow []string) ([]qualTable, error) {
	rows, err := e.db.Query(
		"SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = ?", alias)
	if err != nil {
		return nil, fmt.Errorf("engine: list tables for %q: %w", alias, err)
	}
	defer rows.Close()

	allowSet := map[string]bool{}
	for _, a := range allow {
		allowSet[strings.ToLower(a)] = true
	}
	var out []qualTable
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, err
		}
		if len(allowSet) > 0 && !allowSet[strings.ToLower(name)] && !allowSet[strings.ToLower(schema+"."+name)] {
			continue
		}
		out = append(out, qualTable{schema: schema, name: name})
	}
	return out, rows.Err()
}

// fileTable derives a table name from a file path: the base name without its
// extension (e.g. testdata/users.parquet -> users).
func fileTable(path string) string {
	base := filepath.Base(path)
	return strings.TrimSuffix(base, filepath.Ext(base))
}

// Query runs sql against DuckDB and returns the result rows. The caller owns
// the returned *sql.Rows and must Close it.
func (e *Engine) Query(query string) (*sql.Rows, error) {
	return e.db.Query(query)
}

// QueryResult is a fully-buffered query result. The SQL wire protocols each
// format Rows according to their own encoding.
type QueryResult struct {
	Cols []Column // column name + DuckDB type name
	Rows [][]any  // raw scanned values; a nil element is SQL NULL
}

// Run executes query and buffers the entire result set. Suitable for the small
// result sets this fake serves.
func (e *Engine) Run(query string) (*QueryResult, error) {
	rows, err := e.db.Query(query)
	if err != nil {
		switch classifyErr(err) {
		case errMissingRelation:
			// A referenced table/view/sequence isn't loaded. Always say which one
			// (so a missing schema is visible even without stub mode); serve empty
			// only when stub mode is on.
			name := missingRelationName(err)
			if e.stub {
				e.logOnce("missing:"+name, "engine: relation %q not found -- serving empty (stub-missing-schema)", name)
				return &QueryResult{}, nil
			}
			e.logOnce("missing:"+name, "engine: relation %q not found -- query will error (load it, or run with -stub-missing-schema)", name)
			return nil, err
		case errDialectGap:
			// A query DuckDB can't execute though a real Postgres/MySQL would
			// (e.g. a data-modifying CTE, ON CONFLICT ON CONSTRAINT, an unknown
			// session SET). This is spoofdb's fidelity limit, not the app's bug,
			// so stub mode serves it empty -- loudly flagged as a fidelity gap.
			if e.stub {
				e.logOnce(err.Error(), "engine: WARNING fidelity gap, DuckDB can't run this; serving empty: %s", firstLine(err.Error()))
				return &QueryResult{}, nil
			}
			return nil, err
		default:
			// App errors -- a typo'd column, a syntax error -- always surface, in
			// both modes, so real query bugs are never silently masked.
			return nil, err
		}
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, _ := rows.ColumnTypes()

	res := &QueryResult{}
	for i, n := range names {
		var t string
		if i < len(types) {
			t = types[i].DatabaseTypeName()
		}
		res.Cols = append(res.Cols, Column{Name: n, Type: t})
	}

	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		// Normalize byte slices (e.g. DuckDB json/blob) to strings so the
		// protocol layers get stable text.
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		res.Rows = append(res.Rows, vals)
	}
	return res, rows.Err()
}

// QueryRow runs sql expected to return a single row.
func (e *Engine) QueryRow(query string, args ...any) *sql.Row {
	return e.db.QueryRow(query, args...)
}

// Tables returns the names of all discovered tables, sorted.
func (e *Engine) Tables() []string {
	out := make([]string, len(e.tables))
	copy(out, e.tables)
	return out
}

// Describe returns the column names and types of a table.
func (e *Engine) Describe(table string) ([]Column, error) {
	rows, err := e.db.Query(fmt.Sprintf("DESCRIBE %s", quoteIdent(table)))
	if err != nil {
		return nil, fmt.Errorf("engine: describe %q: %w", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	// DESCRIBE returns: column_name, column_type, null, key, default, extra.
	scan := make([]any, len(cols))
	holders := make([]sql.NullString, len(cols))
	for i := range scan {
		scan[i] = &holders[i]
	}

	var out []Column
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return nil, err
		}
		out = append(out, Column{Name: holders[0].String, Type: holders[1].String})
	}
	return out, rows.Err()
}

// Close releases the underlying DuckDB connection.
func (e *Engine) Close() error { return e.db.Close() }

// quoteIdent wraps a SQL identifier in double quotes, escaping embedded quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapeSQL escapes single quotes for use inside a SQL string literal.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// errClass categorises a DuckDB query error for stub-mode handling.
type errClass int

const (
	errApp             errClass = iota // app's fault (typo'd column, syntax) -- never stub
	errMissingRelation                 // a referenced table/view/sequence isn't loaded
	errDialectGap                      // valid SQL DuckDB can't execute (writable CTE, etc.)
)

// classifyErr buckets a DuckDB error so stub mode can serve missing relations and
// dialect gaps as empty while still surfacing genuine app errors (a misspelled
// column, a syntax error). The distinction matters: an empty result for a missing
// table is expected when no schema is loaded, but an empty result for a typo'd
// column would silently mask a real bug.
func classifyErr(err error) errClass {
	if err == nil {
		return errApp
	}
	m := strings.ToLower(err.Error())
	switch {
	// A missing relation. Column-not-found also says "does not exist" in some
	// phrasings, so require the table/relation wording to avoid stubbing typos.
	case strings.Contains(m, "does not exist") &&
		(strings.Contains(m, "table with name") ||
			strings.Contains(m, "view with name") ||
			strings.Contains(m, "with name") && !strings.Contains(m, "column")):
		return errMissingRelation
	// Valid SQL that DuckDB doesn't implement, or an unknown session knob -- a
	// spoofdb fidelity limit, not an app bug.
	case strings.Contains(m, "not implemented error"):
		return errDialectGap
	case strings.Contains(m, "unrecognized configuration parameter"):
		return errDialectGap
	default:
		// Binder "Referenced column ... not found", "Parser Error: syntax ...",
		// and everything else: the app's problem -- surface it.
		return errApp
	}
}

// missingRelationName best-effort extracts the relation name from a DuckDB
// "... with name X does not exist!" catalog error, for a helpful log line.
func missingRelationName(err error) string {
	if err == nil {
		return "?"
	}
	if m := reMissingName.FindStringSubmatch(err.Error()); m != nil {
		return strings.Trim(m[1], `"`)
	}
	return "?"
}

var reMissingName = regexp.MustCompile(`with name "?([^"\s]+)"? does not exist`)

// firstLine returns the first line of s (DuckDB errors are multi-line with a
// "Did you mean" hint and a caret), for compact one-time logging.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
