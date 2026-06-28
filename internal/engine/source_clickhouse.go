package engine

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// clickhousePull snapshots a live ClickHouse database into DuckDB. DuckDB has no
// ClickHouse reader, so this connects with clickhouse-go, lists the tables, and
// copies each into a local DuckDB table (CREATE + INSERT), then disconnects —
// an offline snapshot like the other sources.
func (e *Engine) clickhousePull(src Source) error {
	if src.DSN == "" {
		return fmt.Errorf("engine: source %q: empty DSN", src.Name)
	}
	opts, err := clickhouse.ParseDSN(src.DSN)
	if err != nil {
		return fmt.Errorf("engine: source %q: parse clickhouse dsn: %w", src.Name, err)
	}
	chdb := clickhouse.OpenDB(opts)
	defer chdb.Close()
	if err := chdb.Ping(); err != nil {
		return fmt.Errorf("engine: source %q: connect clickhouse: %w", src.Name, err)
	}

	tables, err := clickhouseTables(chdb, src.Tables)
	if err != nil {
		return fmt.Errorf("engine: source %q: %w", src.Name, err)
	}
	for _, t := range tables {
		if prev, dup := e.seen[t]; dup {
			log.Printf("engine: table %q already registered from %q; skipping %q", t, prev, src.Name)
			continue
		}
		if err := e.copyClickhouseTable(chdb, t); err != nil {
			return fmt.Errorf("engine: source %q: copy table %q: %w", src.Name, t, err)
		}
		e.seen[t] = src.Name
		e.tables = append(e.tables, t)
	}
	return nil
}

// clickhouseTables returns the allowlist if given, else every table in the
// connection's current database.
func clickhouseTables(chdb *sql.DB, allow []string) ([]string, error) {
	if len(allow) > 0 {
		return allow, nil
	}
	rows, err := chdb.Query("SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list clickhouse tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// copyClickhouseTable reads one ClickHouse table and writes it into DuckDB,
// creating the table with mapped column types and inserting the rows.
func (e *Engine) copyClickhouseTable(chdb *sql.DB, table string) error {
	rows, err := chdb.Query("SELECT * FROM `" + strings.ReplaceAll(table, "`", "``") + "`")
	if err != nil {
		return err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	defs := make([]string, len(names))
	for i, n := range names {
		defs[i] = quoteIdent(n) + " " + chToDuckType(types[i].DatabaseTypeName())
	}
	if _, err := e.db.Exec(fmt.Sprintf("CREATE OR REPLACE TABLE %s (%s)", quoteIdent(table), strings.Join(defs, ", "))); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")
	ins, err := e.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", quoteIdent(table), placeholders))
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer ins.Close()

	vals := make([]any, len(names))
	ptrs := make([]any, len(names))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		args := make([]any, len(vals))
		for i, v := range vals {
			args[i] = normalizeForDuck(v)
		}
		if _, err := ins.Exec(args...); err != nil {
			return fmt.Errorf("insert row: %w", err)
		}
	}
	return rows.Err()
}

// chToDuckType maps a ClickHouse column type to a DuckDB type. Nullable() and
// LowCardinality() wrappers are unwrapped and parameterized types matched on
// their base name; anything unrecognized (Array/Map/Tuple/Decimal/UUID/Enum/...)
// falls back to VARCHAR, with the value stored as text.
func chToDuckType(t string) string {
	base := strings.TrimSpace(t)
	for {
		if inner, ok := unwrapType(base, "Nullable"); ok {
			base = inner
			continue
		}
		if inner, ok := unwrapType(base, "LowCardinality"); ok {
			base = inner
			continue
		}
		break
	}
	name := base
	if i := strings.IndexByte(name, '('); i >= 0 {
		name = name[:i]
	}
	switch name {
	case "UInt8":
		return "UTINYINT"
	case "UInt16":
		return "USMALLINT"
	case "UInt32":
		return "UINTEGER"
	case "UInt64":
		return "UBIGINT"
	case "Int8":
		return "TINYINT"
	case "Int16":
		return "SMALLINT"
	case "Int32":
		return "INTEGER"
	case "Int64":
		return "BIGINT"
	case "Float32":
		return "FLOAT"
	case "Float64":
		return "DOUBLE"
	case "Bool", "Boolean":
		return "BOOLEAN"
	case "Date", "Date32":
		return "DATE"
	case "DateTime", "DateTime64":
		return "TIMESTAMP"
	default:
		return "VARCHAR"
	}
}

// unwrapType returns the inner type of wrapper(inner) and true, else "" and false.
func unwrapType(s, wrapper string) (string, bool) {
	prefix := wrapper + "("
	if strings.HasPrefix(s, prefix) && strings.HasSuffix(s, ")") {
		return s[len(prefix) : len(s)-1], true
	}
	return "", false
}

// normalizeForDuck converts a clickhouse-go scanned value into something the
// DuckDB driver can bind: basic scalars pass through, []byte becomes a string,
// and anything else (decimals, arrays, maps) is stringified to match the
// VARCHAR fallback in chToDuckType.
func normalizeForDuck(v any) any {
	switch x := v.(type) {
	case nil, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, string, time.Time:
		return v
	case []byte:
		return string(x)
	default:
		return fmt.Sprint(x)
	}
}
