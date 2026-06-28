package pg

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/satya10x/spoofdb/internal/engine"
)

func startServer(t *testing.T) string {
	t.Helper()
	dir, _ := filepath.Abs("../../testdata")
	eng, err := engine.New(dir)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go Serve(ln, eng)
	return ln.Addr().String()
}

func connect(t *testing.T, addr string) *pgx.Conn {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://any:any@"+addr+"/any?sslmode=disable")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	return conn
}

func TestSelectRows(t *testing.T) {
	conn := connect(t, startServer(t))
	ctx := context.Background()

	rows, err := conn.Query(ctx, "SELECT id, name, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	type user struct {
		id     int64
		name   string
		active bool
	}
	var got []user
	for rows.Next() {
		var u user
		if err := rows.Scan(&u.id, &u.name, &u.active); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, u)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("rows = %d, want 5", len(got))
	}
	if got[0].id != 1 || got[0].name != "alice" || got[0].active != true {
		t.Errorf("row0 = %+v, want {1 alice true}", got[0])
	}
}

func TestScalarsAndAggregate(t *testing.T) {
	conn := connect(t, startServer(t))
	ctx := context.Background()

	var count int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("orders count = %d, want 5", count)
	}

	// join across two parquet-backed tables, executed entirely by DuckDB
	var name string
	var amount float64
	err := conn.QueryRow(ctx,
		"SELECT u.name, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE o.id = 103").
		Scan(&name, &amount)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if name != "bob" || amount != 199.0 {
		t.Errorf("join = %q %v, want bob 199", name, amount)
	}
}

func TestTimestampColumn(t *testing.T) {
	conn := connect(t, startServer(t))
	ctx := context.Background()
	var ts time.Time
	if err := conn.QueryRow(ctx, "SELECT created_at FROM users WHERE id = 1").Scan(&ts); err != nil {
		t.Fatalf("timestamp: %v", err)
	}
	if ts.Year() != 2024 || ts.Month() != time.January {
		t.Errorf("created_at = %v, want Jan 2024", ts)
	}
}

func TestFidelityNotice(t *testing.T) {
	// orders.amount is DECIMAL, which this server approximates as float8 — the
	// client should receive a NoticeResponse saying so.
	dir, _ := filepath.Abs("../../testdata")
	eng, err := engine.New(dir)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go Serve(ln, eng)

	ctx := context.Background()
	cfg, err := pgx.ParseConfig("postgres://any:any@" + ln.Addr().String() + "/any?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		mu.Lock()
		notices = append(notices, n.Message)
		mu.Unlock()
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	var amount float64
	if err := conn.QueryRow(ctx, "SELECT amount FROM orders WHERE id = 103").Scan(&amount); err != nil {
		t.Fatalf("query: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, m := range notices {
		if strings.Contains(m, "DECIMAL") && strings.Contains(m, "amount") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a DECIMAL approximation notice for amount, got %v", notices)
	}
}

func TestUnsignedNoOverflow(t *testing.T) {
	conn := connect(t, startServer(t))
	// USMALLINT 65535 would wrap to -1 if mapped to a signed int2; it must map
	// to int4 and round-trip exactly.
	var u int64
	if err := conn.QueryRow(context.Background(), "SELECT CAST(65535 AS USMALLINT) AS u").Scan(&u); err != nil {
		t.Fatalf("query: %v", err)
	}
	if u != 65535 {
		t.Errorf("USMALLINT 65535 = %d, want 65535", u)
	}
}

func TestQueryError(t *testing.T) {
	conn := connect(t, startServer(t))
	ctx := context.Background()
	// Unknown table should surface as a query error, not a dropped connection.
	if _, err := conn.Exec(ctx, "SELECT * FROM does_not_exist"); err == nil {
		t.Fatal("expected error for unknown table")
	}
	// Connection must remain usable afterwards.
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("post-error query: %v", err)
	}
	if one != 1 {
		t.Errorf("got %d, want 1", one)
	}
}
