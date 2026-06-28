package clickhouse

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satya10x/spoofdb/internal/engine"
)

func connect(t *testing.T) driver.Conn {
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

	conn, err := chgo.Open(&chgo.Options{
		Addr:     []string{ln.Addr().String()},
		Protocol: chgo.Native,
		// compression must be disabled (see package doc)
		Settings: chgo.Settings{},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestSelectRows(t *testing.T) {
	conn := connect(t)
	ctx := context.Background()

	rows, err := conn.Query(ctx, "SELECT id, name FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var n int
	var firstID int32
	var firstName string
	for rows.Next() {
		var id int32
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if n == 0 {
			firstID, firstName = id, name
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("rows = %d, want 5", n)
	}
	if firstID != 1 || firstName != "alice" {
		t.Errorf("row0 = (%d,%q), want (1,alice)", firstID, firstName)
	}
}

func TestScalarAndJoin(t *testing.T) {
	conn := connect(t)
	ctx := context.Background()

	var count int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("orders count = %d, want 5", count)
	}

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

func TestTimestamp(t *testing.T) {
	conn := connect(t)
	var ts time.Time
	if err := conn.QueryRow(context.Background(),
		"SELECT created_at FROM users WHERE id = 1").Scan(&ts); err != nil {
		t.Fatalf("timestamp: %v", err)
	}
	if ts.Year() != 2024 || ts.Month() != time.January {
		t.Errorf("created_at = %v, want Jan 2024", ts)
	}
}

func TestReportDetectsNullAndUnsigned(t *testing.T) {
	res := &engine.QueryResult{
		Cols: []engine.Column{{Name: "n", Type: "BIGINT"}, {Name: "u", Type: "UINTEGER"}},
		Rows: [][]any{{nil, uint32(5)}},
	}
	joined := strings.Join(report(res).Warnings(), " | ")
	if !strings.Contains(joined, "NULL") || !strings.Contains(joined, `"n"`) {
		t.Errorf("missing NULL warning: %s", joined)
	}
	if !strings.Contains(joined, "unsigned") || !strings.Contains(joined, `"u"`) {
		t.Errorf("missing unsigned warning: %s", joined)
	}
}

func TestFidelityServerLog(t *testing.T) {
	conn := connect(t)
	// orders.amount is DECIMAL, approximated as Float64 — the client should get
	// a server log saying so via WithLogs.
	var mu sync.Mutex
	var texts []string
	ctx := chgo.Context(context.Background(), chgo.WithLogs(func(l *chgo.Log) {
		mu.Lock()
		texts = append(texts, l.Text)
		mu.Unlock()
	}))

	var amount float64
	if err := conn.QueryRow(ctx, "SELECT amount FROM orders WHERE id = 103").Scan(&amount); err != nil {
		t.Fatalf("query: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, txt := range texts {
		if strings.Contains(txt, "DECIMAL") && strings.Contains(txt, "amount") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a DECIMAL approximation server log for amount, got %v", texts)
	}
}

func TestPing(t *testing.T) {
	if err := connect(t).Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
}
