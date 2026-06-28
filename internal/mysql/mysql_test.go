package mysql

import (
	"context"
	"database/sql"
	"net"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/satya10x/spoofdb/internal/engine"
)

func open(t *testing.T) *sql.DB {
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

	db, err := sql.Open("mysql", "any:@tcp("+ln.Addr().String()+")/any")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

func TestTextQuery(t *testing.T) {
	db := open(t)
	rows, err := db.Query("SELECT id, name, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	var n int
	var firstID int64
	var firstName string
	var firstActive int64
	for rows.Next() {
		var id, active int64
		var name string
		if err := rows.Scan(&id, &name, &active); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if n == 0 {
			firstID, firstName, firstActive = id, name, active
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("rows = %d, want 5", n)
	}
	if firstID != 1 || firstName != "alice" || firstActive != 1 {
		t.Errorf("row0 = (%d,%q,%d), want (1,alice,1)", firstID, firstName, firstActive)
	}
}

func TestPreparedAndJoin(t *testing.T) {
	db := open(t)

	// Parameterized query exercises COM_STMT_PREPARE/EXECUTE (binary protocol).
	var name string
	var amount float64
	err := db.QueryRow(
		"SELECT u.name, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE o.id = ?", 103).
		Scan(&name, &amount)
	if err != nil {
		t.Fatalf("prepared join: %v", err)
	}
	if name != "bob" || amount != 199.0 {
		t.Errorf("join = %q %v, want bob 199", name, amount)
	}
}

func TestAggregate(t *testing.T) {
	db := open(t)
	var count int64
	if err := db.QueryRow("SELECT count(*) FROM orders").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("orders count = %d, want 5", count)
	}
}

func TestFidelityShowWarnings(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	// Pin one connection so SHOW WARNINGS reaches the same handler that ran the
	// query (database/sql would otherwise pick any pooled connection).
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// orders.amount is DECIMAL, which this server renders as text.
	var amount float64
	if err := conn.QueryRowContext(ctx, "SELECT amount FROM orders WHERE id = 103").Scan(&amount); err != nil {
		t.Fatalf("query: %v", err)
	}

	rows, err := conn.QueryContext(ctx, "SHOW WARNINGS")
	if err != nil {
		t.Fatalf("show warnings: %v", err)
	}
	defer rows.Close()
	var found bool
	for rows.Next() {
		var level, message string
		var code int
		if err := rows.Scan(&level, &code, &message); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(message, "DECIMAL") && strings.Contains(message, "amount") {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("expected a DECIMAL approximation warning for amount via SHOW WARNINGS")
	}
}

func TestQueryError(t *testing.T) {
	db := open(t)
	if _, err := db.Query("SELECT * FROM nope_missing"); err == nil {
		t.Fatal("expected error for unknown table")
	}
	// Connection pool must still be usable.
	var one int
	if err := db.QueryRow("SELECT 1").Scan(&one); err != nil {
		t.Fatalf("post-error query: %v", err)
	}
	if one != 1 {
		t.Errorf("got %d, want 1", one)
	}
}
