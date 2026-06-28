// Package pg emulates a PostgreSQL server: it speaks the Postgres wire protocol
// (via pgproto3) and executes every query against the shared DuckDB engine.
package pg

import (
	"fmt"
	"net"

	"github.com/satya10x/spoofdb/internal/engine"
)

// Serve accepts connections on ln and handles each in its own goroutine.
// Binding is done by the caller so a port conflict surfaces synchronously.
func Serve(ln net.Listener, eng *engine.Engine) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("pg: accept: %w", err)
		}
		go handleConn(conn, eng)
	}
}
