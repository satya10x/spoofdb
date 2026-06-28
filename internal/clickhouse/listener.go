// Package clickhouse emulates a ClickHouse server over the native TCP protocol.
// It performs the Hello handshake and answers queries by executing them against
// the shared DuckDB engine, streaming results back as native column blocks.
// Built on the wire primitives of github.com/ClickHouse/ch-go (proto package).
//
// Limitations: clients must disable compression (compress=false / CompressionNone);
// only SELECT-style queries returning row blocks are served (no INSERT data in);
// SQL NULL is rendered as the zero value (columns are not Nullable); DateTime is
// limited to the 1970-2106 range; HUGEINT/large-unsigned/DECIMAL lose precision.
package clickhouse

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
			return fmt.Errorf("clickhouse: accept: %w", err)
		}
		go handleConn(conn, eng)
	}
}
