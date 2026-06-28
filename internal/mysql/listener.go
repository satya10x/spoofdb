// Package mysql emulates a MySQL server: it speaks the MySQL wire protocol (via
// go-mysql's server package) and executes every query against the shared DuckDB
// engine. Any username connects; an empty password is expected.
package mysql

import (
	"fmt"
	"log"
	"net"

	"github.com/go-mysql-org/go-mysql/server"

	"github.com/satya10x/spoofdb/internal/engine"
)

// Serve accepts connections on ln, completing the MySQL handshake and serving
// each connection in its own goroutine. Binding is done by the caller so a port
// conflict surfaces synchronously.
func Serve(ln net.Listener, eng *engine.Engine) error {
	srv := server.NewDefaultServer()
	auth := acceptAuth{}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("mysql: accept: %w", err)
		}
		go handleConn(srv, auth, conn, eng)
	}
}

func handleConn(srv *server.Server, auth server.AuthenticationHandler, c net.Conn, eng *engine.Engine) {
	// A panic in the handler must not crash the whole process.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("mysql: recovered from panic on connection: %v", r)
		}
	}()
	conn, err := srv.NewCustomizedConn(c, auth, &handler{eng: eng})
	if err != nil {
		c.Close()
		return
	}
	for {
		if err := conn.HandleCommand(); err != nil {
			return
		}
	}
}
