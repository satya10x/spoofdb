package mysql

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"github.com/satya10x/spoofdb/internal/engine"
)

// acceptAuth authenticates any username with an empty password. (Spoofing tool:
// auth is not enforced; point apps at it with no password.)
type acceptAuth struct{}

func (acceptAuth) GetCredential(string) (server.Credential, bool, error) {
	return server.Credential{
		Passwords:      []string{""},
		AuthPluginName: gomysql.AUTH_CACHING_SHA2_PASSWORD,
	}, true, nil
}
func (acceptAuth) OnAuthSuccess(*server.Conn) error  { return nil }
func (acceptAuth) OnAuthFailure(*server.Conn, error) {}

// handler implements go-mysql's server.Handler over the DuckDB engine. One
// handler is created per connection, so warnings (set by the last statement and
// retrievable via SHOW WARNINGS) need no synchronization.
type handler struct {
	eng      *engine.Engine
	warnings []string
}

func (h *handler) UseDB(string) error { return nil }

func (h *handler) HandleQuery(query string) (*gomysql.Result, error) {
	return h.exec(query, false)
}

func (h *handler) HandleFieldList(string, string) ([]*gomysql.Field, error) {
	return nil, nil
}

// HandleStmtPrepare reports the placeholder count; the column count is learned
// from the execute response, so 0 here is fine for typical clients.
func (h *handler) HandleStmtPrepare(query string) (params int, columns int, ctx any, err error) {
	return strings.Count(query, "?"), 0, query, nil
}

func (h *handler) HandleStmtExecute(_ any, query string, args []any) (*gomysql.Result, error) {
	return h.exec(substituteArgs(query, args), true)
}

func (h *handler) HandleStmtClose(any) error { return nil }

// backticksToQuotes rewrites MySQL backtick-quoted identifiers as standard
// double-quoted identifiers so DuckDB (which doesn't accept backticks) can parse
// the query. Backticks inside single-quoted string literals are left untouched.
func backticksToQuotes(q string) string {
	if !strings.Contains(q, "`") {
		return q
	}
	var b strings.Builder
	b.Grow(len(q))
	inStr := false
	for i := 0; i < len(q); i++ {
		c := q[i]
		switch {
		case c == '\'':
			inStr = !inStr // '' (escaped quote) nets back to the same state
			b.WriteByte(c)
		case c == '`' && !inStr:
			b.WriteByte('"')
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func (h *handler) HandleOtherCommand(cmd byte, _ []byte) error {
	return fmt.Errorf("mysql: unsupported command %d", cmd)
}

// exec runs a fully-substituted statement against DuckDB and builds the wire
// result. binary selects the prepared-statement (binary) resultset encoding.
func (h *handler) exec(query string, binary bool) (*gomysql.Result, error) {
	// MySQL quotes identifiers with backticks; DuckDB uses standard double quotes.
	// Rewrite them so MySQL-dialect SQL (e.g. `select` `from`) parses instead of
	// failing with a syntax error. Done before TrimSpace checks so SHOW/SET etc.
	// are unaffected (they carry no backticks).
	query = backticksToQuotes(query)
	q := strings.TrimSpace(query)
	if q == "" {
		return okResult(0), nil
	}
	// SHOW WARNINGS returns the approximations recorded by the previous
	// statement on this connection; it must not reset them or hit DuckDB.
	if isShowWarnings(q) {
		return h.showWarnings()
	}

	res, err := h.eng.Run(query)
	if err != nil {
		// Session/transaction-control statements DuckDB doesn't understand
		// (e.g. "SET NAMES utf8mb4") are accepted as no-ops so clients connect
		// and run cleanly; everything else surfaces the real error. Running
		// first means session statements DuckDB *does* support still apply.
		if isSessionNoop(firstWord(q)) {
			return okResult(0), nil
		}
		return nil, err
	}
	// A new statement resets the connection's warning set.
	h.warnings = nil
	if len(res.Cols) == 0 {
		return okResult(len(res.Rows)), nil
	}

	names := make([]string, len(res.Cols))
	for i, c := range res.Cols {
		names[i] = c.Name
	}
	values := make([][]any, len(res.Rows))
	for i, row := range res.Rows {
		vr := make([]any, len(row))
		for j, v := range row {
			vr[j] = coerce(v)
		}
		values[i] = vr
	}

	var rs *gomysql.Resultset
	if binary {
		rs, err = gomysql.BuildSimpleBinaryResultset(names, values)
	} else {
		rs, err = gomysql.BuildSimpleTextResultset(names, values)
	}
	if err != nil {
		return nil, err
	}
	result := gomysql.NewResult(rs)
	if rep := report(res); !rep.Empty() {
		log.Printf("mysql: %s", rep.Summary())
		h.warnings = rep.Warnings()
		result.Warnings = uint16(len(h.warnings))
	}
	return result, nil
}

// isShowWarnings matches "SHOW WARNINGS" (whitespace-normalized, optional
// trailing semicolon / LIMIT clause).
func isShowWarnings(q string) bool {
	u := strings.ToUpper(strings.Join(strings.Fields(q), " "))
	u = strings.TrimSuffix(u, ";")
	return u == "SHOW WARNINGS" || strings.HasPrefix(u, "SHOW WARNINGS ")
}

// showWarnings returns the recorded approximations as a Level/Code/Message
// result set, mirroring MySQL's SHOW WARNINGS shape.
func (h *handler) showWarnings() (*gomysql.Result, error) {
	names := []string{"Level", "Code", "Message"}
	values := make([][]any, len(h.warnings))
	for i, w := range h.warnings {
		values[i] = []any{"Warning", int64(1105), "spoofdb: " + w}
	}
	rs, err := gomysql.BuildSimpleTextResultset(names, values)
	if err != nil {
		return nil, err
	}
	return gomysql.NewResult(rs), nil
}

func okResult(affected int) *gomysql.Result {
	return &gomysql.Result{
		Status:       gomysql.SERVER_STATUS_AUTOCOMMIT,
		AffectedRows: uint64(affected),
	}
}

// coerce maps a DuckDB-scanned value to a type go-mysql can encode. bool and
// types it doesn't recognize (e.g. DECIMAL) become int/string respectively.
func coerce(v any) any {
	switch x := v.(type) {
	case nil, int8, int16, int32, int64, int,
		uint8, uint16, uint32, uint64, uint,
		float32, float64, string, []byte, time.Time:
		return v
	case bool:
		if x {
			return int64(1)
		}
		return int64(0)
	default:
		return fmt.Sprint(x)
	}
}

func firstWord(q string) string {
	if i := strings.IndexAny(q, " \t\r\n"); i >= 0 {
		return strings.ToUpper(q[:i])
	}
	return strings.ToUpper(q)
}

// isSessionNoop reports whether a statement whose first word is word is a
// session/transaction-control statement to accept as a no-op when DuckDB
// rejects it.
func isSessionNoop(word string) bool {
	switch word {
	case "SET", "USE", "BEGIN", "START", "COMMIT", "ROLLBACK":
		return true
	}
	return false
}

// substituteArgs inlines prepared-statement parameters as quoted SQL literals.
// Placeholders inside string literals are not special-cased, which is fine for
// the parameterized queries clients actually send.
func substituteArgs(query string, args []any) string {
	if len(args) == 0 {
		return query
	}
	var b strings.Builder
	ai := 0
	var quote byte // 0 when not inside a '...', "...", or `...` literal
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case quote != 0:
			b.WriteByte(c)
			if c == '\\' && i+1 < len(query) { // copy the escaped char verbatim
				i++
				b.WriteByte(query[i])
			} else if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
			b.WriteByte(c)
		case c == '?' && ai < len(args):
			b.WriteString(quoteArg(args[ai]))
			ai++
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func quoteArg(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case bool:
		if x {
			return "1"
		}
		return "0"
	case time.Time:
		return "'" + x.Format("2006-01-02 15:04:05.999999") + "'"
	case []byte:
		return "'" + escapeSQL(string(x)) + "'"
	case string:
		return "'" + escapeSQL(x) + "'"
	default:
		return "'" + escapeSQL(fmt.Sprint(x)) + "'"
	}
}

// escapeSQL escapes a string for a DuckDB single-quoted literal. DuckDB uses
// SQL-standard strings — a backslash is an ordinary character and only a doubled
// quote escapes a quote — so only single quotes are doubled. (Doubling
// backslashes would corrupt values like Windows paths or regexes.)
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
