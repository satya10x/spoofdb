package pg

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/fidelity"
)

// portal is a bound statement ready to execute, plus the result-column formats
// the client requested (empty = all text, one = applies to all, else per-column).
type portal struct {
	sql     string
	formats []int16
}

// session holds per-connection protocol state.
type session struct {
	be      *pgproto3.Backend
	eng     *engine.Engine
	stmts   map[string]string  // prepared statement name -> SQL
	portals map[string]*portal // portal name -> bound portal
	failed  bool               // set on error; messages skipped until Sync

	// memo of the most recent query result, so an extended-protocol
	// Describe followed by Execute of the same SQL runs DuckDB once and
	// reports a consistent shape and rows. Reset on Sync.
	lastSQL string
	lastRes *engine.QueryResult
	lastErr error
}

// run executes sql and memoizes a successful result so a Describe immediately
// followed by Execute of the same SQL hits DuckDB once. Errors are not memoized,
// but a failed Describe sets failed and skips the matching Execute, so the query
// still runs only once. Reset on Sync.
func (s *session) run(sql string) (*engine.QueryResult, error) {
	if s.lastRes != nil && s.lastSQL == sql {
		return s.lastRes, s.lastErr
	}
	res, err := s.eng.Run(sql)
	s.lastSQL, s.lastRes, s.lastErr = sql, res, err
	return res, err
}

func handleConn(conn net.Conn, eng *engine.Engine) {
	defer conn.Close()
	// A panic decoding hostile/malformed wire bytes must not crash the process;
	// contain it to this connection.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("pg: recovered from panic on connection: %v", r)
		}
	}()
	s := &session{
		be:      pgproto3.NewBackend(conn, conn),
		eng:     eng,
		stmts:   map[string]string{},
		portals: map[string]*portal{},
	}
	if err := s.startup(conn); err != nil {
		return
	}
	for {
		msg, err := s.be.Receive()
		if err != nil {
			return
		}
		if s.handle(msg) {
			return
		}
	}
}

// startup performs the Postgres startup handshake, declining SSL/GSS so the
// client falls back to plaintext.
func (s *session) startup(conn net.Conn) error {
	for {
		msg, err := s.be.ReceiveStartupMessage()
		if err != nil {
			return err
		}
		switch msg.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return err
			}
		case *pgproto3.StartupMessage:
			s.be.Send(&pgproto3.AuthenticationOk{})
			params := map[string]string{
				"server_version":              "15.0 (spoofdb)",
				"server_encoding":             "UTF8",
				"client_encoding":             "UTF8",
				"DateStyle":                   "ISO, MDY",
				"TimeZone":                    "UTC",
				"integer_datetimes":           "on",
				"standard_conforming_strings": "on",
			}
			for k, v := range params {
				s.be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
			}
			s.be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
			s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			return s.be.Flush()
		default:
			return fmt.Errorf("pg: unexpected startup message %T", msg)
		}
	}
}

// handle dispatches one frontend message, returning true to close the conn.
func (s *session) handle(msg pgproto3.FrontendMessage) (quit bool) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		s.simpleQuery(m.String)
	case *pgproto3.Parse:
		if !s.failed {
			s.stmts[m.Name] = m.Query
			s.be.Send(&pgproto3.ParseComplete{})
		}
	case *pgproto3.Bind:
		if !s.failed {
			s.portals[m.DestinationPortal] = &portal{
				sql:     s.stmts[m.PreparedStatement],
				formats: m.ResultFormatCodes,
			}
			s.be.Send(&pgproto3.BindComplete{})
		}
	case *pgproto3.Describe:
		if !s.failed {
			s.describe(m)
		}
	case *pgproto3.Execute:
		if !s.failed {
			s.execute(m)
		}
	case *pgproto3.Sync:
		s.failed = false
		s.lastSQL, s.lastRes, s.lastErr = "", nil, nil
		s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		s.be.Flush()
	case *pgproto3.Flush:
		s.be.Flush()
	case *pgproto3.Close:
		// Drop the named statement or portal so it can't be reused stale and
		// the maps don't grow on a long-lived connection.
		if m.ObjectType == 'S' {
			delete(s.stmts, m.Name)
		} else {
			delete(s.portals, m.Name)
		}
		s.be.Send(&pgproto3.CloseComplete{})
	case *pgproto3.Terminate:
		return true
	}
	return false
}

// simpleQuery handles the simple query protocol ('Q'); results are text format.
func (s *session) simpleQuery(sql string) {
	// Postgres replies with EmptyQueryResponse (not an error) when the query
	// string carries no statement -- e.g. "" or a bare ";". lib/pq's Ping()
	// sends ";", so mishandling this surfaces as "driver: bad connection".
	if strings.Trim(sql, "; \t\r\n\v\f") == "" {
		s.be.Send(&pgproto3.EmptyQueryResponse{})
		s.ready()
		return
	}
	res, err := s.eng.Run(sql)
	if err != nil {
		s.sendError(err)
		s.ready()
		return
	}
	if len(res.Cols) > 0 {
		s.be.Send(rowDescription(res, nil))
		for _, row := range res.Rows {
			s.be.Send(dataRow(res, row, nil))
		}
	}
	s.emitFidelity(report(res))
	s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag(sql, len(res.Rows)))})
	s.ready()
}

// emitFidelity logs a server-side summary and sends each approximation to the
// client as a NoticeResponse (warning), so lossy spoofing is never silent.
func (s *session) emitFidelity(rep *fidelity.Report) {
	if rep.Empty() {
		return
	}
	log.Printf("pg: %s", rep.Summary())
	for _, w := range rep.Warnings() {
		s.be.Send(&pgproto3.NoticeResponse{Severity: "WARNING", Code: "01000", Message: "spoofdb: " + w})
	}
}

// describe answers a Describe by reporting the result's columns. It runs the
// query (via the run memo) to learn its shape; the matching Execute reuses that
// memoized result, so DuckDB sees the query once per Describe/Execute pair.
func (s *session) describe(m *pgproto3.Describe) {
	var sql string
	var formats []int16
	if m.ObjectType == 'S' {
		sql = s.stmts[m.Name]
		s.be.Send(&pgproto3.ParameterDescription{})
	} else if p := s.portals[m.Name]; p != nil {
		sql, formats = p.sql, p.formats
	}
	res, err := s.run(sql)
	if err != nil {
		s.sendError(err)
		s.failed = true
		return
	}
	if len(res.Cols) == 0 {
		s.be.Send(&pgproto3.NoData{})
		return
	}
	s.be.Send(rowDescription(res, formats))
}

// execute runs a bound portal and streams its rows in the requested formats.
func (s *session) execute(m *pgproto3.Execute) {
	p := s.portals[m.Portal]
	if p == nil {
		p = &portal{}
	}
	res, err := s.run(p.sql)
	if err != nil {
		s.sendError(err)
		s.failed = true
		return
	}
	for _, row := range res.Rows {
		s.be.Send(dataRow(res, row, p.formats))
	}
	s.emitFidelity(report(res))
	s.be.Send(&pgproto3.CommandComplete{CommandTag: []byte(commandTag(p.sql, len(res.Rows)))})
}

func (s *session) ready() {
	s.be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	s.be.Flush()
}

func (s *session) sendError(err error) {
	s.be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: err.Error()})
}

func rowDescription(res *engine.QueryResult, formats []int16) *pgproto3.RowDescription {
	fields := make([]pgproto3.FieldDescription, len(res.Cols))
	for i, c := range res.Cols {
		fields[i] = pgproto3.FieldDescription{
			Name:         []byte(c.Name),
			DataTypeOID:  oidForType(c.Type),
			DataTypeSize: -1,
			TypeModifier: -1,
			Format:       resolveFormat(formats, i),
		}
	}
	return &pgproto3.RowDescription{Fields: fields}
}

func dataRow(res *engine.QueryResult, row []any, formats []int16) *pgproto3.DataRow {
	vals := make([][]byte, len(row))
	for i, v := range row {
		oid := uint32(25)
		if i < len(res.Cols) {
			oid = oidForType(res.Cols[i].Type)
		}
		vals[i] = encodeValue(v, oid, resolveFormat(formats, i))
	}
	return &pgproto3.DataRow{Values: vals}
}

// resolveFormat applies the libpq result-format-code rule: none -> text, one ->
// applies to every column, otherwise per-column.
func resolveFormat(formats []int16, col int) int16 {
	switch len(formats) {
	case 0:
		return 0
	case 1:
		return formats[0]
	default:
		if col < len(formats) {
			return formats[col]
		}
		return 0
	}
}

// commandTag builds the CommandComplete tag for a statement.
func commandTag(sql string, n int) string {
	fields := strings.Fields(strings.TrimSpace(sql))
	if len(fields) == 0 {
		return ""
	}
	switch word := strings.ToUpper(fields[0]); word {
	case "SELECT", "WITH", "SHOW", "TABLE", "VALUES":
		return fmt.Sprintf("SELECT %d", n)
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", n)
	case "UPDATE", "DELETE":
		return fmt.Sprintf("%s %d", word, n)
	default:
		return word
	}
}

// oidForType maps a DuckDB type name to a PostgreSQL type OID. DECIMAL/NUMERIC
// are mapped to float8 (701): the binary numeric wire format is complex and
// approximate precision is fine for a spoofing tool.
func oidForType(t string) uint32 {
	base := strings.ToUpper(t)
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i] // strip precision/scale, e.g. DECIMAL(5,2) -> DECIMAL
	}
	switch base {
	case "BOOLEAN", "BOOL":
		return 16
	case "TINYINT", "SMALLINT", "INT2", "UTINYINT":
		return 21 // int2 (UTINYINT 0-255 fits)
	case "INTEGER", "INT", "INT4", "USMALLINT":
		return 23 // int4 (USMALLINT 0-65535 needs int4)
	case "BIGINT", "INT8", "UINTEGER":
		return 20 // int8 (UINTEGER 0-~4.29e9 needs int8)
	case "HUGEINT", "UBIGINT", "UHUGEINT":
		return 20 // int8; values beyond int64 wrap (reported via fidelity)
	case "REAL", "FLOAT", "FLOAT4":
		return 700
	case "DOUBLE", "FLOAT8", "DECIMAL", "NUMERIC":
		return 701
	case "DATE":
		return 1082
	case "TIMESTAMP", "DATETIME":
		return 1114
	default:
		return 25 // text / varchar / json / time / fallback
	}
}

// pgEpoch is PostgreSQL's binary date/timestamp epoch.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// encodeValue renders v in the requested wire format (0 = text, 1 = binary).
// A nil result encodes SQL NULL.
func encodeValue(v any, oid uint32, format int16) []byte {
	if v == nil {
		return nil
	}
	if format == 1 {
		return encodeBinary(v, oid)
	}
	return encodeText(v)
}

func encodeBinary(v any, oid uint32) []byte {
	switch oid {
	case 16:
		if toBool(v) {
			return []byte{1}
		}
		return []byte{0}
	case 21:
		b := make([]byte, 2)
		binary.BigEndian.PutUint16(b, uint16(int16(toInt(v))))
		return b
	case 23:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, uint32(int32(toInt(v))))
		return b
	case 20:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(toInt(v)))
		return b
	case 700:
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, math.Float32bits(float32(toFloat(v))))
		return b
	case 701:
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, math.Float64bits(toFloat(v)))
		return b
	case 1114: // timestamp: microseconds since 2000-01-01
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(toTime(v).Sub(pgEpoch).Microseconds()))
		return b
	case 1082: // date: days since 2000-01-01 (floored, so pre-epoch dates round down)
		b := make([]byte, 4)
		days := int32(math.Floor(toTime(v).Sub(pgEpoch).Hours() / 24))
		binary.BigEndian.PutUint32(b, uint32(days))
		return b
	default:
		return encodeText(v)
	}
}

// encodeText renders a scanned value in PostgreSQL text format.
func encodeText(v any) []byte {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		if x {
			return []byte("t")
		}
		return []byte("f")
	case time.Time:
		return []byte(x.Format("2006-01-02 15:04:05.999999"))
	case string:
		return []byte(x)
	case []byte:
		return x
	default:
		return []byte(fmt.Sprint(x))
	}
}

func toInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case uint32:
		return int64(x)
	case float64:
		return int64(x)
	case bool:
		if x {
			return 1
		}
		return 0
	default:
		n, _ := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(x)), 10, 64)
		return n
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	default:
		f, _ := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(x)), 64)
		return f
	}
}

func toBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x == "true" || x == "t" || x == "1"
	default:
		return toInt(v) != 0
	}
}

func toTime(v any) time.Time {
	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		for _, layout := range []string{"2006-01-02 15:04:05.999999", "2006-01-02 15:04:05", "2006-01-02"} {
			if t, err := time.Parse(layout, x); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}
