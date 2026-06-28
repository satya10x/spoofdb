package clickhouse

import (
	"log"
	"net"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/fidelity"
)

// serverRevision is the protocol revision spoofdb advertises.
const serverRevision = proto.Version

type chSession struct {
	conn    net.Conn
	r       *proto.Reader
	eng     *engine.Engine
	version int // protocol revision: the client's, capped at the server's
}

func handleConn(conn net.Conn, eng *engine.Engine) {
	defer conn.Close()
	// The native-protocol decoder reads untrusted wire bytes; a panic here must
	// not crash the process.
	defer func() {
		if r := recover(); r != nil {
			log.Printf("clickhouse: recovered from panic on connection: %v", r)
		}
	}()
	s := &chSession{conn: conn, r: proto.NewReader(conn), eng: eng}

	if err := s.handshake(); err != nil {
		return
	}
	for {
		code, err := s.r.UVarInt()
		if err != nil {
			return
		}
		switch proto.ClientCode(code) {
		case proto.ClientCodeQuery:
			if err := s.handleQuery(); err != nil {
				return
			}
		case proto.ClientCodePing:
			if err := s.write(func(b *proto.Buffer) { b.PutByte(byte(proto.ServerCodePong)) }); err != nil {
				return
			}
		case proto.ClientCodeData:
			// Stray data packet (no preceding query); consume and ignore.
			if _, err := s.readClientBlock(); err != nil {
				return
			}
		case proto.ClientCodeCancel:
			// nothing to cancel; ignore
		default:
			return
		}
	}
}

// handshake reads the client Hello, replies with the server Hello, and consumes
// the optional post-hello addendum (quota key) sent by modern clients.
func (s *chSession) handshake() error {
	code, err := s.r.UVarInt()
	if err != nil {
		return err
	}
	if proto.ClientCode(code) != proto.ClientCodeHello {
		return errUnexpected
	}
	var hello proto.ClientHello
	if err := hello.Decode(s.r); err != nil {
		return err
	}

	s.version = hello.ProtocolVersion
	if s.version == 0 || s.version > serverRevision {
		s.version = serverRevision
	}

	server := proto.ServerHello{
		Name:        "spoofdb",
		Major:       23,
		Minor:       8,
		Revision:    serverRevision,
		Timezone:    "UTC",
		DisplayName: "spoofdb",
		Patch:       0,
	}
	// ServerHello.EncodeAware writes the ServerCodeHello packet code itself.
	if err := s.write(func(b *proto.Buffer) {
		server.EncodeAware(b, s.version)
	}); err != nil {
		return err
	}

	// Addendum: clients at this revision send a quota-key string after Hello.
	if proto.FeatureAddendum.In(s.version) {
		if _, err := s.r.Str(); err != nil {
			return err
		}
	}
	return nil
}

// handleQuery decodes a query, drains the trailing (empty) client data block,
// executes against DuckDB, and streams the result.
func (s *chSession) handleQuery() error {
	var q proto.Query
	if err := q.DecodeAware(s.r, s.version); err != nil {
		return err
	}
	// Client sends data block(s) after the query; for SELECT this is one empty
	// block terminator.
	if err := s.drainClientData(); err != nil {
		return err
	}

	res, err := s.eng.Run(q.Body)
	if err != nil {
		return s.sendException(err)
	}
	rep := report(res)
	if !rep.Empty() {
		log.Printf("clickhouse: %s", rep.Summary())
	}
	return s.sendResult(res, q.ID, rep)
}

// drainClientData consumes client Data packets until the empty terminator block.
func (s *chSession) drainClientData() error {
	for {
		code, err := s.r.UVarInt()
		if err != nil {
			return err
		}
		if proto.ClientCode(code) != proto.ClientCodeData {
			return errUnexpected
		}
		empty, err := s.readClientBlock()
		if err != nil {
			return err
		}
		if empty {
			return nil
		}
	}
}

// readClientBlock reads one block from the client, returning whether it was the
// empty (0-column) terminator. Non-empty inbound data (INSERT) is not supported.
func (s *chSession) readClientBlock() (empty bool, err error) {
	if proto.FeatureTempTables.In(s.version) {
		if _, err = s.r.Str(); err != nil { // temp table name
			return false, err
		}
	}
	if proto.FeatureBlockInfo.In(s.version) {
		var info proto.BlockInfo
		if err = info.Decode(s.r); err != nil {
			return false, err
		}
	}
	cols, err := s.r.UVarInt()
	if err != nil {
		return false, err
	}
	if _, err = s.r.UVarInt(); err != nil { // rows
		return false, err
	}
	return cols == 0, nil
}

// sendResult streams a header block (column names/types, 0 rows), the data
// block, any fidelity warnings as a Log packet, then EndOfStream — one write.
func (s *chSession) sendResult(res *engine.QueryResult, queryID string, rep *fidelity.Report) error {
	now := time.Now()
	return s.write(func(b *proto.Buffer) {
		writeBlockPacket(b, proto.ServerCodeData, s.version, proto.Block{Columns: len(res.Cols), Rows: 0}, buildInput(res, false))
		if len(res.Rows) > 0 {
			writeBlockPacket(b, proto.ServerCodeData, s.version, proto.Block{Columns: len(res.Cols), Rows: len(res.Rows)}, buildInput(res, true))
		}
		// Server logs carry the approximations to the client (visible via
		// clickhouse-go's WithLogs). Gated on the feature so older clients,
		// which wouldn't expect the packet, never receive it.
		if !rep.Empty() && proto.FeatureServerLogs.In(s.version) {
			writeLogPacket(b, s.version, queryID, now, rep.Warnings())
		}
		b.PutByte(byte(proto.ServerCodeEndOfStream))
	})
}

// writeBlockPacket writes a packet code, the (empty) table name, and a block.
// EncodeBlock only errors on a column row-count mismatch, which the callers here
// never produce.
func writeBlockPacket(b *proto.Buffer, code proto.ServerCode, version int, blk proto.Block, cols []proto.InputColumn) {
	b.PutByte(byte(code))
	if proto.FeatureTempTables.In(version) {
		b.PutString("") // temp table name
	}
	_ = blk.EncodeBlock(b, version, cols)
}

// writeLogPacket emits a ServerLog packet whose rows are the approximation
// warnings, matching the system.text_log column layout clickhouse-go decodes.
func writeLogPacket(b *proto.Buffer, version int, queryID string, now time.Time, warnings []string) {
	var lg proto.Logs
	for _, w := range warnings {
		lg.Time.Append(now)
		lg.TimeMicro.Append(0)
		lg.HostName.Append("spoofdb")
		lg.QueryID.Append(queryID)
		lg.ThreadID.Append(0)
		lg.Priority.Append(4) // Poco "Warning"
		lg.Source.Append("spoofdb")
		lg.Text.Append("spoofdb: " + w)
	}
	cols := []proto.InputColumn{
		{Name: "event_time", Data: &lg.Time},
		{Name: "event_time_microseconds", Data: &lg.TimeMicro},
		{Name: "host_name", Data: &lg.HostName},
		{Name: "query_id", Data: &lg.QueryID},
		{Name: "thread_id", Data: &lg.ThreadID},
		{Name: "priority", Data: &lg.Priority},
		{Name: "source", Data: &lg.Source},
		{Name: "text", Data: &lg.Text},
	}
	blk := proto.Block{Columns: len(cols), Rows: len(warnings)}
	writeBlockPacket(b, proto.ServerCodeLog, version, blk, cols)
}

// sendException encodes a ClickHouse exception packet for a query error.
func (s *chSession) sendException(err error) error {
	return s.write(func(b *proto.Buffer) {
		b.PutByte(byte(proto.ServerCodeException))
		b.PutInt32(1)                  // error code
		b.PutString("DB::Exception")   // name
		b.PutString(err.Error())       // message
		b.PutString("")                // stack trace
		b.PutBool(false)               // has nested
	})
}

// write builds a buffer via fn and flushes it to the connection.
func (s *chSession) write(fn func(*proto.Buffer)) error {
	var b proto.Buffer
	fn(&b)
	_, err := s.conn.Write(b.Buf)
	return err
}
