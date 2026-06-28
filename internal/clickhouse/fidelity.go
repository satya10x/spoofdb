package clickhouse

import (
	"strings"
	"time"

	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/fidelity"
)

// report describes the approximations applied when encoding res onto the
// ClickHouse wire. Unlike pg/mysql this protocol loses real information: columns
// are not Nullable (so NULL becomes a zero value), DateTime is limited to
// 1970-2106, and values wider than the chosen integer type wrap.
func report(res *engine.QueryResult) *fidelity.Report {
	r := fidelity.New()
	for ci, c := range res.Cols {
		base := strings.ToUpper(c.Type)
		if i := strings.IndexByte(base, '('); i >= 0 {
			base = base[:i]
		}

		switch base {
		case "DECIMAL", "NUMERIC":
			r.Add(c.Name, "DECIMAL shown as Float64; precision may be lost")
		case "BOOLEAN", "BOOL":
			r.Add(c.Name, "BOOLEAN shown as UInt8")
		case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT":
			r.Add(c.Name, "unsigned integer shown as signed")
		case "DATE", "TIME", "INTERVAL", "UUID", "BLOB", "BIT":
			r.Add(c.Name, base+" shown as text")
		}

		wideInt := base == "HUGEINT" || base == "UHUGEINT" || base == "UBIGINT"
		isTimestamp := base == "TIMESTAMP" || base == "DATETIME"
		for _, row := range res.Rows {
			if ci >= len(row) {
				continue
			}
			v := row[ci]
			switch {
			case v == nil:
				r.Add(c.Name, "NULL rendered as zero value (column is not Nullable)")
			case wideInt && fidelity.ExceedsInt64(v):
				r.Add(c.Name, "value exceeds int64 and is truncated")
			case isTimestamp && outsideDateTimeRange(v):
				r.Add(c.Name, "timestamp outside DateTime range (1970-2106)")
			}
		}
	}
	return r
}

// outsideDateTimeRange reports whether a timestamp value falls outside the
// uint32-seconds range ClickHouse DateTime can hold.
func outsideDateTimeRange(v any) bool {
	t, ok := v.(time.Time)
	if !ok {
		return false
	}
	secs := t.Unix()
	return secs < 0 || secs > 4294967295 // max uint32
}
