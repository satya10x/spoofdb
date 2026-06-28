package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/satya10x/spoofdb/internal/engine"
)

var errUnexpected = errors.New("clickhouse: unexpected packet")

// colBuilder wraps a ch-go column with a value appender for a result column.
type colBuilder struct {
	col proto.ColInput
	add func(v any)
}

// newColBuilder picks a ch-go column type for a DuckDB type name.
func newColBuilder(duckType string) colBuilder {
	switch baseType(duckType) {
	case "TINYINT", "SMALLINT", "INTEGER", "INT", "INT2", "INT4",
		"UTINYINT", "USMALLINT", "UINTEGER":
		c := &proto.ColInt32{}
		return colBuilder{c, func(v any) { c.Append(int32(toInt(v))) }}
	case "BIGINT", "INT8", "HUGEINT", "UBIGINT", "UHUGEINT":
		c := &proto.ColInt64{}
		return colBuilder{c, func(v any) { c.Append(toInt(v)) }}
	case "REAL", "FLOAT", "FLOAT4", "DOUBLE", "FLOAT8", "DECIMAL", "NUMERIC":
		c := &proto.ColFloat64{}
		return colBuilder{c, func(v any) { c.Append(toFloat(v)) }}
	case "BOOLEAN", "BOOL":
		c := &proto.ColUInt8{}
		return colBuilder{c, func(v any) {
			if toBool(v) {
				c.Append(1)
			} else {
				c.Append(0)
			}
		}}
	case "TIMESTAMP", "DATETIME":
		c := &proto.ColDateTime{}
		return colBuilder{c, func(v any) { c.Append(toTime(v)) }}
	default: // VARCHAR / TEXT / JSON / DATE / TIME / fallback
		c := &proto.ColStr{}
		return colBuilder{c, func(v any) { c.Append(toStr(v)) }}
	}
}

// buildInput constructs the native column set for a result. When withRows is
// false the columns are left empty (for the schema header block).
func buildInput(res *engine.QueryResult, withRows bool) []proto.InputColumn {
	builders := make([]colBuilder, len(res.Cols))
	for i, c := range res.Cols {
		builders[i] = newColBuilder(c.Type)
	}
	if withRows {
		for _, row := range res.Rows {
			for i, v := range row {
				builders[i].add(v)
			}
		}
	}
	cols := make([]proto.InputColumn, len(res.Cols))
	for i := range builders {
		cols[i] = proto.InputColumn{Name: res.Cols[i].Name, Data: builders[i].col}
	}
	return cols
}

// baseType strips a parameterized suffix, e.g. DECIMAL(5,2) -> DECIMAL.
func baseType(t string) string {
	t = strings.ToUpper(t)
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = t[:i]
	}
	return t
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

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.Format("2006-01-02 15:04:05")
	case nil:
		return ""
	default:
		return fmt.Sprint(x)
	}
}
