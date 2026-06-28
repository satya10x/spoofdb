package pg

import (
	"strings"

	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/fidelity"
)

// report describes the approximations applied when encoding res onto the
// Postgres wire. NULLs are preserved by this protocol, so they are not flagged.
// The lossy categories mirror the mapping in oidForType and the binary encoders.
func report(res *engine.QueryResult) *fidelity.Report {
	r := fidelity.New()
	for ci, c := range res.Cols {
		base := strings.ToUpper(c.Type)
		if i := strings.IndexByte(base, '('); i >= 0 {
			base = base[:i]
		}
		switch base {
		case "DECIMAL", "NUMERIC":
			r.Add(c.Name, "DECIMAL shown as float8; precision may be lost")
		case "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT":
			r.Add(c.Name, "unsigned integer shown as signed")
		case "TIME", "INTERVAL", "UUID", "BLOB", "BIT":
			r.Add(c.Name, base+" shown as text")
		}
		// A value too wide for the chosen integer type is sent as 0.
		if base == "HUGEINT" || base == "UHUGEINT" || base == "UBIGINT" {
			for _, row := range res.Rows {
				if ci < len(row) && fidelity.ExceedsInt64(row[ci]) {
					r.Add(c.Name, "value exceeds int64 and is sent as 0")
				}
			}
		}
	}
	return r
}
