package mysql

import (
	"strings"

	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/fidelity"
)

// report describes the approximations applied when encoding res onto the MySQL
// wire. This protocol preserves NULLs and renders big-integer/decimal values as
// their exact text form (via coerce), so only type-level approximations are
// flagged — no value is silently lost.
func report(res *engine.QueryResult) *fidelity.Report {
	r := fidelity.New()
	for _, c := range res.Cols {
		base := strings.ToUpper(c.Type)
		if i := strings.IndexByte(base, '('); i >= 0 {
			base = base[:i]
		}
		switch base {
		case "BOOLEAN", "BOOL":
			r.Add(c.Name, "BOOLEAN shown as integer 0/1")
		case "DECIMAL", "NUMERIC":
			r.Add(c.Name, "DECIMAL shown as text")
		case "HUGEINT", "UHUGEINT":
			r.Add(c.Name, base+" shown as text")
		}
	}
	return r
}
