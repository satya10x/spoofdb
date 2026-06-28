package engine

import (
	"testing"
	"time"
)

func TestCHToDuckType(t *testing.T) {
	cases := map[string]string{
		"UInt8":                 "UTINYINT",
		"UInt64":                "UBIGINT",
		"Int32":                 "INTEGER",
		"Int64":                 "BIGINT",
		"Float64":               "DOUBLE",
		"Bool":                  "BOOLEAN",
		"String":                "VARCHAR",
		"FixedString(8)":        "VARCHAR",
		"DateTime":              "TIMESTAMP",
		"DateTime64(3)":         "TIMESTAMP",
		"Date":                  "DATE",
		"Decimal(18, 4)":        "VARCHAR", // exotic -> text fallback
		"UUID":                  "VARCHAR",
		"Enum8('a' = 1)":        "VARCHAR",
		"Array(Int32)":          "VARCHAR",
		"Nullable(Int32)":       "INTEGER",
		"LowCardinality(String)": "VARCHAR",
		"Nullable(DateTime64(3))": "TIMESTAMP",
		"LowCardinality(Nullable(Int64))": "BIGINT",
	}
	for in, want := range cases {
		if got := chToDuckType(in); got != want {
			t.Errorf("chToDuckType(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeForDuck(t *testing.T) {
	now := time.Now()
	// passthrough scalars
	for _, v := range []any{nil, true, int32(5), uint64(9), 3.14, "hi", now} {
		if got := normalizeForDuck(v); got != v {
			t.Errorf("normalizeForDuck(%v) changed passthrough value to %v", v, got)
		}
	}
	// []byte -> string
	if got := normalizeForDuck([]byte("bytes")); got != "bytes" {
		t.Errorf("[]byte normalize = %v, want \"bytes\"", got)
	}
	// exotic type -> stringified
	type decimalish struct{ s string }
	d := decimalish{"1.23"}
	if got := normalizeForDuck(d); got == any(d) {
		t.Errorf("exotic value was not stringified: %v", got)
	}
}
