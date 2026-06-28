package main

import (
	"path/filepath"
	"testing"
)

func TestParseSpoof(t *testing.T) {
	cases := []struct {
		in    string
		proto string
		port  int
		bad   bool
	}{
		{"mysql:3306", "mysql", 3306, false},
		{"postgres", "postgres", 5432, false},
		{"pg:5433", "postgres", 5433, false},
		{"ch:9000", "clickhouse", 9000, false},
		{"redis", "redis", 6379, false},
		{"mongodb:1", "", 0, true},
		{"mysql:notaport", "", 0, true},
	}
	for _, c := range cases {
		proto, port, err := parseSpoof(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("parseSpoof(%q) = (%q,%d), want error", c.in, proto, port)
			}
			continue
		}
		if err != nil || proto != c.proto || port != c.port {
			t.Errorf("parseSpoof(%q) = (%q,%d,%v), want (%q,%d)", c.in, proto, port, err, c.proto, c.port)
		}
	}
}

func TestParseSource(t *testing.T) {
	dir, _ := filepath.Abs("../../testdata")

	// directory
	if src, isRedis, err := parseSource(dir); err != nil || isRedis || src.Type != "dir" || src.Path != dir {
		t.Errorf("dir source = %+v isRedis=%v err=%v", src, isRedis, err)
	}
	// parquet file
	pq := filepath.Join(dir, "users.parquet")
	if src, _, err := parseSource(pq); err != nil || src.Type != "parquet" || src.Path != pq {
		t.Errorf("parquet source = %+v err=%v", src, err)
	}
	// rdb -> redis
	if _, isRedis, err := parseSource(filepath.Join(dir, "cache.rdb")); err != nil || !isRedis {
		t.Errorf("rdb source: isRedis=%v err=%v, want isRedis=true", isRedis, err)
	}
	// DSN
	if src, _, err := parseSource("postgres://u:p@host:5432/db"); err != nil || src.Type != "postgres" || src.DSN == "" {
		t.Errorf("postgres dsn source = %+v err=%v", src, err)
	}
	// unknown
	if _, _, err := parseSource("/nonexistent/thing.xyz"); err == nil {
		t.Error("expected error for nonexistent/unknown source")
	}
}

func TestConfigFromFlags(t *testing.T) {
	dir, _ := filepath.Abs("../../testdata")
	cfg, err := configFromFlags(
		[]string{dir},
		[]string{"mysql:3306", "mysql:3307", "postgres:5433"},
	)
	if err != nil {
		t.Fatalf("configFromFlags: %v", err)
	}
	if len(cfg.Sources) != 1 {
		t.Errorf("sources = %d, want 1", len(cfg.Sources))
	}
	if len(cfg.Spoof.MySQL) != 2 {
		t.Errorf("mysql shards = %d, want 2", len(cfg.Spoof.MySQL))
	}
	if len(cfg.Spoof.Postgres) != 1 {
		t.Errorf("postgres instances = %d, want 1", len(cfg.Spoof.Postgres))
	}
	// ports landed
	ports := map[int]bool{}
	for _, inst := range cfg.Spoof.MySQL {
		ports[inst.Port] = true
	}
	if !ports[3306] || !ports[3307] {
		t.Errorf("mysql ports = %v, want 3306+3307", ports)
	}

	// redis requires a matching .rdb source
	if _, err := configFromFlags([]string{dir}, []string{"redis:6379"}); err == nil {
		t.Error("expected error: redis spoof with no .rdb source")
	}
	rdb := filepath.Join(dir, "cache.rdb")
	cfg2, err := configFromFlags([]string{rdb}, []string{"redis:6379"})
	if err != nil {
		t.Fatalf("redis configFromFlags: %v", err)
	}
	if len(cfg2.Redis) != 1 || cfg2.Redis["shard0"].Dump != rdb {
		t.Errorf("redis instance = %+v", cfg2.Redis)
	}
}
