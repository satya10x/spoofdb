package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSpoofdbConfig(t *testing.T) {
	path, _ := filepath.Abs("../../config/spoofdb.huml")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// sources
	files, ok := cfg.Sources["files"]
	if !ok {
		t.Fatal("missing 'files' source")
	}
	if files.Type != "dir" || files.Path != "./testdata" {
		t.Errorf("files source = %+v, want type=dir path=./testdata", files)
	}

	// spoof instances
	if cfg.Spoof.Postgres["main"].Port != 5433 ||
		cfg.Spoof.MySQL["main"].Port != 3306 ||
		cfg.Spoof.ClickHouse["main"].Port != 9000 {
		t.Errorf("spoof instances = %+v", cfg.Spoof)
	}

	// redis instances
	if len(cfg.Redis) != 2 || cfg.Redis["cache"].Port != 6379 || cfg.Redis["sessions"].Dump != "./testdata/sessions.rdb" {
		t.Errorf("redis = %+v", cfg.Redis)
	}
}

func TestResolvedDSN(t *testing.T) {
	// env var wins when set
	t.Setenv("TEST_DSN", "host=env")
	s := Source{DSN: "host=inline", DSNEnv: "TEST_DSN"}
	if got := s.ResolvedDSN(); got != "host=env" {
		t.Errorf("ResolvedDSN with env set = %q, want host=env", got)
	}
	// falls back to inline when env var is unset/empty
	s2 := Source{DSN: "host=inline", DSNEnv: "DEFINITELY_UNSET_VAR_XYZ"}
	if got := s2.ResolvedDSN(); got != "host=inline" {
		t.Errorf("ResolvedDSN fallback = %q, want host=inline", got)
	}
	// inline only
	s3 := Source{DSN: "host=only"}
	if got := s3.ResolvedDSN(); got != "host=only" {
		t.Errorf("ResolvedDSN inline-only = %q, want host=only", got)
	}
}

func TestShardedSpoof(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.huml")
	doc := "spoof::\n" +
		"  mysql::\n" +
		"    shard0::\n" +
		"      port: 3306\n" +
		"      sources::\n" +
		"        - \"files\"\n" +
		"    shard1::\n" +
		"      port: 3307\n" +
		"      sources::\n" +
		"        - \"files\"\n"
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Spoof.MySQL) != 2 {
		t.Fatalf("mysql instances = %d, want 2", len(cfg.Spoof.MySQL))
	}
	if cfg.Spoof.MySQL["shard0"].Port != 3306 || cfg.Spoof.MySQL["shard1"].Port != 3307 {
		t.Errorf("shard ports = %+v", cfg.Spoof.MySQL)
	}
	if got := cfg.Spoof.MySQL["shard0"].Sources; len(got) != 1 || got[0] != "files" {
		t.Errorf("shard0 sources = %v, want [files]", got)
	}
}

// TestTablesList confirms HUML parses an inline scalar list into []string.
func TestTablesList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.huml")
	doc := "sources::\n" +
		"  db::\n" +
		"    type: \"postgres\"\n" +
		"    tables:: \"public.a\", \"public.b\"\n"
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Sources["db"].Tables
	if len(got) != 2 || got[0] != "public.a" || got[1] != "public.b" {
		t.Errorf("tables = %v, want [public.a public.b]", got)
	}
}
