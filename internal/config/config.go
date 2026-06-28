// Package config loads spoofdb's HUML configuration file.
package config

import (
	"fmt"
	"os"

	huml "github.com/huml-lang/go-huml"
)

// Config is the top-level spoofdb configuration. SQL data comes from one or more
// named sources (files or live databases), all loaded into DuckDB, and is served
// ("spoofed") as any subset of the SQL protocols. Redis is its own world: each
// instance is seeded from an RDB dump and served over RESP.
type Config struct {
	Sources map[string]Source        `huml:"sources"`
	Spoof   Spoof                    `huml:"spoof"`
	Redis   map[string]RedisInstance `huml:"redis"`
}

// Source describes where a set of SQL tables comes from. Type selects the
// loader: "dir" (a directory of parquet/csv), "parquet", "csv", "postgres",
// "mysql", or "clickhouse". Path is used by the file types; DSN/DSNEnv by the
// database types (DSNEnv, if set and present in the environment, wins over DSN
// so secrets need not live in the config). Tables optionally restricts which
// tables a database source loads (default: all).
type Source struct {
	Type   string   `huml:"type"`
	Path   string   `huml:"path"`
	DSN    string   `huml:"dsn"`
	DSNEnv string   `huml:"dsn_env"`
	Tables []string `huml:"tables"`
}

// Spoof holds the SQL listeners. Each protocol is a map of named instances, so
// a single binary can expose several endpoints ("shards") per protocol. The map
// key is a label; an empty map means the protocol is off.
type Spoof struct {
	Postgres   map[string]SQLInstance `huml:"postgres"`
	MySQL      map[string]SQLInstance `huml:"mysql"`
	ClickHouse map[string]SQLInstance `huml:"clickhouse"`
}

// SQLInstance is one SQL endpoint: a port plus the names of the sources whose
// data it serves. An empty Sources list means "all configured sources". Pointing
// several instances at the same source(s) replicates a dataset across shards;
// pointing them at different sources gives each shard its own data.
type SQLInstance struct {
	Port    int      `huml:"port"`
	Sources []string `huml:"sources"`
}

// RedisInstance is one emulated Redis endpoint: a port plus the RDB dump file
// whose keyspace (all logical DBs included) it serves. The map key is a label.
type RedisInstance struct {
	Port int    `huml:"port"`
	Dump string `huml:"dump"`
}

// ResolvedDSN returns the effective connection string for a database source:
// the value of the DSNEnv environment variable if set and non-empty, otherwise
// the inline DSN.
func (s Source) ResolvedDSN() string {
	if s.DSNEnv != "" {
		if v := os.Getenv(s.DSNEnv); v != "" {
			return v
		}
	}
	return s.DSN
}

// Load reads and parses the HUML config at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	if err := huml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return &cfg, nil
}
