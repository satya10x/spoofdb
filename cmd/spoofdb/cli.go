package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/satya10x/spoofdb/internal/config"
)

// stringSlice is a repeatable, comma-splitting flag value.
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }

func (s *stringSlice) Set(v string) error {
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			*s = append(*s, p)
		}
	}
	return nil
}

// configFromFlags builds an ad-hoc config from -source and -spoof flags, so data
// can be served without writing a config file. Every SQL spoof target serves all
// the SQL sources; a redis spoof target is paired with an .rdb source.
func configFromFlags(sourceSpecs, spoofSpecs []string) (*config.Config, error) {
	if len(sourceSpecs) == 0 || len(spoofSpecs) == 0 {
		return nil, fmt.Errorf("-source and -spoof must both be set (or use -config)")
	}

	cfg := &config.Config{
		Sources: map[string]config.Source{},
		Spoof: config.Spoof{
			Postgres:   map[string]config.SQLInstance{},
			MySQL:      map[string]config.SQLInstance{},
			ClickHouse: map[string]config.SQLInstance{},
		},
		Redis: map[string]config.RedisInstance{},
	}

	var redisDumps []string
	sqlCount := 0
	for _, spec := range sourceSpecs {
		src, isRedis, err := parseSource(spec)
		if err != nil {
			return nil, err
		}
		if isRedis {
			redisDumps = append(redisDumps, spec)
			continue
		}
		cfg.Sources[fmt.Sprintf("src%d", sqlCount)] = src
		sqlCount++
	}

	perProto := map[string]int{}
	for _, spec := range spoofSpecs {
		proto, port, err := parseSpoof(spec)
		if err != nil {
			return nil, err
		}
		idx := perProto[proto]
		perProto[proto]++
		name := fmt.Sprintf("shard%d", idx)

		switch proto {
		case "postgres":
			cfg.Spoof.Postgres[name] = config.SQLInstance{Port: port}
		case "mysql":
			cfg.Spoof.MySQL[name] = config.SQLInstance{Port: port}
		case "clickhouse":
			cfg.Spoof.ClickHouse[name] = config.SQLInstance{Port: port}
		case "redis":
			if idx >= len(redisDumps) {
				return nil, fmt.Errorf("redis spoof %q has no matching .rdb source", spec)
			}
			cfg.Redis[name] = config.RedisInstance{Port: port, Dump: redisDumps[idx]}
		}
	}

	if sqlCount == 0 && len(cfg.Redis) == 0 {
		return nil, fmt.Errorf("no usable sources for the given spoof targets")
	}
	return cfg, nil
}

// parseSource infers a source from a CLI spec: a scheme:// URL is a database
// DSN; otherwise it is a path whose type comes from the extension (directory ->
// dir, .csv, .parquet, .rdb). The .rdb case returns isRedis=true.
func parseSource(spec string) (src config.Source, isRedis bool, err error) {
	if i := strings.Index(spec, "://"); i > 0 {
		switch scheme := spec[:i]; scheme {
		case "postgres", "postgresql":
			return config.Source{Type: "postgres", DSN: spec}, false, nil
		case "mysql":
			return config.Source{Type: "mysql", DSN: spec}, false, nil
		case "clickhouse":
			return config.Source{Type: "clickhouse", DSN: spec}, false, nil
		default:
			return config.Source{}, false, fmt.Errorf("source %q: unsupported scheme %q", spec, scheme)
		}
	}

	fi, err := os.Stat(spec)
	if err != nil {
		return config.Source{}, false, fmt.Errorf("source %q: %w", spec, err)
	}
	if fi.IsDir() {
		return config.Source{Type: "dir", Path: spec}, false, nil
	}
	switch strings.ToLower(filepath.Ext(spec)) {
	case ".parquet":
		return config.Source{Type: "parquet", Path: spec}, false, nil
	case ".csv":
		return config.Source{Type: "csv", Path: spec}, false, nil
	case ".rdb":
		return config.Source{}, true, nil
	default:
		return config.Source{}, false, fmt.Errorf("source %q: unknown file type (want a dir, .csv, .parquet, .rdb, or a DSN)", spec)
	}
}

// parseSpoof parses "proto[:port]" into a normalized protocol and port (default
// port when omitted).
func parseSpoof(spec string) (proto string, port int, err error) {
	name, portStr, hasPort := strings.Cut(spec, ":")
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "postgres", "postgresql", "pg":
		proto = "postgres"
	case "mysql", "my":
		proto = "mysql"
	case "clickhouse", "ch":
		proto = "clickhouse"
	case "redis":
		proto = "redis"
	default:
		return "", 0, fmt.Errorf("spoof %q: unknown protocol %q", spec, name)
	}
	if !hasPort {
		return proto, defaultPort(proto), nil
	}
	port, err = strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil {
		return "", 0, fmt.Errorf("spoof %q: bad port: %w", spec, err)
	}
	return proto, port, nil
}

func defaultPort(proto string) int {
	switch proto {
	case "postgres":
		return 5432
	case "mysql":
		return 3306
	case "clickhouse":
		return 9000
	case "redis":
		return 6379
	}
	return 0
}
