// Command spoofdb is a single binary that emulates Postgres, MySQL, ClickHouse,
// and Redis on top of an in-process DuckDB engine (SQL) and embedded miniredis
// servers (Redis). Each SQL protocol can run several instances ("shards"), each
// serving the sources it points at.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"github.com/satya10x/spoofdb/internal/clickhouse"
	"github.com/satya10x/spoofdb/internal/config"
	"github.com/satya10x/spoofdb/internal/engine"
	"github.com/satya10x/spoofdb/internal/mysql"
	"github.com/satya10x/spoofdb/internal/pg"
	"github.com/satya10x/spoofdb/internal/redis"
)

func main() {
	var sourceFlags, spoofFlags stringSlice
	flag.Var(&sourceFlags, "source", "data source: a dir, .csv/.parquet/.rdb file, or a DSN (scheme://...); repeatable or comma-separated")
	flag.Var(&spoofFlags, "spoof", "serve as protocol[:port], e.g. mysql:3306 or postgres; repeatable or comma-separated")
	cfgPath := flag.String("config", "config/spoofdb.huml", "path to HUML config file (used when -source/-spoof are absent)")
	stubMissing := flag.Bool("stub-missing-schema", false, "serve any query referencing an unknown table/column as an empty table instead of erroring (lets apps boot against a spoofdb with absent/partial schema)")
	persist := flag.Bool("persist", true, "persist runtime-created tables/rows to the -data file so they survive a restart; -persist=false for an in-memory database")
	dataFile := flag.String("data", "spoofdb.db", "DuckDB file to persist to (used only when -persist)")
	reset := flag.Bool("reset", false, "wipe persisted data at startup for a clean slate")
	flag.Parse()

	// -source/-spoof build an ad-hoc config; otherwise load the HUML file.
	var cfg *config.Config
	var err error
	if len(sourceFlags) > 0 || len(spoofFlags) > 0 {
		cfg, err = configFromFlags(sourceFlags, spoofFlags)
	} else {
		cfg, err = config.Load(*cfgPath)
	}
	if err != nil {
		log.Fatal(err)
	}
	spoof := cfg.Spoof
	sqlInstances := len(spoof.Postgres) + len(spoof.MySQL) + len(spoof.ClickHouse)

	// -reset wipes any persisted data so the next start is a clean slate.
	if *reset && *persist {
		for _, p := range []string{*dataFile, *dataFile + ".wal"} {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				log.Fatalf("spoofdb: reset: %v", err)
			}
		}
		log.Printf("spoofdb: reset persisted data %q", *dataFile)
	}
	if *persist {
		log.Printf("spoofdb: persisting to %q (use -persist=false for in-memory, -reset to wipe)", *dataFile)
	}

	// One DuckDB engine per distinct source-set, shared by instances that request
	// the same sources — so replica shards don't load the data twice. When
	// persisting, every engine is backed by the single -data file (go-duckdb
	// shares one in-process instance per path), so all data lives in one file.
	engines := map[string]*engine.Engine{}
	engineFor := func(srcNames []string) (*engine.Engine, error) {
		srcs, err := selectSources(cfg, srcNames)
		if err != nil {
			return nil, err
		}
		key := engineKey(srcs)
		if e, ok := engines[key]; ok {
			return e, nil
		}
		dbPath := ""
		if *persist {
			dbPath = *dataFile
		}
		e, err := engine.NewFromSourcesAt(dbPath, srcs)
		if err != nil {
			return nil, err
		}
		e.SetStubMissing(*stubMissing)
		log.Printf("engine[%s]: loaded %d table(s): %v", key, len(e.Tables()), e.Tables())
		engines[key] = e
		return e, nil
	}
	defer func() {
		for _, e := range engines {
			e.Close()
		}
	}()

	// Redis instances: one embedded miniredis per configured endpoint.
	instances, err := redis.Start(cfg.Redis)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		for _, inst := range instances {
			inst.Close()
		}
	}()

	if sqlInstances == 0 && len(instances) == 0 {
		log.Fatal("spoofdb: no listeners configured")
	}

	// Bind each listener up front so a port conflict fails fast and loudly
	// (consistent with redis.Start) instead of dying quietly in a goroutine. A
	// runtime Accept error stops only that listener.
	bind := func(name string, port int) net.Listener {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			log.Fatalf("spoofdb: %s: %v", name, err)
		}
		log.Printf("%s: listening on %s", name, ln.Addr())
		return ln
	}
	serve := func(name string, fn func() error) {
		go func() {
			if err := fn(); err != nil {
				log.Printf("%s: stopped: %v", name, err)
			}
		}()
	}

	// startShards binds and serves every instance of one SQL protocol.
	startShards := func(proto string, insts map[string]config.SQLInstance, serveFn func(net.Listener, *engine.Engine) error) {
		for _, name := range sortedKeys(insts) {
			inst := insts[name]
			if inst.Port == 0 {
				continue
			}
			eng, err := engineFor(inst.Sources)
			if err != nil {
				log.Fatalf("spoofdb: %s[%s]: %v", proto, name, err)
			}
			label := fmt.Sprintf("%s[%s]", proto, name)
			ln := bind(label, inst.Port)
			serve(label, func() error { return serveFn(ln, eng) })
		}
	}
	startShards("pg", spoof.Postgres, pg.Serve)
	startShards("mysql", spoof.MySQL, mysql.Serve)
	startShards("clickhouse", spoof.ClickHouse, clickhouse.Serve)

	log.Printf("spoofdb: running %d SQL shard(s) + %d redis instance(s)", sqlInstances, len(instances))

	// Block until interrupted (Ctrl-C / SIGTERM) so the listeners keep serving;
	// the deferred Close calls run on the way out.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	log.Println("spoofdb: shutting down")
}

// selectSources resolves source names to engine sources, ordered by name. An
// empty list means all configured sources. DSNs are resolved (env var wins).
func selectSources(cfg *config.Config, names []string) ([]engine.Source, error) {
	if len(names) == 0 {
		for name := range cfg.Sources {
			names = append(names, name)
		}
	} else {
		names = append([]string(nil), names...) // don't mutate the config slice
	}
	sort.Strings(names)

	out := make([]engine.Source, 0, len(names))
	for _, name := range names {
		s, ok := cfg.Sources[name]
		if !ok {
			return nil, fmt.Errorf("unknown source %q", name)
		}
		out = append(out, engine.Source{
			Name:   name,
			Type:   s.Type,
			Path:   s.Path,
			DSN:    s.ResolvedDSN(),
			Tables: s.Tables,
		})
	}
	return out, nil
}

// engineKey is a stable identifier for a source-set, so instances requesting the
// same sources share one engine.
func engineKey(srcs []engine.Source) string {
	names := make([]string, len(srcs))
	for i, s := range srcs {
		names[i] = s.Name
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "(none)"
	}
	return strings.Join(names, "\x00")
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
