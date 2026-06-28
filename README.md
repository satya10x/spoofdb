# spoofdb

**A single-binary, drop-in replacement for Postgres, MySQL, ClickHouse, and Redis — all at once.**

spoofdb speaks each database's *real wire protocol* (across as many shards as you like), so any standard client, ORM, or driver connects to it exactly as it would to the real thing — only the host/port changes. SQL is powered by one in-process [DuckDB](https://duckdb.org) engine; Redis by an embedded pure-Go server. It's built for local dev, tests, CI, and demos — anywhere you'd otherwise run a stack of real databases.

---

## Build

Requires **Go 1.26+** with CGO enabled (the DuckDB engine is a CGO library; prebuilt libs ship with the driver, so there's no separate DuckDB install).

```bash
git clone https://github.com/satya10x/spoofdb.git
cd spoofdb
CGO_ENABLED=1 go build -o spoofdb ./cmd/spoofdb
```

## Get started

```bash
./spoofdb -config config/spoofdb.huml
```

You'll see it persist to disk and bind each listener:

```
spoofdb: persisting to "spoofdb.db" (use -persist=false for in-memory, -reset to wipe)
engine[files]: loaded 3 table(s): [events orders users]
redis[cache]: listening on :6379 (keys=7 dbs=1 ...)
pg[main]: listening on [::]:5433
mysql[main]: listening on [::]:3306
clickhouse[main]: listening on [::]:9000
spoofdb: running 3 SQL shard(s) + 1 redis instance(s)
```

Point any client at it — same data, four protocols:

```bash
psql              -h localhost -p 5433 -U any -d any -c "SELECT * FROM users LIMIT 3"
mysql             -h 127.0.0.1 -P 3306 -u any           -e "SELECT * FROM orders LIMIT 3"
clickhouse-client --host localhost --port 9000 --compression 0 -q "SELECT count(*) FROM events"
redis-cli         -p 6379 HGETALL config
```

Any username connects; the SQL listeners take no password (MySQL expects an empty password).

### Without a config file

Skip the config and pass `-source`/`-spoof` (both repeatable / comma-separated):

```bash
spoofdb -source ./testdata              -spoof mysql:3306              # a dir, served as MySQL
spoofdb -source data.parquet            -spoof clickhouse:9000 -spoof postgres:5433
spoofdb -source postgres://u:p@host/db  -spoof mysql:3306              # live Postgres, served as MySQL
spoofdb -source dump.rdb                -spoof redis:6379             # an RDB dump, served as Redis
spoofdb -source ./testdata              -spoof mysql:3306,mysql:3307  # two shards, same data
```

The source type is inferred (a directory → `dir`; `.csv`/`.parquet`/`.rdb` by extension; `scheme://…` → that database). Spoof targets are `proto[:port]` with default ports when omitted. When given, `-source`/`-spoof` take over from `-config`.

---

## Working with data

There are two ways data gets into spoofdb: **load it from a source** (config, below) or **create it live over any SQL connection**. Because every SQL listener is backed by the same DuckDB engine, the two are interchangeable and everything is queryable over Postgres, MySQL, and ClickHouse alike.

### Creating tables, schemas, and rows at runtime

spoofdb runs real SQL — connect with any client and issue DDL/DML. It's DuckDB underneath, so you get DuckDB's [SQL dialect](https://duckdb.org/docs/sql/introduction) (`CREATE TABLE`, `CREATE SCHEMA`, `CREATE VIEW`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE AS …`, `COPY`, window functions, CTEs, etc.).

```bash
psql -h localhost -p 5433 -U any -d any <<'SQL'
CREATE SCHEMA app;
CREATE TABLE app.users (id INTEGER, name TEXT, created_at TIMESTAMP);
INSERT INTO app.users VALUES (1, 'alice', now()), (2, 'bob', now());

-- derive another table from a query
CREATE TABLE app.recent AS
  SELECT * FROM app.users WHERE created_at > now() - INTERVAL 7 DAY;

-- a view works too
CREATE VIEW app.user_names AS SELECT id, name FROM app.users;
SELECT * FROM app.user_names ORDER BY id;
SQL
```

### One engine, every protocol

A table created over one protocol is immediately visible over the others (for instances that serve the same sources — see [Sharding](#sharding)). Create it over Postgres, read it over MySQL:

```bash
psql  -h localhost -p 5433 -U any -d any -c "CREATE TABLE widgets(id INT, sku TEXT); INSERT INTO widgets VALUES (1,'abc');"
mysql -h 127.0.0.1 -P 3306 -u any        -e "SELECT * FROM widgets;"     # -> 1, abc
```

There are no per-protocol catalogs to keep in sync — the listeners are wire-protocol translators in front of one shared DuckDB. (Redis is the exception: it's a separate embedded server, not part of the SQL engine.)

### Bulk-loading data

Pull files straight into a table with DuckDB's readers, or `COPY`:

```sql
CREATE TABLE trades AS SELECT * FROM read_parquet('/data/trades.parquet');
INSERT INTO trades SELECT * FROM read_csv_auto('/data/more_trades.csv');
COPY trades TO '/tmp/trades_export.csv' (HEADER);
```

You can also declare files/dirs as sources in the config so they're registered as tables automatically at startup (see [Configuration](#configuration)).

### Persistence

By default spoofdb **persists to disk**, so everything you create at runtime survives a restart instead of having to be recreated:

```bash
spoofdb                          # persists to ./spoofdb.db (default)
spoofdb -data /var/lib/spoof.db  # choose the file
spoofdb -persist=false           # in-memory only; nothing is written
spoofdb -reset                   # wipe the file and start fresh
```

Everything lives in the single `-data` file. File sources (csv/parquet dirs) are re-registered as views on each start, so the file mainly carries the tables and rows you created at runtime. Live-database snapshot sources are re-pulled at startup (they need the source reachable). Redis data lives in an in-memory miniredis seeded from its RDB dump and is **not** persisted — it resets on restart.

### Booting an app against an empty/partial schema

```bash
spoofdb -config config/spoofdb.huml -stub-missing-schema
```

With `-stub-missing-schema`, a query that references a **missing table** — or uses a dialect feature DuckDB can't execute (a writable CTE, `ON CONFLICT ON CONSTRAINT`, an unknown session `SET`) — is served as an **empty table** instead of erroring. That lets an app connect, prepare *all* its queries, and boot against a spoofdb with little or no schema loaded.

- Each gap is logged once, so the data you'd need to seed is right there: a missing relation names itself (`relation "x" not found`); a dialect gap is a `WARNING fidelity gap`.
- It does **not** swallow *app* errors — a typo'd column or a syntax error still surfaces (in both modes), so real query bugs aren't masked.
- Even without the flag, a missing relation is logged before its error is returned.

> Postgres clients validate at **prepare** time (`lib/pq` sends Parse + Describe, which runs the query), so they hit this the moment they prepare. MySQL/ClickHouse only validate at **execute** time.

---

## Configuration

spoofdb is configured with a [HUML](https://huml.io) file (`-config`, default `config/spoofdb.huml`). Three sections: **`sources`** (where SQL data comes from), **`spoof`** (which SQL protocols expose it), and **`redis`** (independent RESP instances).

```huml
sources::
  files::
    type: "dir"                 # a directory of parquet/csv files
    path: "./testdata"
  orders::
    type: "postgres"            # a live DB, snapshotted into DuckDB at startup
    dsn_env: "ORDERS_PG_DSN"    # read the DSN from this env var (preferred)
    # dsn: "host=... dbname=... user=... password=..."   # or inline
    tables:: "public.orders", "public.users"   # optional; default: all tables

spoof::                         # each protocol is a map of named instances
  postgres::
    main::
      port: 5433
  mysql::
    main::
      port: 3306
  clickhouse::
    main::
      port: 9000

redis::                         # each is an embedded server seeded from a dump
  cache::
    port: 6379
    dump: "./testdata/cache.rdb"
  sessions::
    port: 6380
    dump: "./testdata/sessions.rdb"
```

**Source types:** `dir`, `parquet`, `csv`, `postgres`, `mysql`, `clickhouse`.
**Only what you configure runs.** A config with just a `postgres::` instance runs only Postgres; a config with only `redis::` doesn't even start the DuckDB engine.

### Sharding

Each SQL protocol is a map of named instances, so a "shard" is just another instance. Point several at the **same** sources to replicate a dataset across shards, or at **different** sources to give each shard its own data — spoofdb doesn't partition data itself, you express the topology in config. Instances requesting the same sources share one engine, so replicas cost no extra memory.

```huml
spoof::
  mysql::
    shard0::
      port: 3306
      sources::
        - "files"      # single-element source lists use the dash form
    shard1::
      port: 3307
      sources::
        - "files"      # same data as shard0 (shared engine)
    shard2::
      port: 3308
      sources::
        - "orders_b"   # its own dataset
```

An instance with no `sources` serves all configured sources.

### Credentials

Database sources take a connection string via `dsn` (inline) or `dsn_env` (an environment variable name). If both are set, **`dsn_env` wins**, so secrets needn't live in the config file. spoofdb never logs the DSN.

### Spoofing several Redis instances or DBs

A Redis client picks its server purely by host:port, so each real Redis endpoint maps to its own `redis::` entry on its own port. Multiple *logical* DBs (`SELECT n`) inside one instance come along automatically — the RDB dump carries every DB.

### Snapshotting real databases

To serve a real database offline, point a source at it — it's copied into DuckDB at startup, then disconnected:

```huml
sources::
  prod::
    type: "postgres"
    dsn_env: "PROD_DSN"
```

For Redis, capture an RDB on the source (`redis-cli --rdb dump.rdb`) and point a `redis::` instance at the file.

---

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `-config` | `config/spoofdb.huml` | HUML config file (used when `-source`/`-spoof` are absent) |
| `-source` | — | Ad-hoc data source: a dir, `.csv`/`.parquet`/`.rdb`, or a `scheme://` DSN (repeatable) |
| `-spoof` | — | Ad-hoc listener: `proto[:port]`, e.g. `mysql:3306` (repeatable) |
| `-persist` | `true` | Persist runtime-created tables/rows to the `-data` file; `=false` for in-memory |
| `-data` | `spoofdb.db` | DuckDB file to persist to |
| `-reset` | `false` | Wipe persisted data at startup |
| `-stub-missing-schema` | `false` | Serve unknown tables / unsupported dialect features as empty tables so an app can boot |

---

## How it works

Every SQL-shaped source is loaded into a single in-process **DuckDB** instance, and the Postgres/MySQL/ClickHouse listeners all serve that same DuckDB — so source and protocol are fully decoupled, and runtime DDL just lands in the same engine. Redis is its own world: an [RDB](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/) dump is replayed into an embedded [miniredis](https://github.com/alicebob/miniredis) server.

```
 Sources (where data comes from)         Spoof (how it's served)

  parquet ┐                            ┌─ :5432  Postgres   (pgproto3)
  csv     ┤                            │
  postgres┤──►  DuckDB (in-process) ──►┼─ :3306  MySQL      (go-mysql)
  mysql   ┤      one shared engine     │
  clickhouse┘                          └─ :9000  ClickHouse (native TCP)

  redis RDB dump ──► miniredis ─────────► :6379  Redis      (RESP)
```

### Source × spoof matrix

| Source ↓ \ Spoof → | Postgres | MySQL | ClickHouse | Redis |
|---|:--:|:--:|:--:|:--:|
| parquet file        | ✅ | ✅ | ✅ | — |
| csv file            | ✅ | ✅ | ✅ | — |
| Postgres (live)     | ✅ | ✅ | ✅ | — |
| MySQL (live)        | ✅ | ✅ | ✅ | — |
| ClickHouse (live)   | ✅ | ✅ | ✅ | — |
| Redis RDB dump      | — | — | — | ✅ |

Live database sources are **snapshotted** at startup (copied into DuckDB, then the source is disconnected) — Postgres/MySQL via DuckDB's native scanner extensions, ClickHouse via a `clickhouse-go` pull.

---

## Data fidelity

A fake is never bit-exact, so spoofdb **surfaces every approximation** instead of silently corrupting results — both in its own logs and over each protocol's native warning channel:

- **Postgres** → `NoticeResponse` (e.g. *"column \"amount\": DECIMAL shown as float8"*)
- **MySQL** → a warning count plus `SHOW WARNINGS`
- **ClickHouse** → server log packets (visible via clickhouse-go's `WithLogs`)

Postgres and MySQL preserve `NULL`s and exact big-integer/decimal values (as text); ClickHouse columns are not Nullable, so there `NULL` becomes a zero value — and that loss is reported.

---

## Limitations

- **ClickHouse clients must disable compression** (`--compression 0` / `Compression: None`).
- ClickHouse serving has no `Nullable`/`Array`/`Tuple`/`Map` columns; `NULL` → zero, `DateTime` is limited to 1970–2106, and large/decimal values lose precision (all reported via fidelity warnings).
- SQL is the DuckDB dialect, so a few Postgres/MySQL-only constructs aren't supported (e.g. writable CTEs, `ON CONFLICT ON CONSTRAINT`); `-stub-missing-schema` serves these as empty so an app can still boot.
- Redis data (miniredis) is in-memory and resets on restart; only the SQL engine persists (`-data`).
- Sharding is topology-only: spoofdb runs an instance per shard but does not partition data itself — each shard serves whatever sources you point it at.
- ClickHouse *as a source* is new and validated by unit tests only; live ingestion hasn't been exercised end-to-end.
- Redis Streams are skipped on load (with a notice).

---

## Built with

[DuckDB](https://duckdb.org) (via [go-duckdb](https://github.com/marcboeker/go-duckdb)) · [miniredis](https://github.com/alicebob/miniredis) · [pgproto3](https://github.com/jackc/pgx) · [go-mysql](https://github.com/go-mysql-org/go-mysql) · [ch-go](https://github.com/ClickHouse/ch-go) · [hdt3213/rdb](https://github.com/hdt3213/rdb) · [go-huml](https://github.com/huml-lang/go-huml)
