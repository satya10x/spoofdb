//go:build ignore

// Generates the test RDB fixtures (cache.rdb, sessions.rdb) using the rdb
// encoder, so no real redis-server is needed. Run from the repo root:
//
//	go run testdata/gen_rdb.go
package main

import (
	"os"
	"time"

	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

func main() {
	writeCache("testdata/cache.rdb")
	writeSessions("testdata/sessions.rdb")
}

// writeCache writes a single-DB dump exercising every supported type plus a TTL.
func writeCache(path string) {
	f, err := os.Create(path)
	must(err)
	defer f.Close()

	enc := encoder.NewEncoder(f)
	must(enc.WriteHeader())
	must(enc.WriteAux("redis-ver", "7.0.0"))
	must(enc.WriteDBHeader(0, 7, 1)) // 7 keys, 1 with TTL

	must(enc.WriteStringObject("feature:dark_mode", []byte("true")))
	must(enc.WriteStringObject("counter:signups", []byte("4821")))
	ttlMs := uint64(time.Now().Add(8*time.Hour).UnixMilli())
	must(enc.WriteStringObject("greeting", []byte("hello"), encoder.WithTTL(ttlMs)))
	must(enc.WriteHashMapObject("config", map[string][]byte{
		"max_retries": []byte("3"),
		"timeout_ms":  []byte("5000"),
	}))
	must(enc.WriteListObject("queue", [][]byte{[]byte("job1"), []byte("job2"), []byte("job3")}))
	must(enc.WriteSetObject("tags", [][]byte{[]byte("go"), []byte("redis"), []byte("duckdb")}))
	must(enc.WriteZSetObject("leaderboard", []*model.ZSetEntry{
		{Member: "alice", Score: 10},
		{Member: "bob", Score: 7},
		{Member: "carol", Score: 15},
	}))

	must(enc.WriteEnd())
}

// writeSessions writes a two-DB dump: sessions in DB 0, locks in DB 1.
func writeSessions(path string) {
	f, err := os.Create(path)
	must(err)
	defer f.Close()

	enc := encoder.NewEncoder(f)
	must(enc.WriteHeader())
	must(enc.WriteAux("redis-ver", "7.0.0"))

	// DB 0: sessions
	must(enc.WriteDBHeader(0, 2, 0))
	must(enc.WriteStringObject("session:abc123", []byte(`{"user_id":1001,"role":"admin"}`)))
	must(enc.WriteHashMapObject("user:1001", map[string][]byte{
		"name": []byte("alice"),
		"role": []byte("admin"),
	}))

	// DB 1: locks
	must(enc.WriteDBHeader(1, 1, 0))
	must(enc.WriteStringObject("lock:order:42", []byte("held")))

	must(enc.WriteEnd())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
