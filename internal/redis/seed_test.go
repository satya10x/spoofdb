package redis

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
)

// buildRDB writes a small multi-DB dump covering every supported type + a TTL.
func buildRDB(t *testing.T, path string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	enc := encoder.NewEncoder(f)
	must(t, enc.WriteHeader())
	must(t, enc.WriteAux("redis-ver", "7.0.0"))

	must(t, enc.WriteDBHeader(0, 6, 1))
	must(t, enc.WriteStringObject("greeting", []byte("hi")))
	ttlMs := uint64(time.Now().Add(time.Hour).UnixMilli())
	must(t, enc.WriteStringObject("temp", []byte("soon"), encoder.WithTTL(ttlMs)))
	must(t, enc.WriteHashMapObject("h", map[string][]byte{"f1": []byte("v1"), "f2": []byte("v2")}))
	must(t, enc.WriteListObject("l", [][]byte{[]byte("a"), []byte("b"), []byte("c")}))
	must(t, enc.WriteSetObject("s", [][]byte{[]byte("x"), []byte("y")}))
	must(t, enc.WriteZSetObject("z", []*model.ZSetEntry{{Member: "m1", Score: 1.5}, {Member: "m2", Score: 9}}))

	must(t, enc.WriteDBHeader(1, 1, 0))
	must(t, enc.WriteStringObject("lock", []byte("held")))

	must(t, enc.WriteEnd())
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeedFromRDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.rdb")
	buildRDB(t, path)

	mr := miniredis.NewMiniRedis()
	if err := mr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)

	stats, err := seedFromRDB(mr, path)
	if err != nil {
		t.Fatalf("seedFromRDB: %v", err)
	}
	if stats.keys != 7 {
		t.Errorf("keys = %d, want 7", stats.keys)
	}
	if stats.dbs != 2 {
		t.Errorf("dbs = %d, want 2", stats.dbs)
	}

	db0 := mr.DB(0)

	// string
	if got, _ := db0.Get("greeting"); got != "hi" {
		t.Errorf("greeting = %q, want hi", got)
	}
	// hash
	if got := db0.HGet("h", "f1"); got != "v1" {
		t.Errorf("h.f1 = %q, want v1", got)
	}
	// list (order preserved)
	if got, _ := db0.List("l"); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("list l = %v, want [a b c]", got)
	}
	// set
	if got, _ := db0.Members("s"); len(got) != 2 {
		t.Errorf("set s = %v, want 2 members", got)
	}
	// zset (SortedSet returns a member->score map)
	if got, _ := db0.SortedSet("z"); got["m1"] != 1.5 || got["m2"] != 9 {
		t.Errorf("zset z = %v, want m1=1.5 m2=9", got)
	}
	// TTL was applied and is positive (and the key still exists — no eviction)
	if ttl := db0.TTL("temp"); ttl <= 0 {
		t.Errorf("temp TTL = %v, want > 0", ttl)
	}
	if got, _ := db0.Get("temp"); got != "soon" {
		t.Errorf("temp = %q, want soon (must not have evicted)", got)
	}

	// multi-DB: the lock lives in DB 1, not DB 0
	if got, _ := mr.DB(1).Get("lock"); got != "held" {
		t.Errorf("db1 lock = %q, want held", got)
	}
	if got, _ := db0.Get("lock"); got != "" {
		t.Errorf("db0 lock = %q, want empty (isolated per DB)", got)
	}
}
