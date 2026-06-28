package redis

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/hdt3213/rdb/model"
	"github.com/hdt3213/rdb/parser"
)

// seedStats summarizes what a seed loaded.
type seedStats struct {
	keys int
	dbs  int
}

// seedFromRDB parses an RDB dump and replays every key into mr, preserving the
// key's logical DB index, type, and TTL. Streams and other unsupported types are
// skipped with a notice. miniredis does not advance time on its own, so a key's
// TTL is reported as a fixed value (it does not count down) and the key never
// actually evicts — the snapshot stays intact for the life of the process.
func seedFromRDB(mr *miniredis.Miniredis, path string) (seedStats, error) {
	f, err := os.Open(path)
	if err != nil {
		return seedStats{}, err
	}
	defer f.Close()

	var stats seedStats
	seenDB := map[int]struct{}{}
	now := time.Now()

	err = parser.NewDecoder(f).Parse(func(o model.RedisObject) bool {
		key := o.GetKey()
		db := mr.DB(o.GetDBIndex())

		// wrote stays false for an empty collection, which creates no key in
		// miniredis — so it must not get a TTL or be counted.
		wrote := true
		switch obj := o.(type) {
		case *model.StringObject:
			if err := db.Set(key, string(obj.Value)); err != nil {
				log.Printf("redis: set %q: %v", key, err)
				return true
			}
		case *model.ListObject:
			vals := bytesToStrings(obj.Values)
			if len(vals) == 0 {
				wrote = false
			} else if _, err := db.Push(key, vals...); err != nil {
				log.Printf("redis: rpush %q: %v", key, err)
			}
		case *model.SetObject:
			mems := bytesToStrings(obj.Members)
			if len(mems) == 0 {
				wrote = false
			} else if _, err := db.SetAdd(key, mems...); err != nil {
				log.Printf("redis: sadd %q: %v", key, err)
			}
		case *model.HashObject:
			if len(obj.Hash) == 0 {
				wrote = false
				break
			}
			fv := make([]string, 0, len(obj.Hash)*2)
			for field, val := range obj.Hash {
				fv = append(fv, field, string(val))
			}
			db.HSet(key, fv...)
		case *model.ZSetObject:
			if len(obj.Entries) == 0 {
				wrote = false
			}
			for _, e := range obj.Entries {
				if _, err := db.ZAdd(key, e.Score, e.Member); err != nil {
					log.Printf("redis: zadd %q: %v", key, err)
				}
			}
		default:
			// Aux/dbsize/functions metadata carry no keyspace data; only warn
			// for actual data types we don't handle yet (streams).
			if o.GetType() == model.StreamType {
				log.Printf("redis: skipping key %q (streams not yet supported)", key)
			}
			return true
		}
		if !wrote {
			return true
		}

		if exp := o.GetExpiration(); exp != nil && exp.After(now) {
			db.SetTTL(key, exp.Sub(now))
		}
		if _, ok := seenDB[o.GetDBIndex()]; !ok {
			seenDB[o.GetDBIndex()] = struct{}{}
			stats.dbs++
		}
		stats.keys++
		return true
	})
	if err != nil {
		return seedStats{}, fmt.Errorf("parse rdb: %w", err)
	}
	return stats, nil
}

func bytesToStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i, b := range in {
		out[i] = string(b)
	}
	return out
}
