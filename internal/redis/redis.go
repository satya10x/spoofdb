// Package redis spoofs one or more Redis endpoints by running an embedded
// pure-Go Redis server (miniredis) per configured instance, each seeded from a
// real RDB dump file. Apps point their Redis clients at the bound ports; no real
// Redis process is involved.
package redis

import (
	"fmt"
	"log"

	"github.com/alicebob/miniredis/v2"

	"github.com/satya10x/spoofdb/internal/config"
)

// Instance is one running embedded Redis server.
type Instance struct {
	Name string
	mr   *miniredis.Miniredis
}

// Addr returns the host:port the instance is listening on.
func (i *Instance) Addr() string { return i.mr.Addr() }

// Close stops the instance.
func (i *Instance) Close() { i.mr.Close() }

// Start launches every configured Redis instance, each on its own port and
// seeded from its own RDB dump. It does not block. On any failure the
// already-started instances are closed before returning the error.
func Start(instances map[string]config.RedisInstance) ([]*Instance, error) {
	var started []*Instance
	for name, cfg := range instances {
		inst, err := startInstance(name, cfg)
		if err != nil {
			for _, s := range started {
				s.Close()
			}
			return nil, err
		}
		started = append(started, inst)
	}
	return started, nil
}

func startInstance(name string, cfg config.RedisInstance) (*Instance, error) {
	mr := miniredis.NewMiniRedis()
	addr := fmt.Sprintf(":%d", cfg.Port)
	if err := mr.StartAddr(addr); err != nil {
		return nil, fmt.Errorf("redis[%s]: start %s: %w", name, addr, err)
	}

	var stats seedStats
	if cfg.Dump != "" {
		s, err := seedFromRDB(mr, cfg.Dump)
		if err != nil {
			mr.Close()
			return nil, fmt.Errorf("redis[%s]: seed from %s: %w", name, cfg.Dump, err)
		}
		stats = s
	}

	log.Printf("redis[%s]: listening on %s (keys=%d dbs=%d dump=%s)",
		name, addr, stats.keys, stats.dbs, cfg.Dump)
	return &Instance{Name: name, mr: mr}, nil
}
