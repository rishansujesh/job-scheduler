package redisx

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type LeaderElector struct {
	rdb       *redis.Client
	key       string
	ttl       time.Duration
	instance  string
	isLeader  atomic.Bool
	cancel    context.CancelFunc
}

func NewLeaderElector(rdb *redis.Client, key string, ttlSec int, instanceID string) *LeaderElector {
	if instanceID == "" {
		instanceID = hostname()
	}
	le := &LeaderElector{
		rdb:      rdb,
		key:      key,
		ttl:      time.Duration(ttlSec) * time.Second,
		instance: instanceID,
	}
	return le
}

func (l *LeaderElector) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	l.cancel = cancel

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Try to acquire if not leader
			if !l.isLeader.Load() {
				ok, err := l.rdb.SetNX(ctx, l.key, l.instance, l.ttl).Result()
				if err == nil && ok {
					l.isLeader.Store(true)
				}
			} else {
				// Renew TTL
				_ = l.rdb.PExpire(ctx, l.key, l.ttl).Err()
				// Double-check who owns it (optional)
				val, _ := l.rdb.Get(ctx, l.key).Result()
				if val != l.instance {
					l.isLeader.Store(false)
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (l *LeaderElector) Stop() { if l.cancel != nil { l.cancel() } }

func (l *LeaderElector) IsLeader() bool { return l.isLeader.Load() }

func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "instance"
}
