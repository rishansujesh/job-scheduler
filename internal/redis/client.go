package redisx

import (
	"context"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr     string
	Password string
	DB       int
}

func FromEnv() Config {
	addr := getenv("REDIS_ADDR", "localhost:6379")
	pass := os.Getenv("REDIS_PASSWORD")
	return Config{Addr: addr, Password: pass, DB: 0}
}

func NewClientWithBackoff(ctx context.Context, cfg Config) (*redis.Client, error) {
	backoff := 200 * time.Millisecond
	max := 5 * time.Second

	for {
		rdb := redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < max {
				backoff *= 2
				if backoff > max {
					backoff = max
				}
			}
			continue
		}
		return rdb, nil
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
