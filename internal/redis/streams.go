package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type StreamsConfig struct {
	Scheduled     string
	Adhoc         string
	Retry         string
	DLQ           string
	ConsumerGroup string
}

func StreamsFromEnv() StreamsConfig {
	return StreamsConfig{
		Scheduled:     getenv("REDIS_STREAM_SCHEDULED", "jobs:scheduled"),
		Adhoc:         getenv("REDIS_STREAM_ADHOC", "jobs:adhoc"),
		Retry:         getenv("REDIS_STREAM_RETRY", "jobs:retry"),
		DLQ:           getenv("REDIS_STREAM_DLQ", "jobs:dlq"),
		ConsumerGroup: getenv("REDIS_CONSUMER_GROUP", "cg:workers"),
	}
}

func EnsureGroup(ctx context.Context, rdb *redis.Client, stream, group string) error {
	err := rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	return nil
}

func isBusyGroup(err error) bool {
	if err == nil {
		return false
	}
	// v9 doesn't export ErrGroupExists; detect BUSYGROUP manually
	return strings.Contains(err.Error(), "BUSYGROUP")
}

func XAddJSON(ctx context.Context, rdb *redis.Client, stream string, v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		ID:     "*",
		Values: map[string]any{"data": string(b)},
	}).Result()
}

type ReadOptions struct {
	Streams       []string
	ConsumerGroup string
	ConsumerName  string
	Count         int64
	Block         time.Duration
}

type DecodedMessage struct {
	Stream  string
	ID      string
	Payload map[string]any
	Raw     redis.XMessage
}

func XReadGroupJSON(ctx context.Context, rdb *redis.Client, opt ReadOptions) ([]DecodedMessage, error) {
	if len(opt.Streams) == 0 || len(opt.Streams)%2 != 0 {
		return nil, fmt.Errorf("Streams must be pairs of stream and id ('>' or '0')")
	}
	res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    opt.ConsumerGroup,
		Consumer: opt.ConsumerName,
		Streams:  opt.Streams,
		Count:    opt.Count,
		Block:    opt.Block,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	var out []DecodedMessage
	for _, s := range res {
		for _, m := range s.Messages {
			var item map[string]any
			if raw, ok := m.Values["data"].(string); ok && raw != "" {
				_ = json.Unmarshal([]byte(raw), &item)
			}
			out = append(out, DecodedMessage{
				Stream:  s.Stream,
				ID:      m.ID,
				Payload: item,
				Raw:     m,
			})
		}
	}
	return out, nil
}

func Ack(ctx context.Context, rdb *redis.Client, stream, group string, ids ...string) (int64, error) {
	return rdb.XAck(ctx, stream, group, ids...).Result()
}

func ClaimPending(ctx context.Context, rdb *redis.Client, stream, group, consumer string, idleFor time.Duration, count int64) ([]redis.XMessage, error) {
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: count,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	var ids []string
	for _, p := range pending {
		if p.Idle >= idleFor {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  idleFor,
		Messages: ids,
	}).Result()
}
