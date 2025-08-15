// path: services/admin/main.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	redisx "job-scheduler/internal/redis"
)

type CmdFunc func(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error

func main() {
	log.SetFlags(0)

	// Global config from env
	group := getenv("REDIS_CONSUMER_GROUP", "cg:workers")
	consumer := hostname()

	// Subcommands
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	ctx := context.Background()
	rdb, err := redisx.NewClientWithBackoff(ctx, redisx.FromEnv())
	if err != nil {
		log.Fatalf("redis connect failed: %v", err)
	}
	defer rdb.Close()
	sc := redisx.StreamsFromEnv()

	handlers := map[string]CmdFunc{
		"lag":         cmdLag,
		"pending":     cmdPending,
		"claim-stuck": cmdClaimStuck,
		"requeue-dlq": cmdRequeueDLQ,
		"help": func(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error {
			usage()
			return nil
		},
	}
	fn, ok := handlers[cmd]
	if !ok {
		usage()
		os.Exit(2)
	}
	if err := fn(ctx, rdb, sc, group, consumer, args); err != nil {
		log.Fatalf("error: %v", err)
	}
}

func usage() {
	// Use Print (not Println) to avoid a redundant newline vet warning.
	fmt.Print(`admin cli

Usage:
  admin <command> [flags]

Commands:
  lag                         Show per-stream length, group pending, and per-consumer info
  pending     [--stream S]    List pending entries summary/details
  claim-stuck [--stream S] [--idle-ms 60000] [--count 100]
                              Claim messages idle longer than threshold to this consumer
  requeue-dlq [--count N] [--to-stream jobs:adhoc]
                              Requeue N items from DLQ to another stream

Environment (with defaults):
  REDIS_ADDR                  (redis:6379)
  REDIS_PASSWORD              ()
  REDIS_CONSUMER_GROUP        (cg:workers)
  REDIS_STREAM_SCHEDULED      (jobs:scheduled)
  REDIS_STREAM_ADHOC          (jobs:adhoc)
  REDIS_STREAM_RETRY          (jobs:retry)
  REDIS_STREAM_DLQ            (jobs:dlq)
`)
}

/* -------------------- commands -------------------- */

func cmdLag(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error {
	streams := []string{sc.Scheduled, sc.Adhoc, sc.Retry}
	for _, s := range streams {
		fmt.Printf("== %s ==\n", s)
		// XINFO STREAM LENGTH
		info, err := rdb.XInfoStream(ctx, s).Result()
		if err != nil {
			fmt.Printf("  (error: %v)\n", err)
			continue
		}
		fmt.Printf("  length: %d\n", info.Length)

		// Group level
		groups, _ := rdb.XInfoGroups(ctx, s).Result()
		for _, g := range groups {
			fmt.Printf("  group: %s  consumers=%d  pending=%d\n", g.Name, g.Consumers, g.Pending)
			cons, _ := rdb.XInfoConsumers(ctx, s, g.Name).Result()
			for _, c := range cons {
				fmt.Printf("    - consumer=%s  pending=%d  idle(ms)=%d\n", c.Name, c.Pending, c.Idle)
			}
		}
	}
	return nil
}

func cmdPending(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error {
	fs := flag.NewFlagSet("pending", flag.ContinueOnError)
	stream := fs.String("stream", sc.Scheduled, "stream to inspect")
	limit := fs.Int("count", 50, "max items to list")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Summary
	summary, err := rdb.XPending(ctx, *stream, group).Result()
	if err != nil {
		return err
	}
	fmt.Printf("pending summary: count=%d, min=%s, max=%s, consumers=%d\n", summary.Count, summary.Lower, summary.Higher, len(summary.Consumers))

	// Details
	ext, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: *stream, Group: group, Start: "-", End: "+", Count: int64(*limit),
	}).Result()
	if err != nil {
		return err
	}
	for _, p := range ext {
		fmt.Printf("  id=%s consumer=%s idle(ms)=%d deliveries=%d\n", p.ID, p.Consumer, p.Idle, p.RetryCount)
	}
	return nil
}

func cmdClaimStuck(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error {
	fs := flag.NewFlagSet("claim-stuck", flag.ContinueOnError)
	stream := fs.String("stream", sc.Scheduled, "stream name")
	idle := fs.Int("idle-ms", 60000, "min idle ms")
	count := fs.Int("count", 100, "max to claim")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Fetch pending candidates
	pends, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: *stream, Group: group, Start: "-", End: "+", Count: int64(*count),
	}).Result()
	if err != nil {
		return err
	}
	var ids []string
	for _, p := range pends {
		if int(p.Idle) >= *idle {
			ids = append(ids, p.ID)
		}
	}
	if len(ids) == 0 {
		fmt.Println("no messages over idle threshold")
		return nil
	}

	// Claim to our consumer
	claimed, err := rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   *stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  time.Duration(*idle) * time.Millisecond,
		Messages: ids,
	}).Result()
	if err != nil {
		return err
	}
	fmt.Printf("claimed %d messages to consumer=%s\n", len(claimed), consumer)
	return nil
}

func cmdRequeueDLQ(ctx context.Context, rdb *redis.Client, sc redisx.StreamsConfig, group, consumer string, args []string) error {
	fs := flag.NewFlagSet("requeue-dlq", flag.ContinueOnError)
	to := fs.String("to-stream", sc.Adhoc, "target stream to requeue into")
	count := fs.Int("count", 50, "max items to requeue")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Read oldest N from DLQ
	res, err := rdb.XRangeN(ctx, sc.DLQ, "-", "+", int64(*count)).Result()
	if err != nil {
		return err
	}
	if len(res) == 0 {
		fmt.Println("DLQ is empty")
		return nil
	}
	ok := 0
	for _, m := range res {
		// Expect a "data" JSON value (what we put via XAddJSON)
		raw, _ := getString(m.Values["data"])
		if raw == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		// Reset attempt & backoff markers
		delete(payload, "backoff_ms")
		delete(payload, "available_at_ms")
		payload["attempt"] = 0

		if _, err := redisx.XAddJSON(ctx, rdb, *to, payload); err == nil {
			ok++
		}
	}
	fmt.Printf("requeued %d/%d messages from %s -> %s\n", ok, len(res), sc.DLQ, *to)
	return nil
}

/* -------------------- helpers -------------------- */

func getString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func hostname() string {
	h, _ := os.Hostname()
	if strings.TrimSpace(h) == "" {
		return "admin"
	}
	return h
}
