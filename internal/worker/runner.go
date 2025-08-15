package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"job-scheduler/internal/jobs"
	redisx "job-scheduler/internal/redis"
	"job-scheduler/internal/worker/handlers"
)

type Runner struct {
	DB            *sql.DB
	Store         *jobs.Store
	RDB           *redis.Client
	Streams       redisx.StreamsConfig
	Group         string
	ConsumerName  string
	MaxAttempts   int
	Logger        *log.Logger
}

func (r *Runner) Start(ctx context.Context) {
	// Ensure consumer groups exist
	_ = redisx.EnsureGroup(ctx, r.RDB, r.Streams.Scheduled, r.Group)
	_ = redisx.EnsureGroup(ctx, r.RDB, r.Streams.Adhoc, r.Group)
	_ = redisx.EnsureGroup(ctx, r.RDB, r.Streams.Retry, r.Group)

	go r.consume(ctx, r.Streams.Scheduled)
	go r.consume(ctx, r.Streams.Adhoc)
	go r.consume(ctx, r.Streams.Retry)
}

func (r *Runner) consume(ctx context.Context, stream string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := redisx.XReadGroupJSON(ctx, r.RDB, redisx.ReadOptions{
			Streams:       []string{stream, ">"},
			ConsumerGroup: r.Group,
			ConsumerName:  r.ConsumerName,
			Count:         16,
			Block:         5 * time.Second,
		})
		if err != nil && err != redis.Nil {
			r.Logger.Printf("read error stream=%s: %v", stream, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		for _, m := range msgs {
			if err := r.processMessage(ctx, stream, m); err != nil {
				r.Logger.Printf("process error stream=%s id=%s: %v", stream, m.ID, err)
			}
		}
	}
}

func (r *Runner) processMessage(ctx context.Context, stream string, m redisx.DecodedMessage) error {
	// Retry deferral logic for jobs:retry
	if stream == r.Streams.Retry {
		if due, ok := m.Payload["available_at_ms"].(float64); ok {
			nowms := float64(time.Now().UnixMilli())
			if nowms < due {
				// Not yet due: re-enqueue and ack this message.
				_, _ = redisx.XAddJSON(ctx, r.RDB, r.Streams.Retry, m.Payload)
				_, _ = redisx.Ack(ctx, r.RDB, stream, r.Group, m.ID)
				return nil
			}
		}
	}

	runID, _ := str(m.Payload["run_id"])
	jobID, _ := str(m.Payload["job_id"])
	handlerName, _ := str(m.Payload["handler"])

	// Ensure run row exists; if not, insert queued row using idempotency_key=run_id
	if runID == "" || jobID == "" || handlerName == "" {
		_, _ = redisx.Ack(ctx, r.RDB, stream, r.Group, m.ID)
		return fmt.Errorf("invalid message: missing fields")
	}
	_, err := r.Store.UpdateRunStatus(ctx, jobs.UpdateRunStatusParams{
		RunID:  runID,
		Status: jobs.StatusRunning,
	})
	if err == jobs.ErrNotFound {
		// insert then set running
		if _, err2 := r.Store.InsertRun(ctx, jobs.InsertRunParams{
			JobID:          jobID,
			RunID:          runID,
			Status:         jobs.StatusQueued,
			IdempotencyKey: runID,
		}); err2 != nil {
			return err2
		}
		if _, err3 := r.Store.UpdateRunStatus(ctx, jobs.UpdateRunStatusParams{
			RunID:  runID,
			Status: jobs.StatusRunning,
		}); err3 != nil {
			return err3
		}
	} else if err != nil {
		return err
	}

	// Execute handler
	var execErr error
	var retryable bool
	switch handlerName {
	case "shell":
		var a handlers.ShellArgs
		bytesArg, _ := json.Marshal(m.Payload["args"])
		_ = json.Unmarshal(bytesArg, &a)
		_, execErr = handlers.RunShell(ctx, a)
	case "http":
		var a handlers.HTTPArgs
		bytesArg, _ := json.Marshal(m.Payload["args"])
		_ = json.Unmarshal(bytesArg, &a)
		res, err := handlers.RunHTTP(ctx, a)
		retryable = res.Retryable
		execErr = err
	default:
		execErr = fmt.Errorf("unknown handler: %s", handlerName)
	}

	// Update DB and ack / retry / dlq
	if execErr == nil {
		now := timePtr(time.Now().UTC())
		_, _ = r.Store.UpdateRunStatus(ctx, jobs.UpdateRunStatusParams{
			RunID:      runID,
			Status:     jobs.StatusSuccess,
			FinishedAt: now,
		})
		_, _ = redisx.Ack(ctx, r.RDB, stream, r.Group, m.ID)
		return nil
	}

	// Failure path
	attempt := 0
	if a, ok := toInt(m.Payload["attempt"]); ok { attempt = a }
	attempt++

	if attempt >= r.MaxAttempts {
		// DLQ
		_ = addJSON(ctx, r.RDB, r.Streams.DLQ, with(m.Payload, map[string]any{
			"attempt": attempt,
			"error":   execErr.Error(),
		}))
		now := timePtr(time.Now().UTC())
		errText := execErr.Error()
		_, _ = r.Store.UpdateRunStatus(ctx, jobs.UpdateRunStatusParams{
			RunID:      runID,
			Status:     jobs.StatusDead,
			ErrorText:  &errText,
			FinishedAt: now,
			Attempts:   &attempt,
		})
		_, _ = redisx.Ack(ctx, r.RDB, stream, r.Group, m.ID)
		return nil
	}

	// Retry — exponential backoff (base 1s, cap 30s)
	backoff := time.Duration(1<<min(attempt-1, 5)) * time.Second
	if retryable {
		// leave as computed; otherwise still retry by default
	}
	nextAvail := time.Now().Add(backoff).UnixMilli()
	_ = addJSON(ctx, r.RDB, r.Streams.Retry, with(m.Payload, map[string]any{
		"attempt":         attempt,
		"backoff_ms":      backoff.Milliseconds(),
		"available_at_ms": nextAvail,
		"error":           execErr.Error(),
	}))
	// Update DB to retried
	errText := execErr.Error()
	_, _ = r.Store.UpdateRunStatus(ctx, jobs.UpdateRunStatusParams{
		RunID:    runID,
		Status:   jobs.StatusRetried,
		ErrorText: &errText,
		Attempts: &attempt,
	})
	_, _ = redisx.Ack(ctx, r.RDB, stream, r.Group, m.ID)
	return nil
}

// -------- helpers --------

func addJSON(ctx context.Context, rdb *redis.Client, stream string, payload map[string]any) error {
	_, err := redisx.XAddJSON(ctx, rdb, stream, payload)
	return err
}
func with(m map[string]any, extra map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m { out[k] = v }
	for k, v := range extra { out[k] = v }
	return out
}
func str(v any) (string, bool) {
	if v == nil { return "", false }
	if s, ok := v.(string); ok { return s, true }
	return "", false
}
func toInt(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	default:
		return 0, false
	}
}
func min(a, b int) int { if a<b { return a }; return b }
func timePtr(t time.Time) *time.Time { return &t }
