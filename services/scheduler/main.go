package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"job-scheduler/internal/jobs"
	redisx "job-scheduler/internal/redis"
	"job-scheduler/internal/schedule"
)

func main() {
	// ---- Config ----
	pgHost := getenv("POSTGRES_HOST", "localhost")
	pgPort := getenv("POSTGRES_PORT", "5432")
	pgUser := getenv("POSTGRES_USER", "jobs")
	pgPass := getenv("POSTGRES_PASSWORD", "jobs")
	pgDB := getenv("POSTGRES_DB", "jobs")

	leaderKey := getenv("LEADER_KEY", "scheduler:leader")
	leaderTTL := atoi(getenv("LEADER_TTL_SEC", "10"), 10)
	httpAddr := getenv("SCHEDULER_HTTP_ADDR", ":8081")

	// ---- DB ----
	dsn := "postgres://" + pgUser + ":" + pgPass + "@" + pgHost + ":" + pgPort + "/" + pgDB + "?sslmode=disable"
	db := must(sql.Open("pgx", dsn))
	defer db.Close()
	must0(db.Ping())

	// ---- Redis ----
	ctx := context.Background()
	rdb, err := redisx.NewClientWithBackoff(ctx, redisx.FromEnv())
	if err != nil {
		log.Fatalf("redis connect failed: %v", err)
	}
	defer rdb.Close()

	// ---- Leader election ----
	instanceID := hostname()
	elect := redisx.NewLeaderElector(rdb, leaderKey, leaderTTL, instanceID)
	elect.Start(ctx)
	defer elect.Stop()

	// ---- Scanner loop ----
	store := jobs.NewStore(db)
	sc := &scanLoop{
		DB:      db,
		Store:   store,
		RDB:     rdb,
		Streams: redisx.StreamsFromEnv(),
		Logger:  log.Default(),
		Now:     time.Now,
	}
	go sc.Loop(ctx, elect)

	// ---- Health server ----
	var roleCache atomic.Value
	roleCache.Store("follower")
	go func() {
		for {
			if elect.IsLeader() {
				roleCache.Store("leader")
			} else {
				roleCache.Store("follower")
			}
			time.Sleep(1 * time.Second)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"scheduler"}`))
	})
	mux.HandleFunc("/role", func(w http.ResponseWriter, r *http.Request) {
		role := roleCache.Load().(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`"` + role + `"`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("scheduler listening on %s", httpAddr)
	log.Fatal(srv.ListenAndServe())
}

type scanLoop struct {
	DB      *sql.DB
	Store   *jobs.Store
	RDB     *redis.Client
	Streams redisx.StreamsConfig
	Logger  *log.Logger
	Now     func() time.Time
}

func (s *scanLoop) Loop(ctx context.Context, elect *redisx.LeaderElector) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !elect.IsLeader() {
				continue
			}
			if err := s.runOnce(ctx); err != nil {
				s.Logger.Printf("scanner error: %v", err)
			}
		}
	}
}

func (s *scanLoop) runOnce(ctx context.Context) error {
	now := s.Now().UTC()
	rows, err := s.DB.QueryContext(ctx, `
SELECT id, job_id, cron_expr, fixed_interval_seconds, next_run_at, timezone, last_enqueued_at, enabled
FROM schedules
WHERE enabled = true AND next_run_at <= $1
ORDER BY next_run_at ASC
LIMIT 200;`, now)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var sc jobs.Schedule
		if err := rows.Scan(&sc.ID, &sc.JobID, &sc.CronExpr, &sc.FixedIntervalSeconds, &sc.NextRunAt, &sc.Timezone, &sc.LastEnqueuedAt, &sc.Enabled); err != nil {
			return err
		}
		locked, err := schedule.WithScheduleTxLock(ctx, s.DB, sc.ID, func(tx *sql.Tx) error {
			// still due?
			var stillDue bool
			if err := tx.QueryRowContext(ctx, `
SELECT (enabled = true AND next_run_at <= $1) FROM schedules WHERE id = $2
`, now, sc.ID).Scan(&stillDue); err != nil {
				return err
			}
			if !stillDue {
				return nil
			}

			// load job
			var job jobs.Job
			var argsRaw []byte
			if err := tx.QueryRowContext(ctx, `
SELECT id, name, type, handler, args, enabled, created_at, updated_at
FROM jobs WHERE id = $1 AND enabled = true`, sc.JobID).
				Scan(&job.ID, &job.Name, &job.Type, &job.Handler, &argsRaw, &job.Enabled, &job.CreatedAt, &job.UpdatedAt); err != nil {
				return err
			}
			_ = json.Unmarshal(argsRaw, &job.Args)

			// idempotency + payload
			runID := uuid.NewString()
			idKey, err := jobs.ComputeIdempotencyKey(job.ID, sc.NextRunAt, job.Args)
			if err != nil {
				return err
			}
			payload := map[string]any{
				"run_id":  runID,
				"job_id":  job.ID,
				"handler": job.Handler,
				"args":    job.Args,
			}

			// insert run (queued)
			store := jobs.NewStore(s.DB)
			store.DefaultTO = 5 * time.Second
			if _, err := store.InsertRun(ctx, jobs.InsertRunParams{
				JobID:          job.ID,
				RunID:          runID,
				Status:         jobs.StatusQueued,
				WorkerID:       nil,
				IdempotencyKey: idKey,
			}); err != nil {
				return err
			}

			// enqueue
			if _, err := redisx.XAddJSON(ctx, s.RDB, s.Streams.Scheduled, payload); err != nil {
				return err
			}

			// next run
			next, err := schedule.NextRun(sc.CronExpr, sc.FixedIntervalSeconds, sc.NextRunAt, sc.Timezone)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE schedules SET next_run_at=$1, last_enqueued_at=$2 WHERE id=$3
`, next, now, sc.ID); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !locked {
			continue
		}
	}
	return rows.Err()
}

// ---------- helpers ----------
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func atoi(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	return v
}

func must0(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func hostname() string {
	h, _ := os.Hostname()
	if h == "" {
		h = "instance"
	}
	return h
}
