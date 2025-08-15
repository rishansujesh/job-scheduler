package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"job-scheduler/internal/jobs"
	redisx "job-scheduler/internal/redis"
	"job-scheduler/internal/worker"
)

func main() {
	ctx := context.Background()

	// ---- Config ----
	pgHost := getenv("POSTGRES_HOST", "localhost")
	pgPort := getenv("POSTGRES_PORT", "5432")
	pgUser := getenv("POSTGRES_USER", "jobs")
	pgPass := getenv("POSTGRES_PASSWORD", "jobs")
	pgDB := getenv("POSTGRES_DB", "jobs")

	httpAddr := getenv("WORKER_HTTP_ADDR", ":8082")
	group := getenv("REDIS_CONSUMER_GROUP", "cg:workers")
	consumer := hostname()

	// ---- DB ----
	dsn := "postgres://" + pgUser + ":" + pgPass + "@" + pgHost + ":" + pgPort + "/" + pgDB + "?sslmode=disable"
	db := must(sql.Open("pgx", dsn))
	defer db.Close()
	must0(db.Ping())

	// ---- Redis ----
	rdb, err := redisx.NewClientWithBackoff(ctx, redisx.FromEnv())
	if err != nil {
		log.Fatalf("redis connect failed: %v", err)
	}
	defer rdb.Close()

	// ---- Runner ----
	store := jobs.NewStore(db)
	r := &worker.Runner{
		DB:           db,
		Store:        store,
		RDB:          rdb,
		Streams:      redisx.StreamsFromEnv(),
		Group:        group,
		ConsumerName: consumer,
		MaxAttempts:  5,
		Logger:       log.Default(),
	}
	r.Start(ctx)

	// ---- Health server ----
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"worker"}`))
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
	log.Printf("worker listening on %s (group=%s consumer=%s)", httpAddr, group, consumer)
	log.Fatal(srv.ListenAndServe())
}

// ---- helpers ----
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
		h = "worker"
	}
	return h
}
