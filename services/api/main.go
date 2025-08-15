package main

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/rishansujesh/job-scheduler/internal/api/server"
	redisx "github.com/rishansujesh/job-scheduler/internal/redis"
)

func main() {
	ctx := context.Background()
	_ = ctx

	// ---- Config ----
	pgHost := getenv("POSTGRES_HOST", "localhost")
	pgPort := getenv("POSTGRES_PORT", "5432")
	pgUser := getenv("POSTGRES_USER", "jobs")
	pgPass := getenv("POSTGRES_PASSWORD", "jobs")
	pgDB := getenv("POSTGRES_DB", "jobs")

	httpAddr := getenv("API_HTTP_ADDR", ":8080")
	grpcAddr := getenv("API_GRPC_ADDR", ":9090")

	// ---- DB ----
	dsn := "postgres://" + pgUser + ":" + pgPass + "@" + pgHost + ":" + pgPort + "/" + pgDB + "?sslmode=disable"
	db := must(sql.Open("pgx", dsn))
	defer db.Close()
	must0(db.Ping())

	// ---- Redis ----
	rdb, err := redisx.NewClientWithBackoff(context.Background(), redisx.FromEnv())
	if err != nil {
		log.Fatalf("redis connect failed: %v", err)
	}
	defer rdb.Close()

	// ---- Start gRPC + REST ----
	log.Fatal(server.StartServers(db, rdb, httpAddr, grpcAddr))
}

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
