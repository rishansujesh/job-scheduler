package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	host := getenv("POSTGRES_HOST", "localhost")
	port := getenv("POSTGRES_PORT", "5432")
	user := getenv("POSTGRES_USER", "jobs")
	pass := getenv("POSTGRES_PASSWORD", "jobs")
	dbname := getenv("POSTGRES_DB", "jobs")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, pass, host, port, dbname)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("failed to connect to db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	files, err := filepath.Glob("internal/db/migrations/*.sql")
	if err != nil {
		log.Fatalf("failed to read migrations: %v", err)
	}

	for _, file := range files {
		log.Printf("applying migration: %s", file)
		sqlBytes, err := ioutil.ReadFile(file)
		if err != nil {
			log.Fatalf("failed to read file %s: %v", file, err)
		}
		queries := strings.TrimSpace(string(sqlBytes))
		if queries == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, queries); err != nil {
			log.Fatalf("failed to execute migration %s: %v", file, err)
		}
	}

	log.Println("migrations applied successfully")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
