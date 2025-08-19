package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	host := env("POSTGRES_HOST", "localhost")
	port := env("POSTGRES_PORT", "5432")
	dbname := env("POSTGRES_DB", "jobs")
	user := env("POSTGRES_USER", "jobs")
	pass := env("POSTGRES_PASSWORD", "jobs")
	ssl := env("POSTGRES_SSLMODE", "disable")

	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		host, port, dbname, user, pass, ssl)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	if err := runMigrations(ctx, conn, "internal/db/migrations"); err != nil {
		fmt.Fprintf(os.Stderr, "migrate error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("migrations: done")
}

func runMigrations(ctx context.Context, conn *pgx.Conn, dir string) error {
	_, err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
)`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	var files []string
	err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".sql") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk migrations: %w", err)
	}
	if len(files) == 0 {
		fmt.Println("migrations: no files found, nothing to do")
		return nil
	}
	sort.Strings(files)

	applied := map[string]string{}
	rows, err := conn.Query(ctx, `SELECT filename, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("select schema_migrations: %w", err)
	}
	for rows.Next() {
		var fn, sum string
		if err := rows.Scan(&fn, &sum); err != nil {
			return fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[fn] = sum
	}
	rows.Close()

	for _, f := range files {
		sqlBytes, rerr := os.ReadFile(f)
		if rerr != nil {
			return fmt.Errorf("read %s: %w", f, rerr)
		}
		sum := sha256.Sum256(sqlBytes)
		sumHex := hex.EncodeToString(sum[:])

		if prev, ok := applied[f]; ok {
			if prev != sumHex {
				return fmt.Errorf("migration %s already applied with different checksum (got %s, have %s)", f, sumHex, prev)
			}
			fmt.Printf("migrations: already applied %s\n", f)
			continue
		}

		fmt.Printf("migrations: applying %s\n", f)
		start := time.Now()
		if _, err := conn.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("exec %s: %w", f, err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (filename, checksum) VALUES ($1,$2)`, f, sumHex); err != nil {
			return fmt.Errorf("record %s: %w", f, err)
		}
		fmt.Printf("migrations: applied %s in %s\n", f, time.Since(start).Round(time.Millisecond))
	}

	return nil
}
