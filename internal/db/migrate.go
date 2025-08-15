// path: internal/db/migrate.go
package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func dsnFromEnv() string {
	host := getenv("POSTGRES_HOST", "localhost")
	port := getenv("POSTGRES_PORT", "5432")
	user := getenv("POSTGRES_USER", "jobs")
	pass := getenv("POSTGRES_PASSWORD", "jobs")
	name := getenv("POSTGRES_DB", "jobs")
	return "postgres://" + user + ":" + pass + "@" + host + ":" + port + "/" + name + "?sslmode=disable"
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Run applies all .sql files in internal/db/migrations in alphabetical order.
func Run() error {
	dir := "internal/db/migrations"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil
	}

	db, err := sql.Open("pgx", dsnFromEnv())
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		sqlText := string(b)
		if strings.TrimSpace(sqlText) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, sqlText); err != nil {
			return fmt.Errorf("exec %s: %w", f, err)
		}
	}
	return nil
}
