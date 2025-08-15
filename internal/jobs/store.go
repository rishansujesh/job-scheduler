package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	DB        *sql.DB
	DefaultTO time.Duration // default timeout per query
}

func NewStore(db *sql.DB) *Store {
	return &Store{DB: db, DefaultTO: 5 * time.Second}
}

/* ===================== Jobs ===================== */

type CreateJobParams struct {
	Name    string
	Type    string
	Handler string // "shell" | "http"
	Args    map[string]any
	Enabled bool
}

func (s *Store) CreateJob(ctx context.Context, p CreateJobParams) (*Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	argsJSON, _ := json.Marshal(p.Args)
	q := `
INSERT INTO jobs (name, type, handler, args, enabled)
VALUES ($1, $2, $3, $4::jsonb, $5)
RETURNING id, name, type, handler, args, enabled, created_at, updated_at;
`
	var j Job
	var argsRaw []byte
	if err := s.DB.QueryRowContext(ctx, q, p.Name, p.Type, p.Handler, string(argsJSON), p.Enabled).
		Scan(&j.ID, &j.Name, &j.Type, &j.Handler, &argsRaw, &j.Enabled, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(argsRaw, &j.Args)
	return &j, nil
}

type ListJobsParams struct {
	Limit  int
	Offset int
}

func (s *Store) ListJobs(ctx context.Context, p ListJobsParams) ([]Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	if p.Limit <= 0 || p.Limit > 200 {
		p.Limit = 50
	}
	q := `
SELECT id, name, type, handler, args, enabled, created_at, updated_at
FROM jobs
ORDER BY created_at DESC
LIMIT $1 OFFSET $2;
`
	rows, err := s.DB.QueryContext(ctx, q, p.Limit, p.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		var j Job
		var argsRaw []byte
		if err := rows.Scan(&j.ID, &j.Name, &j.Type, &j.Handler, &argsRaw, &j.Enabled, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(argsRaw, &j.Args)
		out = append(out, j)
	}
	return out, rows.Err()
}

type UpdateJobParams struct {
	ID      string
	Name    *string
	Args    *map[string]any
	Enabled *bool
}

func (s *Store) UpdateJob(ctx context.Context, p UpdateJobParams) (*Job, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	// Build dynamic pieces
	set := ""
	args := []any{}
	i := 1

	if p.Name != nil {
		set += fmt.Sprintf("name = $%d,", i)
		args = append(args, *p.Name)
		i++
	}
	if p.Args != nil {
		b, _ := json.Marshal(*p.Args)
		set += fmt.Sprintf("args = $%d::jsonb,", i)
		args = append(args, string(b))
		i++
	}
	if p.Enabled != nil {
		set += fmt.Sprintf("enabled = $%d,", i)
		args = append(args, *p.Enabled)
		i++
	}
	if set == "" {
		return nil, errors.New("no fields to update")
	}
	set += "updated_at = now()"
	q := fmt.Sprintf(`
UPDATE jobs SET %s
WHERE id = $%d
RETURNING id, name, type, handler, args, enabled, created_at, updated_at;`, set, i)
	args = append(args, p.ID)

	var j Job
	var argsRaw []byte
	if err := s.DB.QueryRowContext(ctx, q, args...).
		Scan(&j.ID, &j.Name, &j.Type, &j.Handler, &argsRaw, &j.Enabled, &j.CreatedAt, &j.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal(argsRaw, &j.Args)
	return &j, nil
}

func (s *Store) DisableJob(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()
	res, err := s.DB.ExecContext(ctx, `UPDATE jobs SET enabled=false, updated_at=now() WHERE id=$1`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

/* ===================== Schedules ===================== */

type CreateScheduleParams struct {
	JobID                string
	CronExpr             *string
	FixedIntervalSeconds *int
	NextRunAt            time.Time
	Timezone             string
	Enabled              bool
}

func (s *Store) CreateSchedule(ctx context.Context, p CreateScheduleParams) (*Schedule, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	q := `
INSERT INTO schedules (job_id, cron_expr, fixed_interval_seconds, next_run_at, timezone, enabled)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING id, job_id, cron_expr, fixed_interval_seconds, next_run_at, timezone, last_enqueued_at, enabled;
`
	var sc Schedule
	if err := s.DB.QueryRowContext(ctx, q, p.JobID, p.CronExpr, p.FixedIntervalSeconds, p.NextRunAt, p.Timezone, p.Enabled).
		Scan(&sc.ID, &sc.JobID, &sc.CronExpr, &sc.FixedIntervalSeconds, &sc.NextRunAt, &sc.Timezone, &sc.LastEnqueuedAt, &sc.Enabled); err != nil {
		return nil, err
	}
	return &sc, nil
}

type ListSchedulesParams struct {
	Limit  int
	Offset int
}

func (s *Store) ListSchedules(ctx context.Context, p ListSchedulesParams) ([]Schedule, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	if p.Limit <= 0 || p.Limit > 200 {
		p.Limit = 50
	}
	q := `
SELECT id, job_id, cron_expr, fixed_interval_seconds, next_run_at, timezone, last_enqueued_at, enabled
FROM schedules
ORDER BY next_run_at ASC
LIMIT $1 OFFSET $2;
`
	rows, err := s.DB.QueryContext(ctx, q, p.Limit, p.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Schedule
	for rows.Next() {
		var sc Schedule
		if err := rows.Scan(&sc.ID, &sc.JobID, &sc.CronExpr, &sc.FixedIntervalSeconds, &sc.NextRunAt, &sc.Timezone, &sc.LastEnqueuedAt, &sc.Enabled); err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

type UpdateScheduleParams struct {
	ID                   string
	CronExpr             *string
	FixedIntervalSeconds *int
	NextRunAt            *time.Time
	Timezone             *string
	Enabled              *bool
	LastEnqueuedAt       *time.Time
}

func (s *Store) UpdateSchedule(ctx context.Context, p UpdateScheduleParams) (*Schedule, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	set := ""
	args := []any{}
	i := 1

	if p.CronExpr != nil {
		set += fmt.Sprintf("cron_expr = $%d,", i)
		args = append(args, *p.CronExpr)
		i++
	}
	if p.FixedIntervalSeconds != nil {
		set += fmt.Sprintf("fixed_interval_seconds = $%d,", i)
		args = append(args, *p.FixedIntervalSeconds)
		i++
	}
	if p.NextRunAt != nil {
		set += fmt.Sprintf("next_run_at = $%d,", i)
		args = append(args, *p.NextRunAt)
		i++
	}
	if p.Timezone != nil {
		set += fmt.Sprintf("timezone = $%d,", i)
		args = append(args, *p.Timezone)
		i++
	}
	if p.Enabled != nil {
		set += fmt.Sprintf("enabled = $%d,", i)
		args = append(args, *p.Enabled)
		i++
	}
	if p.LastEnqueuedAt != nil {
		set += fmt.Sprintf("last_enqueued_at = $%d,", i)
		args = append(args, *p.LastEnqueuedAt)
		i++
	}

	if set == "" {
		return nil, errors.New("no fields to update")
	}

	q := fmt.Sprintf(`
UPDATE schedules SET %s
WHERE id = $%d
RETURNING id, job_id, cron_expr, fixed_interval_seconds, next_run_at, timezone, last_enqueued_at, enabled;`, set[:len(set)-1], i)
	args = append(args, p.ID)

	var sc Schedule
	if err := s.DB.QueryRowContext(ctx, q, args...).
		Scan(&sc.ID, &sc.JobID, &sc.CronExpr, &sc.FixedIntervalSeconds, &sc.NextRunAt, &sc.Timezone, &sc.LastEnqueuedAt, &sc.Enabled); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &sc, nil
}

func (s *Store) DeleteSchedule(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()
	res, err := s.DB.ExecContext(ctx, `DELETE FROM schedules WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

/* ===================== Job Runs ===================== */

type InsertRunParams struct {
	JobID          string
	RunID          string // UUID
	Status         JobRunStatus
	WorkerID       *string
	IdempotencyKey string
}

func (s *Store) InsertRun(ctx context.Context, p InsertRunParams) (*JobRun, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()
	q := `
INSERT INTO job_runs (job_id, run_id, status, worker_id, idempotency_key)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, job_id, run_id, started_at, finished_at, status, attempts, error_text, worker_id, idempotency_key;
`
	var r JobRun
	if err := s.DB.QueryRowContext(ctx, q, p.JobID, p.RunID, string(p.Status), p.WorkerID, p.IdempotencyKey).
		Scan(&r.ID, &r.JobID, &r.RunID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.Attempts, &r.ErrorText, &r.WorkerID, &r.IdempotencyKey); err != nil {
		return nil, err
	}
	return &r, nil
}

type UpdateRunStatusParams struct {
	RunID      string
	Status     JobRunStatus
	ErrorText  *string
	WorkerID   *string
	FinishedAt *time.Time
	Attempts   *int
}

func (s *Store) UpdateRunStatus(ctx context.Context, p UpdateRunStatusParams) (*JobRun, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()

	set := "status = $1"
	args := []any{string(p.Status)}
	i := 2

	if p.ErrorText != nil {
		set += fmt.Sprintf(", error_text = $%d", i)
		args = append(args, *p.ErrorText)
		i++
	}
	if p.WorkerID != nil {
		set += fmt.Sprintf(", worker_id = $%d", i)
		args = append(args, *p.WorkerID)
		i++
	}
	if p.FinishedAt != nil {
		set += fmt.Sprintf(", finished_at = $%d", i)
		args = append(args, *p.FinishedAt)
		i++
	}
	if p.Attempts != nil {
		set += fmt.Sprintf(", attempts = $%d", i)
		args = append(args, *p.Attempts)
		i++
	}

	q := fmt.Sprintf(`
UPDATE job_runs
SET %s
WHERE run_id = $%d
RETURNING id, job_id, run_id, started_at, finished_at, status, attempts, error_text, worker_id, idempotency_key;`, set, i)
	args = append(args, p.RunID)

	var r JobRun
	if err := s.DB.QueryRowContext(ctx, q, args...).
		Scan(&r.ID, &r.JobID, &r.RunID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.Attempts, &r.ErrorText, &r.WorkerID, &r.IdempotencyKey); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRunsForJob(ctx context.Context, jobID string, limit int) ([]JobRun, error) {
	ctx, cancel := context.WithTimeout(ctx, s.DefaultTO)
	defer cancel()
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	q := `
SELECT id, job_id, run_id, started_at, finished_at, status, attempts, error_text, worker_id, idempotency_key
FROM job_runs
WHERE job_id = $1
ORDER BY started_at DESC
LIMIT $2;
`
	rows, err := s.DB.QueryContext(ctx, q, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobRun
	for rows.Next() {
		var r JobRun
		if err := rows.Scan(&r.ID, &r.JobID, &r.RunID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.Attempts, &r.ErrorText, &r.WorkerID, &r.IdempotencyKey); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
