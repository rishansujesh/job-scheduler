package jobs

import (
	"time"
)

type Job struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Type      string         `json:"type"`
	Handler   string         `json:"handler"` // "shell" | "http"
	Args      map[string]any `json:"args"`
	Enabled   bool           `json:"enabled"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type Schedule struct {
	ID                   string     `json:"id"`
	JobID                string     `json:"job_id"`
	CronExpr             *string    `json:"cron_expr,omitempty"`
	FixedIntervalSeconds *int       `json:"fixed_interval_seconds,omitempty"`
	NextRunAt            time.Time  `json:"next_run_at"`
	Timezone             string     `json:"timezone"`
	LastEnqueuedAt       *time.Time `json:"last_enqueued_at,omitempty"`
	Enabled              bool       `json:"enabled"`
}

type JobRunStatus string

const (
	StatusQueued  JobRunStatus = "queued"
	StatusRunning JobRunStatus = "running"
	StatusSuccess JobRunStatus = "success"
	StatusFailed  JobRunStatus = "failed"
	StatusRetried JobRunStatus = "retried"
	StatusDead    JobRunStatus = "dead"
)

type JobRun struct {
	ID             int64        `json:"id"`
	JobID          string       `json:"job_id"`
	RunID          string       `json:"run_id"` // uuid
	StartedAt      time.Time    `json:"started_at"`
	FinishedAt     *time.Time   `json:"finished_at,omitempty"`
	Status         JobRunStatus `json:"status"`
	Attempts       int          `json:"attempts"`
	ErrorText      *string      `json:"error_text,omitempty"`
	WorkerID       *string      `json:"worker_id,omitempty"`
	IdempotencyKey string       `json:"idempotency_key"`
}
