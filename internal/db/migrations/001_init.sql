CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    handler TEXT NOT NULL CHECK (handler IN ('shell', 'http')),
    args JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    cron_expr TEXT,
    fixed_interval_seconds INT,
    next_run_at TIMESTAMPTZ NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    last_enqueued_at TIMESTAMPTZ,
    enabled BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX IF NOT EXISTS idx_schedules_next_run_enabled
    ON schedules(next_run_at, enabled);

CREATE TABLE IF NOT EXISTS job_runs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    run_id UUID NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('queued','running','success','failed','retried','dead')),
    attempts INT NOT NULL DEFAULT 0,
    error_text TEXT,
    worker_id TEXT,
    idempotency_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job_started
    ON job_runs(job_id, started_at DESC);
