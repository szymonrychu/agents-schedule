CREATE TABLE IF NOT EXISTS schedules (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (char_length(name) BETWEEN 1 AND 100),
    cron text NOT NULL,
    timezone text NOT NULL,
    preset_id uuid NOT NULL,
    prompt text NOT NULL CHECK (char_length(prompt) BETWEEN 1 AND 16384),
    enabled boolean NOT NULL DEFAULT true,
    armed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
    id uuid PRIMARY KEY,
    schedule_id uuid NOT NULL REFERENCES schedules (id) ON DELETE CASCADE,
    trigger text NOT NULL CHECK (trigger IN ('scheduled', 'manual')),
    fire_time timestamptz NOT NULL,
    status text NOT NULL CHECK (status IN ('new', 'in-progress', 'handled', 'failed', 'ignored')),
    reason text,
    attempts int NOT NULL DEFAULT 0,
    session_slug text,
    session_id uuid,
    message_id text,
    reuse text CHECK (reuse IN ('delivered', 'fallback')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS runs_scheduled_fire_uniq
    ON runs (schedule_id, fire_time) WHERE trigger = 'scheduled';

CREATE INDEX IF NOT EXISTS runs_status_fire_time_idx ON runs (status, fire_time);
