CREATE TABLE IF NOT EXISTS schedule_chains (
    schedule_id uuid NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    next_schedule_id uuid NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    position int NOT NULL,
    PRIMARY KEY (schedule_id, next_schedule_id),
    CHECK (schedule_id <> next_schedule_id)
);

ALTER TABLE runs DROP CONSTRAINT IF EXISTS runs_trigger_check;
ALTER TABLE runs ADD CONSTRAINT runs_trigger_check
    CHECK (trigger IN ('scheduled', 'manual', 'chained'));

ALTER TABLE runs ADD COLUMN IF NOT EXISTS then_schedule_ids uuid[];
ALTER TABLE runs ADD COLUMN IF NOT EXISTS chain_status text
    CHECK (chain_status IN ('pending', 'fired', 'skipped', 'expired'));
ALTER TABLE runs ADD COLUMN IF NOT EXISTS result_text text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS chained_from uuid REFERENCES runs(id) ON DELETE SET NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS chain_source text
    CHECK (chain_source IN ('schedule', 'webhook'));
ALTER TABLE runs ADD COLUMN IF NOT EXISTS chain_ref text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS chain_depth int NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS previous_name text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS previous_status text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS previous_session_slug text;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS previous_result text;

CREATE UNIQUE INDEX IF NOT EXISTS runs_chain_once
    ON runs (chain_source, chain_ref, schedule_id) WHERE trigger = 'chained';
CREATE INDEX IF NOT EXISTS runs_chain_pending_idx
    ON runs (created_at) WHERE chain_status = 'pending';
