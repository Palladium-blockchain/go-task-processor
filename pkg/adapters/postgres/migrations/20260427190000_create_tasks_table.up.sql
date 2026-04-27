CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    status INTEGER NOT NULL DEFAULT 0,
    type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    run_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tasks_attempts_non_negative CHECK (attempts >= 0),
    CONSTRAINT tasks_max_attempts_positive CHECK (max_attempts > 0)
);

CREATE INDEX IF NOT EXISTS tasks_status_run_at_idx
    ON tasks (status, run_at);

CREATE INDEX IF NOT EXISTS tasks_locked_until_idx
    ON tasks (locked_until);
