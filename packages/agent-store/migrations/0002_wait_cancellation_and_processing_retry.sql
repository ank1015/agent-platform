ALTER TABLE sessions
    ADD COLUMN processing_revision bigint NOT NULL DEFAULT 0 CHECK (processing_revision >= 0);

ALTER TABLE session_events
    ADD COLUMN retry_failure_baseline bigint NOT NULL DEFAULT 0 CHECK (retry_failure_baseline >= 0);

CREATE TABLE session_processing_retries (
    id uuid PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions(id),
    idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    event_id uuid NOT NULL,
    expected_processing_revision bigint NOT NULL,
    processing_revision bigint NOT NULL,
    reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, idempotency_key),
    FOREIGN KEY (session_id, event_id) REFERENCES session_events(session_id, id)
);
