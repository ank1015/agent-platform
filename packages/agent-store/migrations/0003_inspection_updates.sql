ALTER TABLE sessions
    ADD COLUMN next_update_sequence bigint NOT NULL DEFAULT 1 CHECK (next_update_sequence >= 1),
    ADD COLUMN retained_update_sequence bigint NOT NULL DEFAULT 1 CHECK (retained_update_sequence >= 1);

CREATE TABLE session_updates (
    session_id uuid NOT NULL REFERENCES sessions(id),
    sequence bigint NOT NULL CHECK (sequence >= 1),
    schema_version smallint NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    kind text NOT NULL CHECK (kind IN (
        'session.changed', 'history.appended', 'operation.changed',
        'wait.changed', 'event.changed', 'processing.changed', 'harness.progress'
    )),
    payload json NOT NULL,
    source_event_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_id, sequence),
    FOREIGN KEY (session_id, source_event_id) REFERENCES session_events(session_id, id)
);

CREATE INDEX session_updates_cleanup_idx ON session_updates (created_at, session_id, sequence);

ALTER TABLE session_operations
    ADD COLUMN request_removed_at timestamptz;
