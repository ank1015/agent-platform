CREATE TABLE sessions (
    id uuid PRIMARY KEY,
    project_id text NOT NULL,
    harness_id text NOT NULL,
    harness_version text NOT NULL,
    configuration json NOT NULL,
    name text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'idle'
        CHECK (status IN ('idle', 'running', 'waiting', 'cancelling', 'cancelled', 'failed')),
    state json NOT NULL,
    state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    next_event_sequence bigint NOT NULL DEFAULT 1 CHECK (next_event_sequence >= 1),
    next_history_sequence bigint NOT NULL DEFAULT 1 CHECK (next_history_sequence >= 1),
    processing_enabled boolean NOT NULL DEFAULT true,
    processing_error jsonb,
    current_event_id uuid,
    lease_token uuid,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (id, current_event_id),
    CHECK (
        (current_event_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR
        (current_event_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX sessions_project_created_idx ON sessions (project_id, created_at DESC, id DESC);
CREATE INDEX sessions_expired_lease_idx ON sessions (lease_expires_at)
    WHERE lease_token IS NOT NULL;

CREATE TABLE session_events (
    id uuid PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions(id),
    sequence bigint NOT NULL CHECK (sequence >= 1),
    type text NOT NULL
        CHECK (type IN ('user_message', 'operation_completed', 'cancellation_requested', 'wait_resumed')),
    payload json NOT NULL,
    operation_id uuid,
    wait_id uuid,
    idempotency_key text,
    request_hash text,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'handled', 'blocked')),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    handled_at timestamptz,
    UNIQUE (session_id, sequence),
    UNIQUE (session_id, id),
    CHECK ((idempotency_key IS NULL) = (request_hash IS NULL)),
    CHECK (
        (type = 'operation_completed' AND operation_id IS NOT NULL AND wait_id IS NULL)
        OR (type = 'wait_resumed' AND wait_id IS NOT NULL AND operation_id IS NULL)
        OR (type IN ('user_message', 'cancellation_requested') AND operation_id IS NULL AND wait_id IS NULL)
    ),
    CHECK ((status = 'handled') = (handled_at IS NOT NULL))
);

CREATE UNIQUE INDEX session_events_idempotency_idx
    ON session_events (session_id, type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX session_events_operation_completion_idx
    ON session_events (operation_id) WHERE type = 'operation_completed';
CREATE UNIQUE INDEX session_events_wait_resumption_idx
    ON session_events (wait_id) WHERE type = 'wait_resumed';
CREATE INDEX session_events_inbox_idx ON session_events (session_id, sequence)
    WHERE status IN ('pending', 'processing', 'blocked');

ALTER TABLE sessions ADD CONSTRAINT sessions_current_event_fk
    FOREIGN KEY (id, current_event_id) REFERENCES session_events(session_id, id)
    DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE session_event_attempts (
    id uuid PRIMARY KEY,
    event_id uuid NOT NULL REFERENCES session_events(id),
    attempt_number integer NOT NULL CHECK (attempt_number >= 1),
    lease_token uuid NOT NULL,
    input_state_version bigint NOT NULL CHECK (input_state_version >= 0),
    history_through_sequence bigint NOT NULL CHECK (history_through_sequence >= 0),
    status text NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'committed', 'errored', 'abandoned')),
    error jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (event_id, attempt_number),
    CHECK ((status = 'running') = (finished_at IS NULL))
);

CREATE TABLE session_history (
    id uuid PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions(id),
    sequence bigint NOT NULL CHECK (sequence >= 1),
    source_event_id uuid,
    role text NOT NULL CHECK (role IN ('user', 'assistant', 'tool_result', 'system', 'custom')),
    message json NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (session_id, sequence),
    UNIQUE (session_id, id),
    FOREIGN KEY (session_id, source_event_id) REFERENCES session_events(session_id, id)
);

CREATE INDEX session_history_read_idx ON session_history (session_id, sequence);

CREATE TABLE session_operations (
    id uuid PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions(id),
    source_event_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('llm', 'execution', 'llm_cancellation', 'withdraw')),
    gateway_connection_id text,
    target_operation_id uuid,
    previous_operation_id uuid,
    gateway_idempotency_key text NOT NULL,
    request_hash text NOT NULL,
    gateway_job_id uuid,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'submitting', 'accepted', 'succeeded', 'failed', 'cancelled', 'unknown')),
    result json,
    error jsonb,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    submitted_at timestamptz,
    completed_at timestamptz,
    UNIQUE (session_id, id),
    UNIQUE (gateway_connection_id, gateway_job_id, id),
    FOREIGN KEY (session_id, source_event_id) REFERENCES session_events(session_id, id),
    FOREIGN KEY (session_id, target_operation_id) REFERENCES session_operations(session_id, id),
    FOREIGN KEY (session_id, previous_operation_id) REFERENCES session_operations(session_id, id),
    CHECK (
        (kind IN ('llm', 'execution') AND gateway_connection_id IS NOT NULL)
        OR (kind IN ('llm_cancellation', 'withdraw') AND target_operation_id IS NOT NULL)
    ),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK (target_operation_id IS NULL OR target_operation_id <> id),
    CHECK (previous_operation_id IS NULL OR previous_operation_id <> id),
    CHECK (
        (status IN ('succeeded', 'failed', 'cancelled', 'unknown') AND completed_at IS NOT NULL)
        OR (status IN ('pending', 'submitting', 'accepted') AND completed_at IS NULL)
    )
);

CREATE UNIQUE INDEX session_operations_gateway_job_idx
    ON session_operations (gateway_connection_id, gateway_job_id)
    WHERE gateway_job_id IS NOT NULL;
CREATE INDEX session_operations_work_idx ON session_operations (next_attempt_at, created_at, id)
    WHERE status IN ('pending', 'submitting', 'accepted');
CREATE INDEX session_operations_expired_lease_idx ON session_operations (lease_expires_at)
    WHERE lease_token IS NOT NULL;
CREATE INDEX session_operations_session_idx ON session_operations (session_id, created_at, id);

ALTER TABLE session_events ADD CONSTRAINT session_events_operation_fk
    FOREIGN KEY (session_id, operation_id) REFERENCES session_operations(session_id, id);

CREATE TABLE session_operation_requests (
    operation_id uuid PRIMARY KEY REFERENCES session_operations(id),
    request json NOT NULL,
    expires_at timestamptz
);

CREATE INDEX session_operation_requests_expiration_idx
    ON session_operation_requests (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE session_operation_attempts (
    id uuid PRIMARY KEY,
    operation_id uuid NOT NULL REFERENCES session_operations(id),
    attempt_number integer NOT NULL CHECK (attempt_number >= 1),
    phase text NOT NULL CHECK (phase IN ('submission', 'result_retrieval', 'cancellation', 'withdrawal')),
    lease_token uuid NOT NULL,
    status text NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'succeeded', 'failed', 'unknown')),
    http_status integer,
    error jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (operation_id, attempt_number),
    CHECK ((status = 'running') = (finished_at IS NULL))
);

CREATE TABLE session_waits (
    id uuid PRIMARY KEY,
    session_id uuid NOT NULL REFERENCES sessions(id),
    source_event_id uuid NOT NULL,
    resolution_mode text NOT NULL CHECK (resolution_mode IN ('external', 'expiration', 'either')),
    payload json NOT NULL,
    response_schema json,
    expires_at timestamptz,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'resolved', 'expired', 'cancelled')),
    resolution_payload json,
    resolution_idempotency_key text,
    resolution_hash text,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (session_id, id),
    FOREIGN KEY (session_id, source_event_id) REFERENCES session_events(session_id, id),
    CHECK (
        (resolution_mode = 'external' AND expires_at IS NULL)
        OR (resolution_mode IN ('expiration', 'either') AND expires_at IS NOT NULL)
    ),
    CHECK (resolution_mode <> 'expiration' OR response_schema IS NULL),
    CHECK ((resolution_idempotency_key IS NULL) = (resolution_hash IS NULL)),
    CHECK (
        (status = 'pending' AND finished_at IS NULL AND resolution_payload IS NULL)
        OR (status = 'resolved' AND finished_at IS NOT NULL AND resolution_payload IS NOT NULL)
        OR (status IN ('expired', 'cancelled') AND finished_at IS NOT NULL)
    )
);

CREATE INDEX session_waits_expiration_idx ON session_waits (expires_at, id)
    WHERE status = 'pending' AND resolution_mode IN ('expiration', 'either');

ALTER TABLE session_events ADD CONSTRAINT session_events_wait_fk
    FOREIGN KEY (session_id, wait_id) REFERENCES session_waits(session_id, id);

CREATE TABLE gateway_callback_receipts (
    id uuid PRIMARY KEY,
    gateway_connection_id text NOT NULL,
    gateway_event_id uuid NOT NULL,
    gateway_job_id uuid NOT NULL,
    payload json NOT NULL,
    operation_id uuid,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'processed', 'blocked')),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_expires_at timestamptz,
    last_error jsonb,
    received_at timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    UNIQUE (gateway_connection_id, gateway_event_id),
    FOREIGN KEY (gateway_connection_id, gateway_job_id, operation_id)
        REFERENCES session_operations(gateway_connection_id, gateway_job_id, id),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((status = 'processed') = (processed_at IS NOT NULL))
);

CREATE INDEX gateway_callback_receipts_work_idx
    ON gateway_callback_receipts (next_attempt_at, received_at, id)
    WHERE status IN ('pending', 'processing');
CREATE INDEX gateway_callback_receipts_expired_lease_idx
    ON gateway_callback_receipts (lease_expires_at)
    WHERE lease_token IS NOT NULL;
