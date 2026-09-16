# Agent server HTTP API (implemented v1)

This is the human-readable specification of the routes in
[`src/routes/`](../src/routes/). JSON field names are `snake_case` unless an
embedded gateway/LLM contract says otherwise. All IDs in path parameters are
UUIDs except `{harnessId}`, `{version}`, and `{connectionId}`. Timestamps are
UTC RFC 3339 strings. Unknown fields are rejected for the request envelopes
described below.

## Common rules

The existing backend is the API caller. It authenticates end users and sends
`Authorization: Bearer <service-token>` on **every `/v1` route except gateway
callbacks**. Multiple service tokens can be configured for rotation. Project
filtering does not enforce authorization. Gateway callbacks authenticate with
their own signed headers. `/healthz` and `/readyz` do not require a bearer token.

Ordinary JSON requests need `Content-Type: application/json`; callback admission
checks its signed raw body instead of using the JSON request extractor. The
default body limit is 2 MiB (configurable). Every response includes a
server-generated `x-request-id`.
Errors are JSON:

```json
{
  "error": {
    "code": "invalid_request",
    "message": "human-readable explanation",
    "request_id": "00000000-0000-0000-0000-000000000000"
  }
}
```

The header and body request IDs match. Typical codes are `401 unauthorized`,
`404 *_not_found`, `409 idempotency_conflict` or a stale precondition, `410
resnapshot_required`/`request_payload_expired`, `422 invalid_message`/
`invalid_configuration`/`invalid_wait_response`, and `503 not_ready`/
`shutting_down`/`database_unavailable`. Malformed JSON yields `400 invalid_json`;
unsupported media type and oversized bodies yield `415` and `413`. Failed input
admission does not create an event.

The admission endpoints return `202 Accepted` after a transaction has persisted
the input, **not** after harness execution. They require `Idempotency-Key`: 1–200
bytes, no whitespace. Reusing a key for the same route/event type and session with
the identical fingerprinted JSON returns the original acknowledgement; reuse
with different bytes returns `409 idempotency_conflict`. Message/cancel admission
fingerprints the raw envelope, wait resolution fingerprints the raw `response`
value, and processing retry fingerprints the raw body. Insignificant whitespace
inside the fingerprinted value can still conflict. Use a stable key and resend
the same serialization.

## Route inventory

| Group | Method and path | Purpose |
| --- | --- | --- |
| Harnesses | `GET /v1/harnesses` | List registered harness IDs |
| | `GET /v1/harnesses/{harnessId}/versions` | List registered versions |
| | `GET /v1/harnesses/{harnessId}/versions/{version}` | Describe a version and configuration schema |
| Sessions | `POST /v1/sessions` | Create an idle session |
| | `GET /v1/sessions` | List/filter sessions |
| | `GET /v1/sessions/{sessionId}` | Read detail and bootstrap cursors |
| | `PATCH /v1/sessions/{sessionId}` | Change name/metadata only |
| Inputs | `POST /v1/sessions/{sessionId}/messages` | Admit a user-message event |
| | `POST /v1/sessions/{sessionId}/cancel` | Admit a cancellation event |
| Waits | `GET /v1/sessions/{sessionId}/waits` | List waits |
| | `GET /v1/sessions/{sessionId}/waits/{waitId}` | Read wait detail |
| | `POST /v1/sessions/{sessionId}/waits/{waitId}/resolve` | Supply external wait response |
| History | `GET /v1/sessions/{sessionId}/history` | Incremental conversation entries |
| | `GET /v1/sessions/{sessionId}/history/{entryId}` | Read one entry |
| Inspection | `GET /v1/sessions/{sessionId}/operations` | List light operation records |
| | `GET /v1/sessions/{sessionId}/operations/{operationId}` | Operation outcome and retention detail |
| | `GET /v1/sessions/{sessionId}/operations/{operationId}/request` | Fetch retained request payload |
| | `GET /v1/sessions/{sessionId}/operations/{operationId}/attempts` | Submission/control/result attempts |
| | `GET /v1/sessions/{sessionId}/events` | List internal events |
| | `GET /v1/sessions/{sessionId}/events/{eventId}` | Event payload and failure count |
| | `GET /v1/sessions/{sessionId}/events/{eventId}/attempts` | Handler attempts |
| Updates | `GET /v1/sessions/{sessionId}/updates` | Replayable application changes |
| | `GET /v1/sessions/{sessionId}/updates/stream` | Server-sent event feed |
| Recovery | `POST /v1/sessions/{sessionId}/processing/retry` | Retry an exact blocked head event |
| Callbacks | `POST /v1/callbacks/llm/{connectionId}` | Signed LLM notification |
| | `POST /v1/callbacks/execution/{connectionId}` | Signed execution notification |
| Service | `GET /healthz` | Process liveness |
| | `GET /readyz` | Schema/database readiness and admission |
| | `GET /v1/metrics` | Authenticated Prometheus-format metrics |

There are no public endpoints for runs, dynamic harness installation, arbitrary
session-state writes, direct event acknowledgement, or direct operation submission.

## Harness discovery

`GET /v1/harnesses` returns `{ "data": [{ "id", "name", "description" }] }`.
`GET /v1/harnesses/{harnessId}/versions` returns
`{ "data": [{ "version", "name", "description" }] }` and returns `404` for an
unknown ID. The version detail returns `{ "id", "version", "name",
"description", "configuration_schema" }`, where the schema is advertised JSON
Schema. Session creation still runs the harness's authoritative semantic
validation. The production registry currently includes `basic-codex` version
`1`.

## Sessions

`POST /v1/sessions` accepts a caller-generated UUID and opaque harness config:

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "project_id": "project-42",
  "harness_id": "example",
  "harness_version": "1",
  "configuration": {"model": "example-model"},
  "name": "First session",
  "metadata": {}
}
```

`id`, `project_id`, `harness_id`, `harness_version`, and `configuration` are
required. `name` is optional/null; `metadata` defaults to `{}` and must be an
object. Project ID is 1–300 bytes, harness ID/version 1–100 bytes, and name at
most 500 bytes. Creation returns `201`, a `Location` header, and session detail.
An existing session ID returns `409 session_already_exists`; an unregistered
version returns `404 harness_version_not_found`; rejected config returns `422
invalid_configuration`. Creation initializes state but does not invoke `handle`.

`GET /v1/sessions` accepts `project_id`, `status`, `harness_id`, `cursor`, and
`limit` (default 50; 1–100). Status is `idle`, `running`, `waiting`, `cancelling`,
`cancelled`, or `failed`. Results are ordered by `(created_at,id)` descending:
`{ "data": [session summaries], "next_cursor": "..." | null }`. The cursor is
an opaque URL-safe base64 token; pass it unchanged on the next request. A list
summary has `id`, `project_id`, `harness_id`, `harness_version`, `name`,
`metadata`, `status`, `state_version`, `processing_health`, `created_at`, and
`updated_at`. `processing_health` has `enabled`, `error`, `revision`, and
`blocked_event_id`. This is processing health, separate from activity status.

`GET /v1/sessions/{sessionId}` adds `configuration`,
`history_through_sequence`, and `update_through_sequence` to the summary. These
cursors let a backend bootstrap incremental history and application updates.
Serialized harness **state is not exposed** by this route.

`PATCH /v1/sessions/{sessionId}` accepts `name` and/or `metadata` and returns
session detail. `name: null` clears it; omitted name leaves it unchanged.
`metadata` must be an object and replaces the prior object; null is invalid.
At least one field is required. Harness identity/version, configuration,
`project_id`, state, and status cannot be patched.

## Durable inputs and waits

`POST /v1/sessions/{sessionId}/messages` accepts
`{ "message": <user-message> }`. The embedded message follows the LLM message
contract; only role `user` is accepted. Text parts and HTTP(S) image URL parts
are supported. A minimal body is:

```json
{"message":{"role":"user","content":[{"type":"text","text":"Hello"}]}}
```

`POST /v1/sessions/{sessionId}/cancel` accepts `{}` or
`{ "reason": "optional explanation" }`. Both endpoints require
`Idempotency-Key` and return `{ "session_id", "event_id", "event_sequence",
"accepted_at" }` with `202`. Cancellation is delivered to the harness as an
event; it does not itself cancel every in-flight gateway job.

`GET /v1/sessions/{sessionId}/waits` accepts `status`, `cursor`, and `limit`
(default 50; 1–100). Status is `pending`, `resolved`, `expired`, or `cancelled`.
It returns `{ "data": [wait detail], "next_cursor": "..." | null }`, ordered
newest first by `(created_at,id)`. `GET /waits/{waitId}` returns one wait in the
specified session. A wait includes `id`, `session_id`, `source_event_id`,
`resolution_mode` (`external`, `expiration`, `either`), opaque `payload`,
optional `response_schema`, optional `expires_at`, `status`, optional
`resolution`, and creation/finish timestamps.

`POST /v1/sessions/{sessionId}/waits/{waitId}/resolve` accepts
`{ "response": <any JSON value> }` and `Idempotency-Key`. If the wait has a
stored JSON Schema, the response must match it (`422 invalid_wait_response`).
The resolution and a `wait_resumed` event are committed together. A valid request
returns `202` with the normal event acknowledgement plus `wait_id`. Only an
externally resolvable pending wait can accept a new reply; an expiration or a
competing reply can win first (`409 wait_not_resolvable`). Repeating the original
idempotent reply returns its prior acknowledgement.

## History and inspection

`GET /v1/sessions/{sessionId}/history` takes `after_sequence` (exclusive,
default 0), `through_sequence` (inclusive snapshot bound, default current),
`limit` (default 100; 1–999), and optional `source_event_id`. It returns
`{ "data", "through_sequence", "next_after_sequence", "has_more" }`. Keep
the same `through_sequence` across pages for a stable read, advance
`after_sequence` to `next_after_sequence`, then open a new snapshot for later
entries. Each entry has `id`, `session_id`, `sequence`, `source_event_id`, `role`,
full `message`, and `created_at`. `GET /history/{entryId}` retrieves that entry.

`GET /v1/sessions/{sessionId}/operations` accepts `cursor`, `limit` (default
50; 1–100), `kind` (`llm`, `execution`, `llm_cancellation`, `withdraw`),
`status`, and `source_event_id`. The response is `{ "data", "next_cursor",
"has_more" }`, newest first. Operation status is `pending`, `submitting`,
`accepted`, `succeeded`, `failed`, `cancelled`, or `unknown`. List items omit
large requests/results, but include IDs, gateway mapping, target/previous IDs,
timestamps, and `request_retention` (`available`,
`eligible_for_cleanup_at`, `removed_at`). Detail adds `result`, `error`, and
`request_hash`. The `/request` subresource returns `{ "request": <JSON> }` while
retained, or `410 request_payload_expired` after cleanup. The operation and its
outcome remain inspectable after cleanup. `/attempts` returns paginated operation
attempt records. Each has `id`, `operation_id`, `attempt_number`, `phase`,
`status`, optional `http_status`/`error`, `started_at`, and `finished_at`.

`GET /v1/sessions/{sessionId}/events` takes `after_sequence` (exclusive,
default 0), `limit` (default 50; 1–100), and `status` (`pending`,
`processing`, `handled`, `blocked`). It returns `{ "data", "next_cursor",
"has_more" }` in ascending event sequence; the returned `next_cursor` is a
**sequence string**, passed as the next `after_sequence`, not as an opaque
`cursor` query value. List items omit payload. Detail adds `payload` and
`current_failure_count`. Event types are `user_message`,
`operation_completed`, `cancellation_requested`, and `wait_resumed`.
`/events/{eventId}/attempts` returns paginated handler attempts. Each has `id`,
`event_id`, `attempt_number`, `status`, `input_state_version`,
`history_through_sequence`, optional `error`, `started_at`, and `finished_at`.

Both attempt-list endpoints accept `after_attempt` (exclusive number, default 0)
and `limit` (default 50; 1–100). Their `next_cursor`, when present, is the last
attempt number as a string; pass it back as `after_attempt`. All nested detail
reads verify that the object belongs to the path's session.

## Application updates and SSE

`GET /v1/sessions/{sessionId}/updates` takes `after_sequence` (exclusive,
default 0), optional `through_sequence`, and `limit` (default 100; 1–999).
It returns `{ "data", "through_sequence", "retained_sequence",
"next_after_sequence", "has_more" }`. Each update has `session_id`,
`sequence`, `schema_version`, `kind`, opaque `payload`, optional
`source_event_id`, and `created_at`. `retained_sequence` is the earliest still
replayable boundary. A cursor older than retention returns `410
resnapshot_required`; fetch session detail and rehydrate the page.

`GET /v1/sessions/{sessionId}/updates/stream` is a bearer-authenticated
`text/event-stream`. Supply **either** `after_sequence` **or** `Last-Event-ID`,
not both. `through_sequence` and `limit` are not accepted. Each SSE record uses
the durable update sequence as `id`, update `kind` as `event`, and the JSON update
item as `data`; the server also sends 15-second keepalive comments and a 1-second
retry hint. Reconnect with the last received ID. If retention expires after a
subscription begins, it emits `resnapshot_required` and ends. The stream polls
for new durable updates, so it is replayable after a server restart. The
per-process subscription limit returns `429 too_many_streams`; streams end during
shutdown draining.

## Blocked-session recovery

`POST /v1/sessions/{sessionId}/processing/retry` requires `Idempotency-Key` and:

```json
{
  "expected_event_id": "22222222-2222-4222-8222-222222222222",
  "expected_processing_revision": 1,
  "reason": "operator retry"
}
```

The reason is optional (at most 1000 bytes). The event ID and nonnegative
revision must exactly match the blocked head shown in session detail, and no
active handler lease may own it. It returns `202` with `session_id`, `event_id`,
new `processing_revision`, and `retried_at`. A stale head/revision returns `409
stale_processing_retry`. This resets that event's handler-error budget without
rewriting saved state, history, or previously committed operations. Recovery is
durably audited and the endpoint is idempotent for an identical retry body/key.

## Signed callbacks, probes, and metrics

Gateway callbacks use the configured connection ID and kind, not the service
bearer token. Their headers are:

- LLM: `x-llm-gateway-event-id`, `x-llm-gateway-timestamp`,
  `x-llm-gateway-signature`.
- Execution: `x-execution-gateway-event-id`,
  `x-execution-gateway-timestamp`, `x-execution-gateway-signature`.

The timestamp is Unix seconds. The signature is `v1=<hex HMAC-SHA256>` over
`<timestamp>.<event-id>.<raw UTF-8 body>`, using one configured secret and the
connection's 5-minute default tolerance. The body event ID must match its
header. LLM notifications accept `job.succeeded`, `job.failed`, and
`job.cancelled`; execution notifications require schema version 2 and accept
`job.succeeded`, `job.failed`, and `job.unknown`. The notification bodies have
these shapes (UUID strings and RFC 3339 timestamps substituted):

```json
{"eventId":"33333333-3333-4333-8333-333333333333","type":"job.succeeded","jobId":"44444444-4444-4444-8444-444444444444","completedAt":"2026-01-01T00:00:00Z"}
```

```json
{"schemaVersion":2,"eventId":"33333333-3333-4333-8333-333333333333","type":"job.succeeded","jobId":"44444444-4444-4444-8444-444444444444","machineId":"55555555-5555-4555-8555-555555555555","completedAt":"2026-01-01T00:00:00Z"}
```

The first is LLM and the second is execution. A verified, durably recorded receipt
returns `202` with no result body, including on a duplicate. An invalid signature
returns `401 invalid_callback_signature`; unknown/mismatched connection returns
`404 callback_connection_not_found`. Receipt admission does not wait for
authoritative result retrieval or a harness invocation.

`GET /healthz` returns `{ "status": "ok" }` for process liveness. `GET
/readyz` returns `{ "status": "ready" }` only while accepting work and the
database has the exact expected schema; otherwise `503 not_ready`. It does not
probe every gateway account or machine. `GET /v1/metrics` requires the service
bearer token and returns Prometheus text with HTTP request counts/latency,
per-process live SSE subscriptions, database-wide pending work and leases,
overdue waits, and cleanup counts.
