# Agent platform: API endpoint inventory

This document records the grouped endpoint discussion following
[System working](system_working.md),
[Detailed design decisions](system_design_decisions.md), and
[Proposed database schemas](schemas.md).

The inventory records the implemented API through Part 10 and remaining design
direction. It is not a finalized OpenAPI specification. History, inspection, and
replayable SSE updates are implemented; deployment choices remain open.

## Shared API boundaries

- Trusted backends call the `/v1` APIs using service-level authentication.
- The existing backend owns end-user authentication and authorization. The platform
  does not introduce local user accounts or a separate end-user ownership model.
- Gateway callbacks use the corresponding gateway's signature verification rather
  than ordinary backend-call authentication.
- Health endpoints are service probes; their exposure policy remains to be finalized.
- A session carries a backend-supplied `project_id`. Filtering by this field is
  grouping, not an end-user authorization mechanism.
- There is no separate run resource. Inputs make an existing session eligible for
  processing.
- Durable acceptance, handler processing, and external-operation completion are
  separate milestones.
- Exact naming conventions for request/query fields remain open. Examples below use
  the names discussed so far.

For implemented endpoints, fields use `snake_case`, every `/v1` route requires a
service bearer token, and errors have the shape
`{"error":{"code","message","request_id"}}`. The request ID is also returned in
`x-request-id`. Session IDs are caller-supplied UUIDs; a duplicate returns `409`.

## 1. Harness discovery

These proposed endpoints let a backend discover the harness implementations that
the server can instantiate and the configuration required by each version.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/harnesses` | List available harnesses |
| `GET` | `/v1/harnesses/{harnessId}/versions` | List registered versions |
| `GET` | `/v1/harnesses/{harnessId}/versions/{version}` | Describe one registered version and its configuration schema |

Version descriptions may include a display name, description, and configuration
schema. Session creation still performs authoritative validation, including semantic
checks that are not fully described by the schema.

Harnesses are imported and registered through server code and deployment. There are
no initial endpoints for creating, installing, or dynamically loading harness code.

## 2. Session management

These endpoint paths are agreed.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/sessions` | Create an idle session with a harness/version and immutable configuration |
| `GET` | `/v1/sessions` | List sessions |
| `GET` | `/v1/sessions/{sessionId}` | Read session details |
| `PATCH` | `/v1/sessions/{sessionId}` | Update permitted mutable metadata |

### Creation

The caller selects the harness and version, supplies configuration and project
identity, and may provide a name and metadata. The platform validates configuration
and initializes saved state. External work begins through later inputs.

The harness identity/version and configuration remain fixed for the session's
lifetime. Creation payload details and creation-idempotency behavior remain open.

### Listing and detail

The implemented list filters are:

- `project_id`
- `status`
- `harness_id`
- Cursor and page size

For example:

```text
GET /v1/sessions?project_id=...
```

List responses should be lightweight and omit large configuration/state payloads.
Session detail can include configuration and processing-health information. Raw
harness state is not proposed as part of the ordinary detail response; a diagnostic
facility could expose it separately if needed.

Activity statuses are `idle`, `running`, `waiting`, `cancelling`, `cancelled`, and
`failed`. These are distinct from whether a worker is currently executing a handler
or an operational fault has paused processing.

### Metadata updates

PATCH supports only `name` and `metadata`. Omitting a field leaves it unchanged,
`name: null` clears the name, and supplied metadata must be an object and replaces
the complete previous metadata object. Project, harness identity/version,
configuration, state, activity, and processing fields are immutable through this
endpoint.

Deletion and archiving have not been defined. Do not add those endpoints without
settling their effects on pending events, operations, waits, and retained history.

## 3. Session inputs

These endpoint paths are agreed.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/sessions/{sessionId}/messages` | Durably submit a user message |
| `POST` | `/v1/sessions/{sessionId}/cancel` | Durably request cancellation |

### Acceptance and idempotency

Both return an acknowledgement identifying the accepted event. They do not wait for
the harness to finish processing it.

Both require an `Idempotency-Key` header containing 1–200 bytes with no whitespace.
Idempotency is scoped by session and event type. Repeating the same key and exact
request bytes returns the original acknowledgement; changing the request bytes
under that key returns `409 idempotency_conflict`. The same key may therefore be
used once for a message and once for a cancellation request in one session.

The message body is `{"message": <user message>}` using the conversation contract.
Other message roles, unknown message/content fields, and image values that are not
valid HTTP(S) URLs return `422 invalid_message`. The cancellation body is
`{"reason": <optional string>}`. Unknown request-envelope fields are rejected.

Successful admission returns `202`:

```json
{
  "session_id": "...",
  "event_id": "...",
  "event_sequence": 1,
  "accepted_at": "..."
}
```

Accepting a message records an input event. Conversation history is appended when
the harness incorporates the input in a committed outcome.

### Processing semantics

Events can be accepted while a handler is executing, but are delivered to later
invocations. One event is handled per invocation, with one authoritative handler at
a time per session.

Cancellation is delivered as an event. Acknowledgement does not mean the current
handler has stopped or external operations have been cancelled. The harness decides
how to stop its work and perform any required cleanup.

Admission does not change session activity, harness state, state version, or
conversation history. It remains available in all six activity statuses and when
processing is paused. A later scheduler part claims the persisted events.

There are no separate `/start` or `/run` endpoints. There is also no generic `/resume`
endpoint: an external reply resolves a specific wait through its resolution endpoint.

## 4. Conversation history and live updates

### History

History retrieval and incremental reads are implemented:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/sessions/{sessionId}/history` | Read paginated conversation history, including entries after a sequence |
| `GET` | `/v1/sessions/{sessionId}/history/{entryId}` | Retrieve one conversation entry |

Example incremental read:

```text
GET /v1/sessions/{sessionId}/history?after_sequence=42&through_sequence=120&limit=100
```

Each appended entry has an increasing per-session history sequence. Its message
follows the existing conversation contract: `user`, `assistant`, `tool_result`,
`system`, or `custom`.

The list returns `data`, captured `through_sequence`, `next_after_sequence`, and
`has_more`. Entries are ascending and include ID, sequence, source event ID, role,
full message, and timestamp. The first page captures the current history boundary;
subsequent pages can reuse it while the harness appends newer entries. Optional
`source_event_id` filters entries produced by one input. `limit` is 1 through 999
(default 100). Entry detail is scoped to its session. Admitted user input becomes
conversation history only if the harness appends it.

This proposal assumes append-only history. Edits or deletions would require change
revision semantics beyond an append sequence.

### Live updates

The update resource is distinct from the internal harness inbox:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/sessions/{sessionId}/updates` | Retrieve persisted UI updates after a cursor |
| `GET` | `/v1/sessions/{sessionId}/updates/stream` | Subscribe through SSE with reconnection support |

JSON reads use `after_sequence`, optional `through_sequence`, and `limit` 1 through
999 (default 100). Responses contain `data`, `through_sequence`,
`retained_sequence`, `next_after_sequence`, and `has_more`. Each update carries a
per-session sequence, kind, schema version 1, timestamp, small payload, and optional
source event ID. Kinds are `session.changed`, `history.appended`,
`operation.changed`, `wait.changed`, `event.changed`, `processing.changed`, and
`harness.progress`. Resource change updates reference IDs or ranges; they do not
copy complete messages or gateway payloads. Harness progress is staged in an
outcome, bounded to 20 records of at most 2048 bytes each, and visible after commit.
Operation updates cover submission, acceptance, retry/error, deferral, completion,
and request-payload removal; unchanged result checks and lease renewals do not
generate feed records.

SSE replays the same records in sequence order. Its `id` is the update sequence;
reconnection supplies `Last-Event-ID`, or a first subscription uses
`after_sequence`. Sending both is invalid. The stream polls durable storage,
provides keepalive comments, and reconnects safely across server restarts. A
cursor older than the retained boundary returns `410 resnapshot_required` on
JSON or at SSE admission; if retention overtakes an active stream it emits
`resnapshot_required` and closes. The default update retention is seven days.
The application should reload session detail and history after this response.

Session detail exposes `update_through_sequence` and
`history_through_sequence`. The backend can capture a cursor before loading its
page resources and then replay subsequent updates to cover changes during load.
Creation returns cursor zero; metadata PATCH returns a cursor captured before the
patch. These may replay a change already present in the response, but cannot skip
a committed change absent from its snapshot.

A client displaying only a saved conversation can use session detail and history
without subscribing to live updates.

## 5. Waits

These endpoint paths are agreed.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/sessions/{sessionId}/waits` | List waits, optionally filtered by status |
| `GET` | `/v1/sessions/{sessionId}/waits/{waitId}` | Read a wait's request and current outcome |
| `POST` | `/v1/sessions/{sessionId}/waits/{waitId}/resolve` | Submit an external resolution |

The list accepts `status=pending|resolved|expired|cancelled`, `limit` from 1 through
100 (default 50), and an opaque `cursor`. With no status it returns all waits ordered
newest first. The response is `{"data": [...], "next_cursor": ...}`. Detail and
resolution both scope the wait ID to the session path.

Resolution requires `Idempotency-Key` and a `{"response": <any JSON>}` body; explicit
JSON `null` is valid. The platform validates the response against the wait's optional
Draft 2020-12 JSON Schema, with local references only, then atomically resolves the
pending wait and records one resumption event. Schema mismatch returns
`422 invalid_wait_response`. Invalid stored schemas are an internal error.

A successful resolution returns `202` with the ordinary event acknowledgement plus
`wait_id`. Its response acknowledges resolution; harness processing remains
asynchronous.

A reply racing with expiration has one authoritative outcome using the database wall
clock after row locking. A late reply, an expiration-only wait, or any other
non-resolvable state returns `409 wait_not_resolvable`. An exact retry of an accepted
response returns the original acknowledgement even after its former deadline;
another key or different response returns `409 idempotency_conflict`.

An ordinary message does not implicitly resolve a wait. Resolution targets the
specific wait ID.

There is no initial public wait-creation endpoint. Harnesses create waits through
the outcome builder. Whether callers need an individual wait-cancellation endpoint
remains an open behavioral decision.

## 6. Inspection and operational recovery

These authenticated diagnostic endpoints are implemented. Lists are bounded and
omit full requests, results, and input payloads.

### Operation inspection

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/sessions/{sessionId}/operations` | Inspect platform operations |
| `GET` | `/v1/sessions/{sessionId}/operations/{operationId}` | Read an operation and its result/error |
| `GET` | `/v1/sessions/{sessionId}/operations/{operationId}/request` | Read its retained request payload |
| `GET` | `/v1/sessions/{sessionId}/operations/{operationId}/attempts` | Inspect gateway communication attempts |

Operation lists accept `kind`, `status`, `source_event_id`, `cursor`, and `limit`
1 through 100 (default 50). They use newest-first opaque cursor pagination and
include `request_retention.available`, cleanup eligibility, and removal time.
Detail returns the complete retained result/error. Request detail returns
`410 request_payload_expired` after physical cleanup; operation identity, mapping,
and outcome remain available. Attempts use `after_attempt` and `limit`.

Operation attempts describe platform-to-gateway communication. They are different
from the LLM gateway's provider attempts.

Inspection must preserve the distinction between start-operation completion and
process completion. The platform does not automatically observe a running command;
subsequent observations are explicit harness-requested operations.

### Internal event inspection

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/v1/sessions/{sessionId}/events` | Inspect the internal inbox |
| `GET` | `/v1/sessions/{sessionId}/events/{eventId}` | Check whether an accepted input was handled |
| `GET` | `/v1/sessions/{sessionId}/events/{eventId}/attempts` | Inspect handler invocation attempts |

Event lists accept `after_sequence`, `status`, and `limit` 1 through 100 (default
50), return ascending summaries, and omit input payloads. Detail includes the
input payload and failure count in the current recovery generation. Handler
attempts use `after_attempt` and `limit`. Event records can change status without
acquiring a new inbox sequence; the UI update cursor reports those changes.

### Blocked-session recovery

Implemented endpoint:

```text
POST /v1/sessions/{sessionId}/processing/retry
```

The trusted backend sends an `Idempotency-Key` header and a JSON body such as
`{"expected_event_id":"<uuid>","expected_processing_revision":1,"reason":"operator retry"}`.
The session detail/list response exposes the blocked head ID and revision under
`processing_health`. Only that exact blocked head with no active handler lease can
be retried. The same event becomes pending with a fresh handler-error budget; saved
state, history, activity status, and already committed gateway work remain unchanged.
An identical retry returns the original `202` acknowledgement, even after the
event later completes. A stale head/revision or reused key with different body
returns `409`. Recovery actions are durably audited.

There are no proposed public APIs for arbitrary state replacement, manually marking
events handled, or directly inserting session operations. Those bypass the harness
outcome and consistency rules.

## 7. Gateway callbacks and service health

### Callback endpoints

| Method | Proposed endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/v1/callbacks/llm/{connectionId}` | Receive signed LLM gateway notifications |
| `POST` | `/v1/callbacks/execution/{connectionId}` | Receive signed execution gateway notifications |

The connection identifier selects server-configured verification settings. Knowing
the identifier does not authorize a callback; the signature must verify against the
incoming raw body under the gateway's signing contract.

The platform durably records verified notifications and acknowledges receipt.
Acknowledgement does not wait for result retrieval or harness processing. Repeated
notifications are deduplicated using gateway connection and event identity.

Fast notifications arriving before the gateway job mapping is saved remain available
for reconciliation. Background result processing retrieves the authoritative result
and creates the session's operation-completion event.

Callback payloads, headers, and signatures follow the existing gateway contracts.
Execution callbacks use the lightweight version 2 payload; both callback types are
authoritative-result hints rather than embedded result delivery.

### Service probes

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness |
| `GET` | `/readyz` | Readiness to serve requests |
| `GET` | `/v1/metrics` | Authenticated Prometheus-format operational metrics |

Readiness needs a documented scope, such as database access and availability of
required schema. It should not imply that every LLM account, machine, or callback
destination is usable. Exact checks and response shapes remain open.

The metrics endpoint uses the ordinary backend bearer token. It reports HTTP
request counts and latency, live SSE subscriptions, pending work, active leases,
overdue waits, and operation-request cleanup status. The SSE connection limit is
configured per server process; a subscription over the limit receives `429`.

## Remaining API decisions

Further discussion should settle:

- Service authentication, credential rotation, and diagnostic/recovery access.
- Request/response shapes, error codes, validation, and HTTP statuses.
- Creation idempotency behavior.
- Project identifier format and reassignment policy.
- Pagination ordering, filters, cursor encoding, and response-size limits.
- Any future history edit/revision support.
- Whether more diagnostic filters or aggregate views are needed.
- Recovery endpoint preconditions and retry semantics.
- Wait cancellation behavior.
- Exact callback routing and health-probe contracts.

These are follow-up decisions beyond the implemented Part 10 API.
