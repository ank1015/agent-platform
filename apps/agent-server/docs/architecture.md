# Server architecture

## Boundaries and components

The server is one Rust application composed from five workspace crates:

| Crate / module | Responsibility |
| --- | --- |
| `apps/agent-server` | HTTP routes, trusted-backend authentication, signed callback admission, configuration, process supervision, readiness, metrics |
| `packages/agent-contracts` | Harness interface; session, event, message, operation, and wait types |
| `packages/agent-store` | PostgreSQL schema, repositories, claim and commit transactions, inspection queries, durable application updates |
| `packages/agent-runtime` | Session scheduler, gateway dispatcher, result completion, wait expiration, operation-request cleanup |
| `packages/agent-gateways` | Configured LLM/execution HTTP clients and callback signature verification |

The backend calling this service authenticates its own users and decides which
`project_id` each session belongs to. The agent server only authenticates that
backend as a service; it has no end-user accounts or project authorization logic.
Gateway credentials and callback secrets belong to server configuration, not to
the harness or session.

## Session and event flow

```text
trusted backend                PostgreSQL                 runtime workers
    | POST session/input            |                            |
    |------------------------------>| persist session/event      |
    |<------ 201 / 202 -------------|                            |
    |                               |<--- claim head event -------|
    |                               |    harness handles one      |
    |                               |<--- atomic outcome commit --|
    |                               |    operations / waits       |
    |                               |<--- gateway result or wait -|
    |                               |    next completion event    |
    |                               |<--- claim next event -------|
```

Session creation selects a registered harness ID and version, validates its opaque
configuration, and saves initial harness state. Both harness identity/version and
configuration stay fixed for the lifetime of that session. The session status is
one of `idle`, `running`, `waiting`, `cancelling`, `cancelled`, or `failed`; it is
saved by the harness outcome, not inferred merely from an outstanding gateway job.
Name and metadata are mutable. There is no independent run or turn record.

Inputs (`user_message` and `cancellation_requested`) and internally produced
events (`operation_completed` and `wait_resumed`) share a strictly ordered,
per-session inbox. A later user message can be admitted while the handler or a
gateway operation is in progress. It queues durably; it is **not** delivered into
the current invocation. The scheduler permits only one authoritative handler
invocation for a session at a time. Each invocation receives exactly one event.
The harness uses saved state, history, and bounded context reads to decide how that
event changes its plan. The status of outstanding operations does not itself force
a new run or preempt the handler.

The handler returns a prepared outcome rather than writing tables directly. A
single transaction saves its new state and state version, conversation history
entries, session status, operation/wait actions, small application updates, and
acknowledgement of the input event. A stale state version or lost lease rejects the
whole commit. This is the central crash boundary: a crash before commit leaves the
event available for replay; a crash after commit leaves staged actions available to
the other workers.

## Why history, inbox, and application updates are separate

The event inbox is platform control flow: it identifies the next event to handle
and records delivery status and attempts. The conversation history holds messages
in the LLM contracts (`user`, `system`, `assistant`, `tool_result`, `custom`) and has
its own append sequence. A submitted user message becomes history only when the
harness outcome appends it. An assistant tool-call message therefore does not imply
that the tool has already executed; harness state and operation records determine
the next action.

The `session_updates` log is for the backend's chat page. It records small,
versioned, replayable changes such as `event.changed`, `history.appended`,
`operation.changed`, `wait.changed`, `session.changed`, `processing.changed`, and
`harness.progress`. A `harness.progress` update is committed with the handler
outcome; it is not streamed mid-invocation. Clients can inspect a referenced
history entry, event, or operation through the separate endpoints. Updates are not
the internal inbox and should not be interpreted as harness instructions.

## Gateway work and completion

Harness actions stage LLM, execution, cancellation, and withdrawal operations with
stable operation IDs. The outcome transaction stores one request payload per
operation in `session_operation_requests`; the dispatcher reads those committed
actions and records submission attempts. A
gateway admission can be accepted, definitively rejected, or uncertain (for
example, a lost HTTP response). Uncertainty retains the original request and
stable gateway idempotency identity so lookup or retry does not create a second
logical job. A definite rejection is delivered to the harness as a failed
operation outcome, even if the gateway never created a job.

Callbacks are authenticated against the **raw body** and persisted as deduplicated
receipts. They are hints, not authoritative results. A callback can arrive before
the accepted job mapping is stored; the completion worker later reconciles it.
Without a callback, the worker also performs fallback checks. It retrieves the
gateway's authoritative result and atomically saves an operation outcome plus one
`operation_completed` event. Execution `unknown` remains distinct from failure.
Starting an execution process does **not** automatically observe that process;
observation and controls are explicit harness-selected actions.

## Waits and cancellation

Waits are durable harness actions in modes `external`, `expiration`, or `either`.
An `external` wait accepts an authenticated backend resolution. An `expiration`
wait resolves at its deadline without external input. An `either` wait has exactly
one winner: an external reply or the deadline. Resolution, expiration, or harness
cancellation atomically finishes the wait and appends one `wait_resumed` event.
The handler receives that event on a later invocation. An optional JSON Schema
validates external replies before admission.

`POST /cancel` admits a `cancellation_requested` event; it does not directly kill
every gateway job. The harness decides how to update its state, cancel waits,
withdraw unsubmitted operations, request cancellation of accepted LLM work, and
issue execution controls. Late accepted results stay correlated with their
operations and do not implicitly reactivate a cancelled session.

## Claims, retries, and recovery

Scheduler, dispatcher, and result workers use PostgreSQL claims and leases rather
than assuming process-local ownership. The session handler renews its lease while
running. A second claimant cannot commit against the old lease token and state
version. Abandoned claims become eligible after lease expiry. Worker pools are
bounded by per-process handler, dispatch, and result limits. The scheduler moves a
recently touched session back in selection order so one busy session does not
monopolize capacity; result and dispatch work rotate their phases. Wait and request
cleanup sweeps use bounded keyset scans and revisit skipped locked candidates.

Handler errors retry the same head event under a configured attempt budget.
Infrastructure errors and safe transaction retries are treated separately from
harness failure. Exhausting the handler-error budget or an invalid outcome blocks
processing **for that session**, without discarding the inbox or changing already
committed work. An operator can inspect the blocked event, processing revision,
and attempts, then use the guarded, idempotent `/processing/retry` endpoint. There
is no API for arbitrary state replacement or manually acknowledging events.
General enforced handler execution deadlines remain deferred; harnesses should
cooperate with stop requests and ownership loss.

## Process lifecycle and retention

The HTTP server supervises the scheduler, dispatcher, completion worker, wait
expiration worker, operation-request cleanup worker, and update cleanup loop. On
SIGTERM/Ctrl-C, or when a required worker/server terminates unexpectedly, readiness
switches to draining, new authenticated API and callback work receives `503`, SSE
subscriptions end, and workers receive cancellation. The process waits up to the
configured shutdown grace. An unfinished handler remains recoverable after its
lease expires; a timed-out shutdown returns an error.

Operation requests are physically removed only after a terminal outcome and their
assigned expiration. Pending, submitting, accepted, and uncertain work retains
its request even past the nominal deadline. Operation identity, attempts, mapping,
and outcome remain after request removal. Application updates are retained for
seven days by default and then pruned in small batches; an expired update cursor
requires a new snapshot. Sessions, history, events, and operation outcomes have no
automatic retention in this version.

For the current table relationships and commit boundaries see
[persistence](persistence.md). Concrete worker settings are in
[configuration](configuration.md).
