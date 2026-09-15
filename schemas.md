# Agent platform: database schemas

This document records the database discussion following
[System working](system_working.md) and
[Detailed design decisions](system_design_decisions.md).

The table design is implemented as the initial migration in
`packages/agent-store/migrations`. This document remains the conceptual reference;
later API and lifecycle discussions may add forward migrations without rewriting
the applied initial migration.

The latest revision separates large operation requests into an expiring payload
table. It replaces the earlier proposal to retain both `request` and
`gateway_request` on every operation.

## 1. Scope and conventions

The initial migration has **nine tables**; Part 8 adds a retry audit table and
Part 9 adds an application update table:

| Table | Responsibility |
| --- | --- |
| `sessions` | Session identity, configuration, saved state, activity, and processing ownership |
| `session_events` | Ordered durable inbox |
| `session_event_attempts` | Individual handler invocation attempts |
| `session_history` | Conversation messages and incremental retrieval |
| `session_operations` | External operation identity, lifecycle, gateway mapping, and outcome |
| `session_operation_requests` | One large request payload per operation, with independent expiration |
| `session_operation_attempts` | Attempts to communicate with a gateway |
| `session_waits` | Durable waits and accepted resolutions |
| `gateway_callback_receipts` | Verified notifications awaiting mapping/result reconciliation |
| `session_processing_retries` | Idempotent, auditable operator retries of blocked head events |
| `session_updates` | Small, ordered, replayable UI change records and committed harness progress |

The Part 9 migration adds `next_update_sequence` and `retained_update_sequence`
to `sessions`. The first allocates a separate per-session UI cursor; the second
records the oldest replayable sequence after cleanup. `session_updates` stores
`session_id`, sequence, kind, schema version, optional source event ID, small JSON
payload, and creation time. Every update is inserted in the transaction that
changes its resource. Cleanup removes a contiguous expired prefix and advances
the retained boundary atomically; clients with older cursors must resnapshot.

`session_operations.request_removed_at` records physical removal of the separately
stored request payload. The request itself is still stored only once in
`session_operation_requests` while recovery or inspection needs it.

There are no proposed tables for users, projects, runs, or registered harnesses:

- The existing backend owns end-user authentication and authorization.
- Sessions carry the backend-supplied `project_id`.
- A session is the persistent agent instance; there is no separate run entity.
- Harnesses are registered in code by identity and version.
- Gateway connection credentials and signing secrets belong to server configuration.

General conventions:

- Platform identifiers use `uuid` primary keys.
- Timestamps use `timestamptz`.
- Per-session sequences and state versions use `bigint`.
- Status columns use `text` with check constraints matching the repository state
  transitions.
- Opaque messages, requests, results, and harness payloads use PostgreSQL `json`,
  following the LLM gateway's approach to preserving provider-native JSON.
- Metadata and sanitized errors can use `jsonb`.
- Fields used for ownership, ordering, scheduling, and correlation are ordinary
  columns rather than values extracted from large payloads.
- Foreign keys should enforce same-session relationships where applicable.
- Recovery-relevant records should not disappear through automatic cascading deletion.

Columns below are required unless marked nullable. The initial migration is the
authority for defaults and constraints.

## 2. `sessions`

### Need and use

One row is one persistent agent session. Session APIs read its identity and metadata;
workers use its saved state, state version, and processing claim to advance it.

The initial proposal keeps saved state and processing ownership on this row. Whether
to separate them later remains open. List endpoints should project lightweight
columns rather than loading configuration and state for every session.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Stable session identity |
| `project_id` | `text` | Project identifier supplied by the calling backend |
| `harness_id` | `text` | Registered harness implementation |
| `harness_version` | `text` | Pinned version interpreting configuration and state |
| `configuration` | `json` | Immutable harness-specific configuration |
| `name` | `text`, nullable | Display name |
| `metadata` | `jsonb` | Application-provided metadata |
| `status` | `text` | Session activity/outcome |
| `state` | `json` | Current saved harness state |
| `state_version` | `bigint` | Checkpoint version advanced by handler commits |
| `next_event_sequence` | `bigint` | Allocates ordered inbox positions |
| `next_history_sequence` | `bigint` | Allocates ordered history positions |
| `processing_enabled` | `boolean` | Scheduling switch independent of activity status |
| `processing_error` | `jsonb`, nullable | Operational fault preventing processing |
| `current_event_id` | `uuid`, nullable, FK | Currently claimed event |
| `lease_token` | `uuid`, nullable | Identity of the current processing claim |
| `lease_expires_at` | `timestamptz`, nullable | Expiration permitting recovery after worker loss |
| `created_at` | `timestamptz` | Creation time |
| `updated_at` | `timestamptz` | Last relevant record update |

The six activity statuses are `idle`, `running`, `waiting`, `cancelling`,
`cancelled`, and `failed`.

`processing_enabled` and `processing_error` distinguish an operational problem from
the harness deliberately returning a failed activity outcome. For example, a missing
harness version or repeatedly failing handler may require pausing scheduling.
The precise operational-fault representation and recovery API remain open.

### Constraints and indexes

- Configuration and harness identity/version are immutable under normal session APIs.
- The caller supplies the session UUID. Creation is not idempotent; a duplicate UUID
  violates the primary key.
- The metadata mutation replaces both `name` and the complete `metadata` object.
- A claimed event must belong to this session.
- Lease fields must be paired; claim consistency checks will be defined with the
  scheduling state machine.
- Sequence allocation uses a short row lock or equivalent atomic transaction.
- Event acceptance advances the event counter but does **not** advance
  `state_version`. Inputs arriving during a handler should not invalidate its state
  commit merely because the inbox changed.
- Listing index: `(project_id, created_at DESC, id DESC)`.
- Recovery index: active `lease_expires_at` values.

The repository does not expose project reassignment.

## 3. `session_events`

### Need and use

This is the durable inbox. A message accepted over HTTP remains here until a handler
successfully processes it, even if the accepting process disappears.

The scheduler selects events in per-session sequence order. Completion and wait
resumption events use the same inbox as user messages and cancellation requests.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Event identity |
| `session_id` | `uuid`, FK | Target session |
| `sequence` | `bigint` | Ordering within the session |
| `type` | `text` | Event category |
| `payload` | `json` | Event-specific input |
| `operation_id` | `uuid`, nullable, FK | Source operation for completion events |
| `wait_id` | `uuid`, nullable, FK | Source wait for resumption events |
| `idempotency_key` | `text`, nullable | Retry identity for externally submitted input |
| `request_hash` | `text`, nullable | Detects changed input using the same key |
| `status` | `text` | `pending`, `processing`, `handled`, `blocked` |
| `next_attempt_at` | `timestamptz` | Earliest processing/retry time |
| `last_error` | `jsonb`, nullable | Latest processing failure |
| `created_at` | `timestamptz` | Durable acceptance time |
| `handled_at` | `timestamptz`, nullable | Successful handler commit time |

Event categories are user message, operation completed, cancellation requested, and
wait resolver/resume. Suggested wire names remain `user_message`,
`operation_completed`, `cancellation_requested`, and `wait_resumed`.

### Constraints and indexes

- Unique `(session_id, sequence)`.
- External idempotency scope is `(session_id, type, idempotency_key)` for non-null
  keys. Its SHA-256 fingerprint covers the exact bytes of the raw JSON payload;
  semantically equal JSON with different serialization is not the same request.
- At most one logical completion event for each operation.
- At most one logical resumption event for each wait.
- Event type constrains which source reference is required or permitted.
- Source operation/wait must belong to the same session.
- Index the oldest unhandled events within a session for scheduling.

A completion payload can reference an immutable stored operation result instead of
duplicating a large output in the inbox.

With strict ordering, the scheduler examines the oldest unhandled event, including
its retry delay. A delayed or blocked head prevents later events in that session
from being claimed; it does not prevent another session from progressing.

## 4. `session_event_attempts`

### Need and use

One event may require several invocations after crashes or handler errors. This
table explains which checkpoint each invocation used and whether it committed.
It supports diagnostics and recovery, without introducing a separate agent run.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Attempt identity |
| `event_id` | `uuid`, FK | Input being processed |
| `attempt_number` | `integer` | Increasing number within the event |
| `lease_token` | `uuid` | Processing claim associated with the invocation |
| `input_state_version` | `bigint` | Saved-state checkpoint read by the handler |
| `history_through_sequence` | `bigint` | History boundary captured at invocation start |
| `status` | `text` | `running`, `committed`, `errored`, `abandoned` |
| `error` | `jsonb`, nullable | Sanitized failure details |
| `started_at` | `timestamptz` | Invocation start |
| `finished_at` | `timestamptz`, nullable | Attempt settlement time |

Unique `(event_id, attempt_number)` with positive attempt numbers.

`abandoned` means the invocation lost ownership or disappeared without committing.
It does not mean arbitrary direct external I/O had no effect.

The attempt stores the history boundary captured by the claim. There is no enforced
handler execution deadline initially; lease renewal continues while an invocation
owns the session.

## 5. `session_history`

### Need and use

Each row contains one conversation message. Harnesses query it to construct context,
and APIs expose incremental history to the calling application.

Messages follow the existing
[LLM message contract](../llm-providers/packages/contracts/src/messages.ts), including
opaque provider-native assistant content.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Platform history-entry identity |
| `session_id` | `uuid`, FK | Owning session |
| `sequence` | `bigint` | Incremental retrieval order |
| `source_event_id` | `uuid`, nullable, FK | Handler input responsible for the addition |
| `role` | `text` | Message role for lightweight inspection/filtering |
| `message` | `json` | Complete contract message |
| `created_at` | `timestamptz` | Persistence time |

Roles are `user`, `assistant`, `tool_result`, `system`, and `custom`. The repository
deserializes the raw message through `agent_contracts::Message` and derives the
stored role from that typed message.

### Constraints and use

- Unique `(session_id, sequence)` supports reads after a sequence:

  ```sql
  WHERE session_id = $1 AND sequence > $2
  ORDER BY sequence
  LIMIT $3
  ```

- The platform row ID is independent of an optional ID inside the message.
- A handler may append zero, one, or several entries in one committed outcome.
- Source events, when present, must belong to the same session.
- Nullable source events leave room for initial history if initialization is later
  allowed to produce it; that initialization contract is not yet settled.

The current proposal is append-only history. Editing/deleting entries would require
additional revision semantics for incremental clients.

## 6. `session_operations`

### Need and use

An operation is durable intent to perform gateway work, plus its lifecycle and
outcome. Its ID is returned immediately when the harness stages an action in its
outcome builder. Gateway submission occurs after the handler outcome commits.

This replaces the earlier generic `session_actions` name. Conversation additions
and activity changes commit directly; waits have their own table. Gateway requests
need independent dispatch, correlation, and result processing.

**Large request bodies are stored in `session_operation_requests`, not in this row.**

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Stable platform operation identity |
| `session_id` | `uuid`, FK | Owning session |
| `source_event_id` | `uuid`, FK | Handler input that requested the operation |
| `kind` | `text` | `llm`, `execution`, `llm_cancellation`, or `withdraw` |
| `gateway_connection_id` | `text` | Stable name of a server-configured gateway connection |
| `target_operation_id` | `uuid`, nullable, FK | Target for cancellation or related-operation reference |
| `previous_operation_id` | `uuid`, nullable, FK | Previous platform operation for an LLM continuation |
| `gateway_idempotency_key` | `text` | Stable submission identity generated from the operation ID |
| `request_hash` | `text` | Fingerprint retained independently of the large request |
| `gateway_job_id` | `uuid`, nullable | Accepted gateway job identity |
| `status` | `text` | Operation lifecycle state |
| `result` | `json`, nullable | Full operation response |
| `error` | `jsonb`, nullable | Sanitized submission/platform error |
| `next_attempt_at` | `timestamptz` | Dispatch or reconciliation schedule |
| `lease_token` | `uuid`, nullable | Claim for current gateway integration work |
| `lease_expires_at` | `timestamptz`, nullable | Integration claim expiration |
| `created_at` | `timestamptz` | Intent commit time |
| `submitted_at` | `timestamptz`, nullable | Time the gateway accepted the operation and returned a job ID |
| `completed_at` | `timestamptz`, nullable | Terminal settlement time |

Statuses are `pending`, `submitting`, `accepted`, `succeeded`, `failed`, `cancelled`,
and `unknown`. `submitting` represents unresolved gateway acceptance and remains
recoverable; `unknown` is a terminal settled outcome.

`previous_operation_id` records an LLM continuation's previous platform operation.
The gateway adapter resolves it without storing a second full request payload.

### Constraints and indexes

- Source and related operations must satisfy the intended same-session rules.
- Lease fields are paired and completion/claim transitions are checked.
- Submission identity must not change across retries of the same operation.
- Indexes cover pending work by `next_attempt_at`, expired leases,
  `(gateway_connection_id, gateway_job_id)`, and session operation listings.
- Callback matching includes the gateway connection, not only the job UUID.
- Terminal outcome persistence and insertion of the completion event are atomic.

A cancellation operation can complete without creating a new gateway job, so a
successful operation may still have a null `gateway_job_id`.

A failed execution protocol response may live in `result`; `error` is not the sole
indicator of failure. Preserve gateway rejection, protocol failure, nonzero command
exit, and ambiguous outcome as distinct cases.

### No implicit process tracking

An `execution.start` operation ends with its returned start response. The platform
does not automatically observe the resulting process or generate a later process-exit
event. Every observation/control request is explicitly staged by the harness and
creates its own operation.

## 7. `session_operation_requests`

### Need and use

This table isolates large request data from durable operation metadata so it can
expire independently. The dispatcher loads it when preparing or retrying a request.

Repeated full-context LLM requests can produce roughly quadratic cumulative storage
growth over a conversation if every historical prompt is retained. This is primarily
a database-storage cost, with additional transient memory pressure when payloads
are loaded. Keeping both a logical request and a nearly identical gateway request
would further duplicate the data.

The revised design stores **one full request copy per operation**, with small
correlation/resolution fields kept separately as needed.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `operation_id` | `uuid`, PK and FK | One request row per platform operation |
| `request` | `json` | Large request payload |
| `expires_at` | `timestamptz`, nullable | Cleanup eligibility deadline; initially null |

An index over non-null expiration times supports bounded cleanup batches.

### Stable submission without duplicate full payloads

The adapter may add correlation data or resolve a previous platform operation into
a gateway job ID. It must produce a stable submission across retries. Transformations
that could change between attempts must be pinned or materialized before first
submission. This does not normally require storing two full prompts.

The exact canonical request representation and adapter-version handling remain to
be designed. The retained request fingerprint and submission identity belong to the
operation metadata so deleting the payload does not delete those records.

### Retention policy

| Situation | Request handling |
| --- | --- |
| Pending submission | Keep the request |
| Submission outcome uncertain | Keep it for retry/reconciliation |
| Gateway accepted and operation outstanding | Initially keep it for a simple recovery policy |
| Operation definitively completed | Assign a configurable retention deadline |
| Deadline passed | Delete the request row, retaining operation metadata and outcome |

The grace period after completion is for inspection/debugging. Immediate deletion
may be supported if that inspection window is unnecessary. The default duration has
not been chosen.

An unresolved submission remains `submitting` and is never cleanup-eligible. An
`unknown` outcome is terminal; its request becomes cleanup-eligible only after an
explicit expiration timestamp has been assigned and that deadline has passed.

Operation requests are temporary transport records, not permanent harness memory.
Anything required for later decisions must remain in saved state, history, or other
retained platform records. Completion handling should rely on the retained result,
not require the expiring original request body.

Deleting the local request does not delete the LLM gateway's retained parent request.
Gateway continuations remain subject to the gateway's separate retention deadline.

Response, event, and history retention are separate policies and remain undecided.
Expiring local operation requests alone does not remove storage growth inside the
gateway or repeated context snapshots stored elsewhere.

## 8. `session_operation_attempts`

### Need and use

This records attempts to communicate with a gateway. It is distinct from the
provider attempts already tracked by the LLM gateway.

It distinguishes definite rejection from a lost connection where gateway acceptance
is uncertain, and provides diagnostics for dispatch and result-retrieval failures.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Attempt identity |
| `operation_id` | `uuid`, FK | Parent operation |
| `attempt_number` | `integer` | Increasing number within the operation |
| `phase` | `text` | `submission`, `result_retrieval`, `cancellation`, or `withdrawal` |
| `lease_token` | `uuid` | Integration claim associated with the attempt |
| `status` | `text` | `running`, `succeeded`, `failed`, `unknown` |
| `http_status` | `integer`, nullable | Observed gateway HTTP response |
| `error` | `jsonb`, nullable | Sanitized diagnostic |
| `started_at` | `timestamptz` | Attempt start |
| `finished_at` | `timestamptz`, nullable | Attempt settlement time |

Unique `(operation_id, attempt_number)` with positive attempt numbers.

Payloads should not be copied into every attempt row. Exact retry budgets and which
reconciliation reads merit individual attempt records remain to be settled.

## 9. `session_waits`

### Need and use

A wait can be inspected or resolved without loading its harness. The expiration
worker and external resolution API operate on the same durable record.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Wait identity returned by the outcome builder |
| `session_id` | `uuid`, FK | Owning session |
| `source_event_id` | `uuid`, FK | Event whose handler created the wait |
| `resolution_mode` | `text` | External input, expiration-only, or either |
| `payload` | `json` | Harness-defined request for input/event |
| `response_schema` | `json`, nullable | Optional reply validation schema |
| `expires_at` | `timestamptz`, nullable | Expiration deadline |
| `status` | `text` | `pending`, `resolved`, `expired`, `cancelled` |
| `resolution_payload` | `json`, nullable | Accepted external reply |
| `resolution_idempotency_key` | `text`, nullable | Identity of accepted resolution request |
| `resolution_hash` | `text`, nullable | Detects conflicting resolution retries |
| `created_at` | `timestamptz` | Wait creation time |
| `finished_at` | `timestamptz`, nullable | Resolution, expiration, or cancellation time |

### Constraints and use

- Source event belongs to the same session.
- Expiration-only waits require an expiration timestamp.
- A pending wait can accept only one terminal resolution.
- The reply API and expiration worker lock or conditionally update the same row.
- Updating a resolved/expired wait and creating `wait_resumed` are atomic.
- An index on pending `expires_at` values drives expiration processing.
- Idempotent resolution retries are checked against the accepted key and fingerprint.

Harness-staged wait cancellation and its single `wait_resumed` event commit with
the handler outcome. If its deadline already passed, expiration wins. The
`session_processing_retries` audit table records idempotency key, request hash,
blocked event, expected/resulting processing revisions, optional reason, and time.
`sessions.processing_revision` advances whenever processing blocks or is retried.
`session_events.retry_failure_baseline` gives a recovered event a fresh retry budget
without deleting historical handler attempts.
Validation, late-reply behavior, and session-cancellation interactions still need
exact rules. No separate timer table or timer event is proposed.

## 10. `gateway_callback_receipts`

### Need and use

The callback endpoint must acknowledge a verified notification even when gateway
submission mapping or result retrieval is not yet ready. This table retains those
notifications for reconciliation and deduplicates webhook retries.

### Columns

| Column | Type | Purpose |
| --- | --- | --- |
| `id` | `uuid`, PK | Local receipt identity |
| `gateway_connection_id` | `text` | Configured gateway that sent the notification |
| `gateway_event_id` | `uuid` | Sender's stable event identity |
| `gateway_job_id` | `uuid` | Job named in the notification |
| `payload` | `json` | Verified notification contents |
| `operation_id` | `uuid`, nullable, FK | Matched platform operation |
| `status` | `text` | `pending`, `processing`, `processed`, `blocked` |
| `next_attempt_at` | `timestamptz` | Reconciliation schedule |
| `lease_token` | `uuid`, nullable | Processing claim |
| `lease_expires_at` | `timestamptz`, nullable | Claim expiration |
| `last_error` | `jsonb`, nullable | Latest reconciliation failure |
| `received_at` | `timestamptz` | Durable receipt time |
| `processed_at` | `timestamptz`, nullable | Successful processing time |

### Constraints and use

- Unique `(gateway_connection_id, gateway_event_id)` deduplicates callbacks.
- Index pending reconciliation by schedule and expired processing leases.
- A matched operation must correspond to the named gateway connection/job.
- `session_id` is unnecessary at receipt time: a fast callback can arrive before the
  platform knows which operation owns the gateway job.
- Verify signatures against the incoming raw body before acknowledging receipt.
- Gateway credentials and signing secrets are not stored in these rows.

The notification wakes result processing; the authoritative outcome is retrieved
from the gateway. Retried notifications and fallback status checks must converge on
one operation outcome and one logical session completion event.

## 11. How the records work together

### User message to external operation

1. Accept a message into `session_events`, allocating its sequence through the
   session record and resolving input idempotency in the same transaction.
2. Claim the session/event and create `session_event_attempts`.
3. Execute the handler outside the database transaction.
4. Verify the processing lease and expected state version.
5. Atomically update `sessions`, append history, insert operations and request
   payloads, create any waits, and mark the event/attempt successfully processed.
6. Dispatch recorded operations, recording gateway communication attempts.

### External completion to harness resumption

1. Persist a verified notification in `gateway_callback_receipts`.
2. Resolve its operation mapping, retaining unmatched receipts for later recovery.
3. Retrieve the gateway outcome.
4. Atomically settle the operation and enqueue its `operation_completed` event.
5. Set request expiration when the operation is eligible under the retention policy.
6. A later handler invocation incorporates the result and decides subsequent work.

Fallback gateway status checks use the same settlement path. They do not create
unsolicited execution observations.

### Wait resolution

1. A committed handler outcome creates the wait.
2. An external reply or expiration worker attempts to resolve its pending row.
3. The winning transaction stores the resolution and inserts its resumption event.
4. The harness later receives that event and decides how its activity should change.

### Recovery boundaries

- Failure before a handler commit leaves the input available for retry.
- Failure after commit leaves recorded operations available for dispatch.
- Uncertain gateway submission retries use the same request and idempotency identity.
- Late workers cannot commit after their lease is replaced or expires.
- Only one authoritative wait resolution and operation completion event is recorded.
- Expired request payloads can be removed without removing operation identity,
  gateway mapping, or result handling.

## 12. Deferred schema decisions

The following remain for further discussion:

- Exact state/processing separation and scheduling fields on `sessions`.
- Event retry budgets, blocking, priority, and manual recovery behavior.
- Canonical gateway request materialization and any small resolution columns.
- Request-retention configuration and default duration.
- History revisions if edits/deletions are supported.
- History, result, event, receipt, and attempt retention policies.
- Wait cancellation/resolution semantics and validation.
- A possible durable UI-progress/update table after the live-update contract is set.

The internal inbox should not automatically become the UI progress stream. A
replayable progress API may need its own durable records; that design is deferred.
