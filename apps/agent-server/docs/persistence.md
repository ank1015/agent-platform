# Durable model and transaction boundaries

PostgreSQL is the authority for sessions and work. The current schema is defined
by [`agent-store/migrations`](../../../packages/agent-store/migrations/) and
accessed through [`agent-store`](../../../packages/agent-store/src/). This page
explains the model and its invariants; it is not a copy of every SQL column or a
migration procedure.

## Tables and their consumers

| Table | Durable role | Primary consumer |
| --- | --- | --- |
| `sessions` | Project grouping, fixed harness/version/config, saved state/version, activity status, processing health, sequence allocators, handler lease | Session API, scheduler, harness context |
| `session_events` | Ordered per-session inbox with input fingerprint, processing status, operation/wait correlation | Input API, scheduler, result/wait completion |
| `session_event_attempts` | Invocation number, captured state/history revision, lease identity, result/error | Scheduler, attempt inspection |
| `session_history` | Append-only LLM-contract conversation entries and per-session sequence | Harness context, history API |
| `session_operations` | Staged LLM/execution/control work, stable gateway identity, claim, mapping, outcome, request-removal marker | Dispatcher, completion, inspection |
| `session_operation_requests` | The one potentially large JSON request per operation and its cleanup deadline | Dispatcher, request inspection, cleanup |
| `session_operation_attempts` | Gateway submission/result/cancellation/withdrawal attempt audit | Dispatcher, completion, attempt inspection |
| `session_waits` | External/expiration/either waits, response schema, resolution fingerprint, finish state | Wait API, expiry worker, harness context |
| `gateway_callback_receipts` | Verified, deduplicated notification hints; may be unmatched before job mapping | Callback API, completion worker |
| `session_processing_retries` | Idempotent, audited operator reset of an exact blocked head | Processing retry API |
| `session_updates` | Small replayable UI changes with an independent sequence/retention boundary | Update API and SSE feed |

## Identity, ordering, and same-session references

Session IDs are caller-supplied UUIDs. Events, history, and updates each have
their **own** per-session sequence; a number in one stream cannot be used as a
cursor in another. `sessions` owns each next-sequence allocator. Inputs and
resolutions lock the session row while allocating an event sequence, so
concurrent callers cannot create gaps or reverse admission order. History is
assigned only by a committed handler outcome. Updates use versioned kinds and a
retention boundary (`retained_update_sequence`) independent of the inbox and
history.

Composite foreign keys and uniqueness constraints keep an event's operation or
wait, a history entry's source event, an operation's target/previous operation,
and an update's source event in the **same session**. There is at most one
`operation_completed` event per operation and one `wait_resumed` event per wait.
External event idempotency is unique by `(session_id,type,idempotency_key)`.
Gateway job mapping is unique by `(gateway_connection_id,gateway_job_id)`;
callbacks are unique by `(gateway_connection_id,gateway_event_id)`. These
constraints are a backstop to transactional repository methods, not a substitute
for their precondition checks.

## Claims and state version

The scheduler claims the head event and records the current event ID, lease
token/expiry, input state version, and history-through bound. An authoritative
outcome must still own that lease and match the captured state version. Otherwise
its entire commit is rejected. Lease expiry makes interrupted invocations
recoverable, while the state version prevents a stale handler from overwriting a
new checkpoint. Operation and callback receipt workers likewise use durable
claims; attempt rows record what happened under each claim. PostgreSQL time is
used for due/lease decisions so process clock skew is not the arbiter of races.

`processing_enabled`, `processing_error`, and `processing_revision` distinguish
operational blocking from the harness's activity `status`. A blocked head remains
in the inbox. The retry API checks the exact event/revision, resets its failure
baseline, and audits the action without mutating history or saved harness state.

## Atomic write boundaries

The application does not expose generic write access to every table. Its main
repository transactions are:

1. **Session creation:** save fixed configuration and initialized state; emit a
   session update.
2. **Input admission:** fingerprint the external request; allocate an event
   sequence; insert one inbox event and its update, or return the existing event.
3. **Handler outcome:** verify lease/version; save state, status, history,
   staged operations and their sole requests, waits, cancelled waits, progress
   updates, and event acknowledgement together.
4. **Wait finish:** external reply, expiration, or cancellation wins once;
   finish the wait and create its `wait_resumed` event in the same transaction.
5. **Operation finish:** save authoritative outcome and exactly one
   `operation_completed` event together. A callback receipt is separately
   durable before this reconciliation.
6. **Request cleanup:** recheck terminal status and deadline while locked;
   delete only the request, mark removal, and emit an operation update together.

Gateway HTTP calls happen **outside** these transactions. Consequently, a lost
gateway response can be uncertain even though the operation was committed. The
stable gateway identity and retained request let workers reconcile that state;
the system never treats a missing HTTP acknowledgement as proof no job exists.

## Retention and read surfaces

`session_operation_requests` can be much larger than every other operation row.
The server keeps one payload instead of storing full repeated LLM requests on
each attempt. Once a terminal outcome has been retained through its assigned
deadline, the request cleanup worker removes that row. The operation, hash,
attempts, result, and removal timestamp remain for inspection. Requests needed
for pending, submitting, accepted, or uncertain work cannot be removed.

`session_updates` are pruned by age (seven days by default); a client behind the
retained boundary receives a resnapshot signal. Session rows, history, event
records, operation outcomes, and attempt audits are not automatically pruned in
this version. List APIs use bounded pages and omit large operation payloads.

See [architecture](architecture.md) for how workers use these transactions and
[the API](api.md) for the corresponding read surfaces.
