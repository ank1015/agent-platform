# Agent platform: detailed design decisions

This document continues [System working](system_working.md). It records the next
round of discussion across five topics: session lifecycle and API, harness
contracts, persistence and recovery, gateway integration, and server operation.

The project remains in the discussion phase. This document does not authorize or
specify a complete implementation. Concrete Rust signatures, database schemas,
API payloads, and several operational policies remain to be discussed.

Where this document refines the earlier document, use the newer decision here.
In particular, backend authentication replaces the earlier possibility of local
customer accounts, one event is delivered per handler invocation, actions use an
outcome builder, and enforced handler execution deadlines are deferred.

## 1. Session lifecycle and external API

### Identity, configuration, and project grouping

A session remains the persistent agent instance. There is no separate run entity.
It has a selected harness/version, immutable harness configuration, saved state,
conversation history, activity status, and mutable display metadata.

Each session will also have a `project_id`. Callers need to list sessions belonging
to a particular project. The proposed query is:

```text
GET /v1/sessions?project_id=...
```

The initial proposal is to treat this as an opaque identifier supplied by the
calling backend, without a local projects table. Identifier format, whether a
session may change projects, and exact query naming are not finalized.

An existing backend authenticates end users and authorizes their access. Only
authorized backends may call the agent platform. The platform needs service-level
authentication, but no separate user-account or per-user ownership model.

The exact service credential mechanism and rotation policy remain open. Project
grouping does not by itself implement end-user authorization; the calling backend
owns that responsibility.

### Agreed endpoint paths

| Method and path | Purpose |
| --- | --- |
| `POST /v1/sessions` | Create and initialize a session |
| `GET /v1/sessions` | List sessions, including filtering by project |
| `GET /v1/sessions/{sessionId}` | Read a session |
| `PATCH /v1/sessions/{sessionId}` | Update allowed mutable metadata |
| `POST /v1/sessions/{sessionId}/messages` | Submit a user message |
| `POST /v1/sessions/{sessionId}/cancel` | Request cancellation |
| `GET /v1/sessions/{sessionId}/waits` | List session waits |
| `GET /v1/sessions/{sessionId}/waits/{waitId}` | Read a specific wait |
| `POST /v1/sessions/{sessionId}/waits/{waitId}/resolve` | Resolve a wait with input |

Request/response bodies, pagination, error shapes, filters, and HTTP status codes
still require detailed API discussion. PATCH cannot change the harness or immutable
configuration.

The working creation flow validates configuration and initializes state before
returning an idle session. Initialization does not launch external operations.
Input arrives separately through the appropriate endpoint.

Messages and cancellation requests are accepted durably and processed
asynchronously. A proposed caller-supplied idempotency key prevents retried HTTP
requests from creating duplicate inputs. Acknowledgement indicates durable
acceptance, not that the harness has processed the request.

### Session statuses

The six statuses are agreed:

| Status | Working meaning |
| --- | --- |
| `idle` | No current work; ready for input |
| `running` | Pursuing work, including outstanding gateway operations |
| `waiting` | Intentionally suspended pending a wait resolution |
| `cancelling` | Handling cancellation and required cleanup |
| `cancelled` | Current work stopped through cancellation |
| `failed` | The harness could not continue its current work |

The exact transition matrix remains open. The proposal is that messages can be
accepted in all six states, and the harness determines their meaning. A message
received while waiting does not implicitly resolve the wait. Later input may
reactivate a cancelled or failed session according to harness behavior.

Activity status is distinct from scheduling state. Accepting an event makes a
session eligible for processing; the committed handler outcome determines its
activity status. A running session need not have a handler executing at that moment.

### History and live updates

History needs incremental retrieval after a sequence or revision. The proposed
append-only form is:

```text
GET /v1/sessions/{sessionId}/history?after_sequence=42
```

Each history entry would receive an increasing per-session sequence number. The
exact endpoint query and pagination are not finalized. If history entries can be
edited or deleted, append sequence alone is insufficient to describe changes;
revision semantics would need to be designed.

Incoming messages become conversation entries when the harness incorporates them
in its committed outcome. Acceptance at the messages endpoint does not automatically
append them to conversation history.

The public events API and SSE are deferred. The UI needs updates when showing live
progress, but it does not necessarily need the internal events that drive handlers.
Durable input events, persisted conversation messages, and UI progress updates are
separate concepts. UI event shapes, replay cursors, history associations, and delivery
transport will be discussed later.

## 2. Harness contract and execution model

### Conceptual interface

The accepted direction is:

```text
describe()
    -> identity, version, configuration description/schema

initialize(configuration)
    -> initial saved state

handle(configuration, saved state, event, context)
    -> handler outcome
```

These are conceptual operations, not final Rust signatures. Each harness owns typed
configuration and state. A platform adapter serializes them at the storage boundary;
harness code need not represent all internal state as untyped JSON.

The context supplies session identity and platform-supported reads, such as history
and operation/wait information. History is loaded on demand rather than passed in
full to every invocation.

### One event per invocation

One handler invocation processes one event. Only one invocation may authoritatively
advance a session at a time. Outstanding external operations do not prevent the
platform from delivering another event when no handler currently owns the session.

Incoming events can be accepted while a handler executes. They are persisted for
later invocations; they are not injected into the currently executing handler.

For example:

1. LLM operation A is outstanding.
2. A user message starts a handler invocation.
3. A completes while that handler executes, and another user message arrives.
4. Both events are recorded in the inbox.
5. The current handler commits its outcome.
6. Later invocations process the queued events.

The harness decides whether a message should affect current work, be deferred, or
be rejected. Deferring its use must preserve the necessary information in durable
state/history. It cannot depend on an uncommitted local variable surviving.

### Outcome builder

The preferred programming style is an outcome builder. For example:

```text
operation_id = outcome.request_llm(request)
outcome.state.pending_operation = operation_id
```

The method allocates an operation identity and stages an action. It does not submit
the gateway request during the handler invocation.

Other conceptual methods include:

```text
outcome.append_message(message)
outcome.request_execution(request) -> operation_id
outcome.create_wait(specification) -> wait_id
outcome.request_llm_cancellation(operation_id)
outcome.set_status(status)
outcome.emit_progress(update)
```

Exact method names and specialized execution helpers remain to be designed.

A handler outcome contains updated state, history additions, action intents,
activity changes, and optional progress updates. Durable changes commit together.
If execution fails before a valid outcome commits, the builder's staged changes
have no durable effect.

This yields an important ordering guarantee:

```text
Handler stages request and updates state
    -> handler returns
    -> platform commits state and action
    -> dispatcher submits request
    -> completion becomes a later input event
```

A newly staged operation therefore cannot complete before the state that requested
it is saved. An older outstanding operation can complete during handler execution;
its event simply waits in the inbox.

Progress staged through the builder becomes visible after commit. Immediate output
from inside a handler would require a separate facility and has not been designed.

### Async handlers, cancellation, and deadlines

Handlers may be asynchronous. Platform-managed LLM requests, execution requests,
waits, and history access use the platform interface. Arbitrary direct external I/O
is permitted where a harness needs it.

While a handler awaits a read or direct I/O, it retains the session's handler slot.
Later inputs can be accepted but cannot execute their handlers concurrently.

User cancellation is delivered as a normal event to a later invocation. It does not
forcefully abort the currently executing handler. Runtime shutdown and lease loss
are separate conditions for signalling the handler to stop.

**Enforced handler execution deadlines are deferred.** A stuck handler can delay
the session's later events, including cancellation, indefinitely while ownership
remains valid. This limitation must be documented and observable.

Deferring execution deadlines does not remove lease expiration or shutdown grace
periods. A processing lease renews during execution and permits recovery after a
worker disappears. Exact lease and shutdown policies remain open.

### Failure semantics

A harness deliberately returning an outcome with activity status `failed` differs
from handler execution failing before it can produce a valid outcome.

The latter leaves the event eligible for the platform's recovery/retry policy.
Bounded retries and handling repeatedly failing events were proposed, but exact
budgets, classifications, and recovery APIs remain undecided.

Direct I/O can have an external effect before the outcome commits. Retrying the
handler can repeat that effect; the harness owns the recovery semantics for such
calls. Platform-managed actions instead use persisted intent and stable submission
identities.

## 3. Persistence, ordering, and recovery

The following design was broadly accepted as a direction. Detailed database and API
discussions may revise table boundaries and policies.

### Proposed records

| Record | Responsibility |
| --- | --- |
| `sessions` | Project, harness/version, configuration, metadata, activity, state/version, processing ownership |
| `session_events` | Ordered durable inputs, processing state, and retry information |
| `session_history` | Conversation entries with per-session sequence numbers |
| `session_actions` | Committed intents, submission state, gateway correlation, and outcomes |
| `session_waits` | Wait definitions, deadlines, resolution, and lifecycle |
| Callback receipts | Durable notifications that can be reconciled independently of HTTP receipt |

These are conceptual boundaries, not a final schema. Separating state or large
results into additional tables remains an option.

### Ordering and atomic commit

The initial proposal is an increasing per-session event sequence allocated
transactionally. For concurrent arrivals, the database determines the order rather
than client timestamps. History has its own sequence because one event can append
zero, one, or multiple conversation messages.

Processing has three stages:

1. Claim the session and oldest eligible event in a short transaction, recording
   ownership and reading the expected state version.
2. Execute the handler outside the database transaction, renewing its lease.
3. Commit through a short transaction that verifies ownership and state version.

The final transaction writes state, history, activity, actions/wait changes, and the
event acknowledgement together. External I/O does not occur while this transaction
is held open.

An old worker cannot commit after losing ownership. It should receive a stop signal,
but lease fencing only protects platform commits; it cannot undo arbitrary external
effects from direct harness I/O.

A fixed history boundary captured when the handler starts was proposed for context
reads. Newly staged messages remain in the builder. Exact read consistency for
history and operation/wait inspection still needs discussion.

### Recovery cases

| Failure point | Intended recovery |
| --- | --- |
| Input acknowledgement is lost | Caller retries with the same input identity |
| Handler fails before commit | Retry the event from committed state after claim recovery |
| Process fails after outcome commit | Keep event handled; dispatch its recorded actions |
| Gateway accepts but submission reply is lost | Retry the same request and gateway idempotency key |
| Callback precedes saved job mapping | Retain receipt and reconcile after mapping recovery |
| Result is saved before handler processing | Keep its completion event pending |
| Wait reply races with expiration | Accept one resolution and create one logical resumption |

Saving an operation outcome and its completion event should be atomic. Uniqueness
constraints should prevent repeated notifications from creating duplicate logical
completion events. A handler invocation may nevertheless repeat after a failure.

### Policies still open

- Strict arrival ordering was proposed, including cancellation. Priority behavior
  and exceptions are not finalized.
- Actions within an outcome were proposed to be independently dispatchable. Their
  list order would not guarantee external execution order. Dependencies would be
  expressed through later completion handling or supported gateway batches.
- Pausing a session after exhausted handler retries was proposed. Whether later
  events, especially cancellation, may bypass a repeatedly failing event is open.
- An operational processing fault should remain distinguishable from a deliberate
  harness activity outcome; its exact representation is undecided.

## 4. Gateway integration and result delivery

### Credentials and operation identities

The platform owns gateway connections and credentials. Harness configuration
selects resources such as LLM account IDs and machine IDs rather than owning
gateway service credentials.

The initial suggested deployment uses one configured customer identity per gateway.
Connection naming, credential storage/rotation, and support for additional gateway
identities still require configuration design.

The harness immediately receives a platform operation ID when staging an action:

```text
Session -> platform operation ID -> gateway identity + gateway job ID
```

Gateway job IDs are populated after dispatch. The platform uses a stable gateway
idempotency key for each submission and preserves its request across retries.

### Every operation produces an outcome, including admission failures

| Situation | Behavior |
| --- | --- |
| Gateway accepts | Save job mapping and follow the existing job |
| Submission outcome is uncertain | Retry with the same identity and request |
| Gateway definitively rejects | Record failure and deliver a completion event |
| Accepted job becomes terminal | Retrieve and deliver the gateway outcome |

**The harness receives failures even when no gateway job was created.** Therefore,
`operation_completed` refers to a platform operation reaching an outcome, not only
to an accepted gateway job finishing. The harness decides what to do with failure.

Retry budgets, transient rejection handling, and reconciliation intervals remain
open. Exactly-once execution at an external provider is not implied.

### Callback receipt and result retrieval

Callbacks go to stable platform endpoints. The receiver verifies signatures,
persists the notification, and acknowledges receipt promptly. Result workers fetch
the authoritative job result through the gateway API.

Notifications can arrive before submission mapping is committed. The platform
retains these receipts and reconciles them rather than discarding them. Retrying
the original submission recovers the accepted job mapping.

Callbacks are the primary completion signal. Periodic status checks for outstanding
gateway jobs provide fallback recovery when notifications are missed or webhook
delivery retries are exhausted. These checks concern gateway job completion; they
do not automatically observe the process behind an execution handle.

### Result contract

A completion envelope includes the platform operation ID, operation kind, optional
gateway job ID, and outcome/result/error. Preserve gateway-specific distinctions:

- LLM results use the existing `AssistantResponse` shape.
- Execution results preserve protocol responses, batches, and execution handles.
- Gateway rejection, protocol failure, nonzero command exit, and `unknown` outcome
  remain distinguishable.

The harness decides which outcomes produce conversation messages. The platform
does not automatically append every result as a tool-result message.

### Execution observation is explicit

**The platform does not automatically observe a command or generate a later
command-completed event after `execution.start`.**

A start operation completes when the gateway returns the start response. The
command may still be running. Harnesses need platform methods for explicit start,
observe, and control operations. Each requested observation is its own operation.

The harness chooses whether to observe immediately, wait before observing, continue
other work, or leave the command running. Generic background reconciliation only
follows submitted gateway jobs; it does not submit unsolicited observations.

### Cancellation

- LLM cancellation uses the gateway's job-cancellation API. The original operation
  remains outstanding until its terminal outcome is known.
- Stopping a machine process requires an explicit execution control operation
  targeting its handle; the execution gateway has no generic job-cancel endpoint.
- A pending action may be withdrawn before submission, but withdrawal must coordinate
  with dispatch. Uncertain submission requires reconciliation before claiming it
  never started.
- The cancellation request's outcome and the original operation's outcome are
  separate facts. Stopping work does not undo its earlier effects.

### LLM continuations

Harnesses may submit fresh requests or use gateway continuations. The proposed
interface references a previous platform LLM operation; the adapter resolves its
gateway job ID into `previousJobId`.

Gateway rules still apply: the parent must have succeeded, its input must remain
retained, and account/model/settings are inherited as defined by the gateway.
If continuation fails, the harness receives that failure and decides whether to
construct a fresh request. There is no silent conversion to different semantics.

## 5. Server composition and operation

This section records the initial operating direction accepted in the discussion.
Exact settings and deployment guarantees remain to be designed and tested.

### Process and crate composition

Start with one `agent-server` executable and one server replica. The crate structure
from [System working](system_working.md#12-server-and-crate-structure) remains:

```text
apps/agent-server
packages/agent-contracts
packages/agent-runtime
packages/agent-store
packages/agent-gateways
packages/harness-<name>
```

The executable starts independently running components:

| Component | Responsibility |
| --- | --- |
| HTTP API | Authenticate backend callers, accept inputs, expose session data |
| Scheduler/session workers | Claim sessions and process one event per invocation |
| Action dispatchers | Submit committed gateway actions |
| Callback/result workers | Reconcile notifications and retrieve outcomes |
| Wait resolver | Resolve expirations and enqueue resumptions |
| Recovery/reconciliation | Recover claims and check outstanding gateway jobs |

These share resources but do not serialize all work through one loop. Slow gateway
I/O should not prevent input admission or wait expiration processing.

### Harness registration and versions

The server statically imports harness crates and registers them by ID/version.
Duplicate registrations should fail startup. Session creation requires an available
implementation, and existing sessions require their pinned compatible version.

A missing version is an operational problem. Leave session events intact rather
than interpreting saved state with another version. The initial deployment rule is
to retain versions still used by sessions. Explicit migrations remain future design.

### Concurrency, scheduling, and memory

Use separate configurable limits for active handler invocations, gateway network
work, and database pool connections. Waiting gateway jobs consume no handler slot.
Direct external I/O inside a handler retains that handler's slot until it returns.

PostgreSQL is the source of truth and initial work queue. Periodic scans guarantee
discovery of pending work. Local signals or database notifications may reduce
latency, but missing a signal must not lose work. No separate broker or Redis is
required initially.

The proposed fairness baseline is one event per claim, then releasing the session
for scheduling again. Exact cross-session ordering, project fairness, admission
limits, and quotas remain open.

The initial proposal is to load saved state per invocation, query history as needed,
and release it afterward. No application-level session cache is planned initially;
retaining active state can be added after measurements. This is an implementation
starting point rather than a change to the durable session model.

Session and dispatch ownership should be enforced in PostgreSQL even with one
replica. Supporting multiple replicas requires explicit verification of coordination
and recovery; it is not guaranteed merely by choosing database leases. API and worker
executables can be separated later without changing harness semantics.

### Shutdown

On shutdown, stop admission and new claims. Signal active handlers cooperatively
and allow a grace period. A handler that completes while it still owns its claim
can commit; otherwise its uncommitted outcome is discarded and the event recovers
after lease expiration.

Gateway jobs may continue independently. Interrupted submissions recover using
their original identities. Infrastructure shutdown does not mark sessions cancelled.
Grace duration and forced termination behavior remain to be specified.

### Visibility and retention

Provide liveness and database readiness, plus structured operational diagnostics
correlated with session, event, operation, and harness identity. Avoid logging
credentials and full conversation/configuration payloads.

Relevant measurements include pending-event age, handler duration, repeated handler
errors, lease recovery, submission backlog, outstanding-job age, and unresolved
callback receipts. Health checks need precisely documented scope.

Automatic retention/deletion is deferred. History can be necessary for harness
continuation, while processed event payloads may primarily serve diagnostics. The
retention policies for history, events, operations, waits, and results must account
for their different recovery and product roles.

## 6. Outstanding discussion areas

Before implementation, continue refining:

- Concrete database schemas, constraints, claim queries, and transaction boundaries.
- Exact API bodies, error contracts, pagination, idempotency scope, and project rules.
- Rust traits, outcome-builder types, context reads, and serialization/versioning.
- Wait resolution schemas, authorization, cancellation, expiration, and late replies.
- Handler retry policy, repeatedly failing events, cancellation priority, and manual
  recovery behavior.
- Live progress/history delivery, replay cursors, and whether/where SSE is exposed.
- Gateway connection configuration, error mapping, retry budgets, and reconciliation.
- Concurrency, resource limits, shutdown policy, observability, and retention.

No implementation is started by this document. It preserves the discussion so the
remaining decisions can be made deliberately in later conversations.
