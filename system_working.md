# Agent platform: system working

This document records the working design discussed so far. It is a reference for
continued architecture discussion, not a finalized API, database schema, or
implementation specification. Implementation has not been requested at this stage.

The main boundaries below are agreed directions. Candidate field names, payloads,
status semantics, and policies are identified where they still need refinement.

## 1. Purpose and overall model

The platform hosts persistent agent sessions. Each session runs a selected harness
implementation, receives inputs over time, remembers its progress, and requests
external work through the existing LLM and execution gateways.

The platform should support thousands of sessions using a shared pool of server
capacity. A session does not require a dedicated process, thread, or permanently
assigned worker. Workers execute handlers when sessions have events to process.

External work can take seconds, minutes, or hours. Sessions can also wait
indefinitely for input. Session identity and progress survive independently of the
process that last handled them.

Small, recently active states may remain in memory. Unloading and reloading should
be supported by the execution model from the beginning; the policy for retaining
state in memory will be decided later. Durability is necessary even for resident
sessions because processes can fail or be replaced.

## 2. Harnesses and sessions

### Harness

A harness is an implementation of agent behavior. Different harnesses can differ in:

- Configuration and saved-state schemas.
- Interpretation of incoming messages and events.
- Construction of LLM requests and interpretation of model responses.
- Construction of execution requests and interpretation of operation results.
- Conversation management and selection of context for the model.
- Handling of messages arriving during outstanding work.
- Waiting, cancellation, completion, and failure behavior.

We control the initial harness implementations and can write them as explicit
event handlers. Each harness will live in its own Rust crate and implement a
shared platform interface.

### Session

A session is a persistent instance of a harness. It is created before work starts.
Creation selects a harness and version, validates configuration, initializes saved
state, and assigns a session ID. A later message or other supported event gives it
work to process.

There is no separate run entity in the current design. A session can work, become
idle, and later continue when another message arrives. Additional messages arriving
while it works belong to that same session. The harness decides how to handle them.

A `sessions` table is the agreed starting point. Its conceptual contents include:

| Information | Purpose |
| --- | --- |
| Session ID | Stable identity |
| Harness ID and version | Select the implementation interpreting the session |
| Configuration | Fixed harness-specific settings |
| Name and metadata | Display and application information |
| Activity status | Current session activity/outcome |
| Creation and update timestamps | Lifecycle information |

Ownership will also be needed if the platform serves multiple customers. The exact
identity and authorization model remains open. Whether saved harness state lives in
the session row or a separate table is also undecided.

## 3. Immutable harness configuration

Each harness defines its own typed configuration. The platform need not require
common harness configuration fields. Some harnesses may use one model and machine;
others may use several or none. Shared configuration types can be reused where
useful without becoming mandatory for every harness.

Configuration is immutable for the session's lifetime. Accounts, models, machines,
instructions, or other settings supplied at creation remain fixed in that session's
configuration. Mutable display metadata, such as the name, is separate.

Harness-specific configuration and saved state can use typed Rust structures inside
the harness and serialize as JSON at the persistence boundary. Their exact schemas
belong to that harness.

An immutable account or machine reference does not itself freeze the external
resource. For example, the LLM gateway can rotate an account's credentials between
attempts, and a machine daemon can restart with a new runtime generation. The
platform and harness must handle such outcomes under the gateway contracts.

## 4. Session activity status

The proposed statuses are:

| Status | Working meaning |
| --- | --- |
| `idle` | No current work; ready for another input |
| `running` | Pursuing work, including outstanding LLM or execution operations |
| `waiting` | Progress intentionally suspended pending a wait resolution |
| `cancelling` | Processing cancellation and any required cleanup |
| `cancelled` | Current work stopped through cancellation |
| `failed` | The harness could not continue its current work |

These meanings are a working proposal; the exact transition rules still need
discussion. The suggested interpretation is that `cancelled` and `failed` describe
the latest activity outcome and may allow later input to reactivate the session.
Permanent closure or deletion has not been designed.

Activity status is separate from worker execution state. A `running` session may
have no handler executing while an LLM request is outstanding, and its state may be
unloaded from memory.

The harness indicates activity transitions. Outstanding operations alone do not
determine status: a harness might intentionally leave a background command running
after becoming idle. Creating a wait likewise need not suspend all other work.

## 5. Event-driven harness execution

Conceptually, a handler receives:

```text
Fixed configuration + saved state + event + platform context
    -> updated state + history additions + requested actions + status changes
```

This is a behavioral description, not a finalized Rust signature.

The handler decides what to do next and finishes. External results arrive in later
invocations. Saved information must be sufficient to continue without retaining the
previous invocation's call stack or local variables.

Only one handler invocation should authoritatively advance a session at a time.
Several external operations can still be outstanding concurrently. Incoming events
are durably accepted while the session is busy or unloaded.

Receiving an event and acting on its meaning are different. A harness can process a
new message immediately, defer it, or reject it according to its behavior. If it
marks an event handled while deferring the message's use, that deferred information
must remain recoverable from persisted records.

## 6. Event categories

The agreed initial event categories are:

| Event | Meaning |
| --- | --- |
| User message | New input for the session |
| Operation completed | A gateway job reached a terminal outcome |
| Cancellation requested | A request to stop the session's current work |
| Wait resolver/resume | A previously created wait was resolved or expired |

Suggested serialized names are `user_message`, `operation_completed`,
`cancellation_requested`, and `wait_resumed`; exact names are not frozen.

Events will have a common platform envelope containing identity, session identity,
type, timestamps, and relevant correlation identifiers. Payloads differ by event
type. Harness-specific wait data can be carried inside a fixed event category.

Operation completion includes unsuccessful outcomes. A failed LLM job or an
execution gateway job with an `unknown` outcome must be represented without
pretending it succeeded or is safe to repeat.

Event ordering, cancellation priority, handler time limits, and retry policies are
still to be defined. The initial proposal is durable arrival ordering with one
handler invocation at a time per session.

## 7. Waits and resumption

Waits are a first-class platform facility. There is no separate timer event.

A harness can create a wait for user input or some externally supplied event. A
wait may have an expiration. An expiration-only wait provides the equivalent of a
timer without requiring input.

The proposed separation is:

- The platform owns wait identity, pending/resolved state, expiration, and reliable
  resumption.
- The harness owns the meaning of the request and the response.

Candidate wait fields include:

| Field | Purpose |
| --- | --- |
| `waitId` | Identifies this particular wait |
| `resolutionMode` | External input, expiration only, or whichever arrives first |
| `expiresAt` | Optional deadline; required for expiration-only waits |
| `payload` | Harness-defined JSON describing the requested input/event |
| `responseSchema` | Optional schema for validating replies |
| `status` | Pending, resolved, expired, or cancelled |
| `resolution` | Accepted response or expiration outcome |

These fields are proposals, not an agreed wire schema. User-facing payloads might
describe a question, choices, approval, or form.

A reply targets a specific wait. A successful reply or expiration creates a
resumption event. A reply racing with expiration must produce only one accepted
resolution, and retries must not create multiple logical resumptions. Event
processing can still be retried after a handler failure.

The initial suggestion for external events is explicit resolution by wait ID.
Automatic matching of arbitrary events against predicates has not been agreed.

Authorization to resolve a wait, response validation rules, late replies, wait
cancellation, and the effect of session cancellation on pending waits remain open.

## 8. Actions and the platform interface

A harness decides which external work to request and how to interpret it. The
platform owns durable recording, dispatch, correlation, and result delivery for
its supported facilities.

The capabilities discussed so far are:

- Submit an LLM job.
- Submit an execution operation or batch.
- Request cancellation through a supported mechanism.
- Create and manage a wait.
- Append conversation entries or emit user-visible output/progress.
- Change session activity status.

The precise distinction between returned actions and other fields in a handler
outcome is still open. For example, conversation additions and status changes may
be explicit outcome fields rather than separately dispatched actions.

LLM calls, execution operations, waits, and history reads go through the
platform-provided interface. Reading history is a query; requesting an external
operation produces durable intent for later dispatch.

Direct external I/O from a harness is allowed when needed. It has a different
recovery boundary: an external call can succeed before the handler commits, and a
retry can repeat it. The harness must account for that ambiguity. The platform
cannot promise automatic recovery or deduplication for arbitrary direct I/O.

## 9. Conversation history and saved state

Conversation history belongs to the session and is stored separately from event
delivery records. Its message shapes follow the existing
[LLM contracts](../llm-providers/packages/contracts/src/messages.ts):

- `user`
- `assistant`
- `tool_result`
- `system`
- `custom`

Provider-native assistant content remains opaque so it can be replayed correctly.
The Rust platform needs equivalent serialized contracts. Event records are not
automatically conversation entries: a wait expiration, for example, may have no
place in the model context or user-visible conversation.

Each harness controls how events contribute to its conversation and which history
it uses in model requests. It may need only conversation history plus a small
saved state, or it may retain plans, deferred inputs, parallel work, or intermediate
calculations.

Stored history, current working state, and assembled LLM request context are
different quantities. A long session need not keep its entire history in memory
while waiting.

## 10. Operation records and gateway integration

Conversation history captures the agent's information and decisions. Platform
operation records capture what external work was requested and what happened to it.

An assistant message containing a tool call is not enough to determine whether to
execute it again:

| Persisted situation | Next responsibility |
| --- | --- |
| Tool call exists but no execution action is recorded | Harness prepares the action |
| Action recorded but submission pending | Platform dispatches it |
| Gateway already accepted a job | Platform follows the existing job |
| Result received but event not handled | Harness processes the completion event |
| Result already incorporated | Harness continues its next decision |

Stable platform operation identities connect session work to gateway jobs. The
platform owns these mappings; the gateways have no agent/session model. Correlation
must distinguish the gateway as well as its job ID.

### LLM gateway

The [LLM gateway](../llm-providers/apps/llm-gateway/README.md) accepts durable jobs,
performs provider calls through leased workers, stores outcomes, and sends signed
terminal notifications. It owns provider retries and supports job cancellation.

The platform recovers an existing submission with the same idempotency identity.
Creating another logical job is a separate decision. Gateway recovery may repeat
provider work; submission idempotency does not guarantee exactly-once provider
execution.

Harnesses choose request semantics. One can construct fresh context while another
uses the gateway's retained-job continuation facility. The harness remains
responsible for respecting that facility's retention and inheritance rules.

### Execution gateway

The [execution gateway](../execution-providers/apps/execution-gateway/README.md)
routes jobs to registered machine daemons. It stores operation/batch responses and
does not automatically replay dispatched operations with uncertain outcomes.

A completed `execution.start` job can return a handle to a command that is still
running. The harness, or a reusable adapter it uses, requests subsequent observation
or control operations. Gateway job completion is not automatic process-exit
notification.

The current execution gateway has no generic job-cancellation endpoint. Stopping
a process requires the appropriate execution control operation. An `unknown`
outcome requires reconciliation where possible; it must not imply a safe retry.

Machine enrollment and command execution are provided by the execution system.
Sandbox provisioning and lifecycle are not provided by the current execution
gateway and have not been designed as part of this platform discussion.

### Receiving results

Callbacks target a stable platform endpoint, independent of the worker processing
a session. The platform verifies and durably records the notification, retrieves
the authoritative result, and makes a completion event available to the session.

Notification receipt and harness processing are separate acknowledgements. Callback
retries must be deduplicated. Fast callbacks arriving before the submission response
has persisted the job mapping must also be recoverable.

The exact reconciliation mechanism, fallback status checks, callback registration,
and gateway credential ownership remain to be specified.

## 11. Persistence and recovery

The agreed persistence boundary for a handler outcome is one atomic transaction
containing:

- Updated harness state.
- Conversation additions.
- Session activity changes.
- Outgoing action intents, including wait-related changes as appropriate.
- The processed event's handled marker.

External side effects occur after the intent is committed. If the handler fails
before commit, the input remains available for another attempt. If commit succeeds
and the process then fails, the actions remain available for dispatch.

For example, processing an LLM completion can atomically append its assistant
message, record execution actions for its tool calls, and update saved state. The
dispatcher subsequently submits those actions using stable idempotency keys.

Do not hold a database transaction open while executing arbitrary harness code or
waiting on external I/O. The proposed sequence is to claim a session, execute its
handler, and commit through a short transaction that verifies the claim and expected
state version. Exact lease, fencing, and read-consistency rules still need design.

The persistence model therefore needs durable records for sessions, harness state,
history, incoming events, outgoing operations/actions, waits, and submission/result
correlation. This is a list of responsibilities, not a finalized table layout.

Recovery must distinguish handler execution failures, deliberate harness failure
outcomes, uncertain submissions, and gateway failures. Retry limits and the handling
of repeatedly failing events have not been settled.

## 12. Server and crate structure

The agreed initial structure is one server application, shared platform crates,
and one crate per harness:

```text
agent-platform/
├── apps/
│   └── agent-server/
└── packages/
    ├── agent-contracts/
    ├── agent-runtime/
    ├── agent-store/
    ├── agent-gateways/
    └── harness-<name>/       # One crate per harness
```

| Crate | Responsibility |
| --- | --- |
| `agent-contracts` | Harness interface, event/action types, statuses, conversation shapes, and identifiers |
| `agent-runtime` | Registry, scheduling, handler execution, action dispatch, waits, and recovery |
| `agent-store` | PostgreSQL schema, migrations, queries, and atomic persistence operations |
| `agent-gateways` | Gateway clients, wire contracts, and webhook verification |
| `harness-<name>` | Harness-specific configuration, state, initialization, and behavior |
| `agent-server` | HTTP, authentication, application configuration, dependency wiring, and process lifecycle |

Harnesses principally depend on the shared contracts and receive platform access
through a provided context. The exact placement of context interfaces and query
abstractions will be worked out when defining Rust dependencies.

The server statically imports and registers harness implementations. Adding a
harness initially means adding its crate and rebuilding the server. Dynamic library
loading is unnecessary for the current design.

Existing sessions must retain a compatible registered harness version, or undergo
an explicit migration before that version is removed. A version field alone cannot
provide compatibility; versioning and migration policy still need discussion.

### Runtime components in the server

Initially the HTTP API and background processing run in one executable and
deployment. Their logical responsibilities are:

- HTTP admission of messages, cancellation requests, wait resolutions, and callbacks.
- Session workers processing pending events.
- Dispatchers submitting persisted gateway actions.
- Result processing retrieving outcomes and recording completion events.
- Wait expiration processing generating resumption events.
- Recovery of interrupted claims and incomplete submissions.

These components share capacity with configurable concurrency. Their startup and
shutdown should remain separable so an independent worker executable can be added
later if useful. Multi-replica behavior still requires explicit coordination design;
the crate structure alone does not establish it.

`agent-store` should expose coherent persistence operations such as claiming a
session and committing a handler outcome, keeping transactional invariants together.

## 13. Topics for further discussion

The architecture provides a foundation for further design. Remaining topics include:

1. Exact Rust harness interface, platform context, and event/action payloads.
2. Ownership, authentication, permissions, and gateway account/credential mapping.
3. Event ordering, cancellation priority, stale results, and reactivation semantics.
4. Wait schemas, resolution authorization, expiration, cancellation, and late replies.
5. Database tables, indexes, claim leases, fencing, and history read consistency.
6. Handler failure classification, bounded retries, and operator recovery.
7. Action-submission races, callback correlation, and result reconciliation.
8. Harness version compatibility and state migrations.
9. Output subscriptions, history retention, large-result storage, and capacity limits.
10. Admission limits, fairness, memory retention, observability, and deployment behavior.

No implementation is implied by this document. These topics will be discussed
further before implementation proceeds.
