# Agent platform

The implemented platform provides a harness-facing Rust contract, a registry,
durable PostgreSQL persistence, authenticated session/input/wait APIs, the session
scheduler, gateway dispatch, asynchronous result delivery, inspection, replayable
application updates, and retention/operational workers.
See [agent-server/docs](apps/agent-server/docs/README.md) for the implemented
architecture, persistence model, HTTP API, configuration, and harness integration.

| Crate | Role |
| --- | --- |
| `packages/agent-contracts` | Public types, `Harness`, `HarnessContext`, and outcome builder |
| `packages/agent-runtime` | Registration, typed JSON adapter, session scheduler, and database-backed harness context |
| `packages/agent-test-support` | In-memory context for harness tests |
| `tests/harness-fixture` | Independent example harness and contract tests |
| `packages/agent-store` | PostgreSQL schema and transactional repository operations |
| `packages/agent-gateways` | Configured gateway clients and signed callback verification |
| `apps/agent-server` | Authenticated HTTP API and executable |

Harnesses implement `Harness` with their own `Config` and `State` types. Config is
deserialized and validated when a session is initialized. The registry exposes a
JSON Schema for discovery and resolves only exact registered versions. An unavailable
version is handled as an operational fault by the server; saved state is
never passed to a different version automatically.

Each `handle` call receives one event and returns a `HandlerOutcome`. A handler can
read its session's bounded history and committed operation/wait records through
`HarnessContext`. It stages new messages, operations, waits, and activity changes
with `OutcomeBuilder`. Staging allocates IDs but performs no external work. The
runtime's JSON adapter returns the serialized outcome. The session scheduler claims
one event per invocation, renews its fenced lease through handling, outcome
preparation, and persistence, then commits the complete outcome atomically. Gateway
dispatch and result retrieval run independently after that commit. If a handler
returns an error, its uncommitted builder is discarded. A deliberate
`SessionStatus::Failed` outcome is different
from a handler error.

The execution request payload uses the existing `process-execution-protocol` Rust
types, re-exported as `agent_contracts::execution` and `execution_core`. An explicit
`execution.observe` operation is available through that payload;
`execution.start` does not create automatic process observation. LLM requests and
conversation messages mirror the LLM gateway's JSON contract. A continuation names
the preceding **platform** operation ID; the dispatcher resolves it to the prior
gateway job. Result records preserve gateway rejection, protocol failure,
cancellation, and unknown execution outcomes separately.

Assistant content keeps each provider-native JSON item as raw JSON. Future storage
and gateway adapters must serialize messages directly to JSON text when handling
these items; routing them through `serde_json::Value` can lose valid escaped
provider data. The registry adapter uses raw JSON for configuration and saved state
checkpoints for the same reason. `StopReason`, `Usage`, and `UsageCost` mirror the
typed LLM response contract.

The current context contract fixes a maximum visible history sequence per invocation.
Operation and wait reads return currently committed values. Calls to arbitrary
external services inside a handler are possible, but the harness must handle repeated
effects if an uncommitted invocation is retried.

Wait creation supports `External` (reply only, no deadline), `Expiration` (deadline
only), and `Either` (reply or deadline). Both modes involving expiration require
`expires_at`. A wait may expire without creating a separate timer event. Harnesses
can stage `cancel_wait(wait_id)` in an outcome. A pending wait then becomes cancelled
and gets one `wait_resumed` event with reason `cancelled`; a deadline already reached
takes precedence and produces `expired`. External replies, expiration, and
cancellation serialize on the session row. A harness can stage bounded progress
records with `outcome.emit_progress(payload)`. Progress is visible after the
handler outcome commits; mid-handler streaming remains deferred.
Creating and cancelling the same wait in one outcome is supported: the committed
wait is immediately terminal and its resumption event follows the source event.
The store migration creates sessions, events and attempts, history, operations and
their expiring request payloads and attempts, waits, and callback receipts. Repository
methods allocate per-session sequences, enforce exact-byte idempotency, claim work
with fenced leases, commit complete handler outcomes atomically, settle operations
with one completion event, resolve waits with one resumption event, and retain early
callbacks for later matching. Opaque state and payloads remain PostgreSQL `json` and
round-trip as raw JSON text.

The store supplies persistence transitions but does not itself run scheduler loops,
invoke harnesses, call gateways, or expose HTTP endpoints. Handler execution
deadlines remain deferred.

The server runs a wait-expiration worker beside the scheduler, dispatcher, and
result worker. It processes due waits after restart and converges safely with other
workers. An update-cleanup worker retains small application updates for seven days
by default; history and operation outcomes have independent retention.

## Session runtime

The server supervises `SessionScheduler` beside HTTP. The scheduler scans PostgreSQL
for the oldest eligible event in each session, takes capacity before claiming, and
runs at most one authoritative handler for a session. Inputs accepted while a
handler is active remain queued in strict sequence order. Different sessions may run
concurrently up to the configured handler limit.

Every claim records a handler attempt and a fixed history boundary. `StoreContext`
uses that boundary for history while returning current committed operation and wait
records. Harness state, history additions, operations, waits, activity status, event
acknowledgement, and the attempt result commit in one fenced transaction. Staged
operation and wait IDs and provider-native raw JSON are retained across safe commit
retries.

Handler errors are attempted three times by default: the first retry waits one
second and the second waits five seconds. The third failure blocks the head event,
sets `processing_enabled=false`, and records a separate operational
`processing_error`; it does not change the harness activity status. Missing harness
versions, invalid saved data, validation panics, and invalid outcomes block
immediately. A deliberate successful outcome with `SessionStatus::Failed` is still
a normal commit.

Database failures from platform context reads do not consume the handler retry
budget. The current attempt is abandoned and made eligible after the infrastructure
retry delay. A definitely rolled-back transient outcome transaction retries the
same prepared outcome with the same IDs. An uncertain commit is never replayed
blindly: the runtime checks the durable attempt status and otherwise leaves the
lease to recover it. Abandoned crash/shutdown attempts do not count as handler
failures.

Lease loss sets `HarnessContext::stop_requested`. The runtime gives that local
invocation a short ownership-loss grace, then drops it and discards any result so it
cannot retain worker capacity. This is an infrastructure fencing policy, not a
general handler execution deadline. During server shutdown, new admission and
claims stop, active contexts receive the same cooperative stop signal, and the
runtime continues renewing leases until handlers commit or the shutdown grace
expires. Forced shutdown leaves uncommitted attempts recoverable after lease expiry.

Runtime settings and defaults are:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `AGENT_SCHEDULER_ENABLED` | `true` | Run the in-process session scheduler |
| `AGENT_MAX_CONCURRENT_HANDLERS` | `32` | Maximum active handler invocations |
| `AGENT_SCHEDULER_POLL_MS` | `100` | PostgreSQL work-scan interval |
| `AGENT_HANDLER_LEASE_MS` | `30000` | Session claim lifetime |
| `AGENT_HANDLER_LEASE_RENEWAL_MS` | `10000` | Lease renewal interval |
| `AGENT_HANDLER_RETRY_DELAYS_MS` | `1000,5000` | Handler-error retry delays; count plus one is the attempt budget |
| `AGENT_HANDLER_COMMIT_RETRY_DELAYS_MS` | `10,50` | Safe prepared-outcome transaction retry delays |
| `AGENT_INFRASTRUCTURE_RETRY_DELAY_MS` | `1000` | Delay after an abandoned platform-infrastructure attempt |
| `AGENT_OWNERSHIP_LOSS_GRACE_MS` | `100` | Cooperative stop grace after claim loss |
| `AGENT_SHUTDOWN_GRACE_MS` | `30000` | HTTP and active-handler shutdown grace |
| `AGENT_WAIT_EXPIRATION_ENABLED` | `true` | Run due-wait expiration processing |
| `AGENT_WAIT_EXPIRATION_POLL_MS` | `100` | Wait expiration scan interval |
| `AGENT_WAIT_EXPIRATION_BATCH_SIZE` | `100` | Due waits examined per scan |
| `AGENT_REQUEST_CLEANUP_ENABLED` | `true` | Remove eligible terminal operation requests automatically |
| `AGENT_REQUEST_CLEANUP_POLL_MS` | `60000` | Request cleanup scan interval |
| `AGENT_REQUEST_CLEANUP_BATCH_SIZE` | `100` | Maximum requests examined per scan |
| `AGENT_UPDATE_RETENTION_MS` | `604800000` | Retain replayable application updates for seven days |
| `AGENT_UPDATE_CLEANUP_POLL_MS` | `60000` | Poll interval for update cleanup |
| `AGENT_MAX_CONCURRENT_SSE` | `200` | Maximum live update subscriptions in this process |

The authenticated `history`, `operations`, and `events` routes expose conversation
entries, gateway work, and handler attempts. History uses per-session append
sequences and a captured `through_sequence` for stable incremental reads. Operation
lists omit large requests and results; request payloads are fetched separately at
`GET /v1/sessions/{sessionId}/operations/{operationId}/request`. Once its retention
worker removes a request, that endpoint returns `410 request_payload_expired` and
the operation still exposes its outcome and attempts.

Request cleanup checks the assigned expiration only after an operation has a
terminal outcome. Pending, submitting, and accepted operations retain their sole
stored request, including when gateway submission is uncertain. Cleanup uses small
batches, skips locked work on the current sweep, and revisits it later. Core session
records, history, events, attempts, and operation outcomes do not have automatic
retention in this version.

`GET /v1/metrics` requires the ordinary backend service token and returns
Prometheus text. HTTP counts and durations and the SSE gauge are per process;
pending work, lease gauges, overdue waits, and request cleanup counts are read from
PostgreSQL and represent shared state. Do not sum the shared gauges across server
replicas. Logs are JSON by default and include request IDs and worker correlation
fields without logging request payloads.

See the [harness integration guide](apps/agent-server/docs/harness-integration.md)
for the contract used by a real harness crate. Run `sh scripts/verify.sh` with
`TEST_DATABASE_URL` set to run the full
workspace and disposable-PostgreSQL test suites; ordinary `cargo test` skips the
ignored database tests.

For local gateway callbacks, `scripts/dev-agent-server-ngrok.sh` builds and migrates
the server, waits for readiness, and exposes it through the account's fixed ngrok
HTTPS domain. Supply the normal server configuration through the environment:

```sh
AGENT_DATABASE_URL='postgresql:///agent_platform' \
AGENT_SERVICE_TOKENS='replace-with-a-local-backend-token' \
./scripts/dev-agent-server-ngrok.sh
```

The default public base URL is `https://streak-upscale-okay.ngrok-free.dev`; the
script requests that name explicitly, so it does not change between runs. Override
it with `NGROK_DOMAIN` if the assigned ngrok domain changes. Callback URLs append
`/v1/callbacks/llm/<connection-id>` or
`/v1/callbacks/execution/<connection-id>`.

The launcher automatically adds the `execution-primary` and `llm-primary` gateway
connections and their callback verifiers. It reads
`agent-server-execution-gateway-api-key`,
`agent-server-execution-gateway-webhook-secret`,
`agent-server-llm-gateway-api-key`, and
`agent-server-llm-gateway-webhook-secret` from Secret Manager in project
`project-2c02a9f6-1ff5-461d-ae0`. No gateway secret is stored in this repository.
Set `AGENT_EXECUTION_GATEWAY_AUTO_CONFIG=false` or
`AGENT_LLM_GATEWAY_AUTO_CONFIG=false` to configure that gateway manually. Harnesses
should select `execution-primary` for execution operations and `llm-primary` for LLM
operations.
Any additional `AGENT_GATEWAY_CONNECTIONS` and `AGENT_GATEWAY_CALLBACKS` entries
supplied by the caller are preserved.

`GET /v1/sessions/{sessionId}/updates` reads the durable update log, and
`GET /v1/sessions/{sessionId}/updates/stream` replays it as SSE. Each SSE `id` is
the per-session update sequence; reconnection uses `Last-Event-ID` or an initial
`after_sequence` query. Updates contain small resource references and optional
harness progress. Session detail exposes `update_through_sequence` and
`history_through_sequence` for page bootstrap. A cursor older than retained updates
returns `410 resnapshot_required`; the caller reloads session detail and history.
The create and metadata-PATCH responses advertise cursors taken no later than their
resource snapshot, so replay can duplicate a visible change but cannot miss one.
SSE subscriptions stop promptly when the server begins draining.

A blocked head event can be retried with authenticated
`POST /v1/sessions/{sessionId}/processing/retry`. The request needs an
`Idempotency-Key` and the exact `expected_event_id` and
`expected_processing_revision` shown in session `processing_health`. This clears
the operational block and gives the same event a fresh handler-error budget without
changing harness state, activity status, history, or gateway operations. Duplicate
requests return their original acknowledgement; stale event/revision checks return
`409`.

Cancellation remains a harness decision. The Part 8 test harness withdraws work
that has not reached a gateway, requests LLM cancellation for an accepted job,
cancels its pending wait, and ignores old operation/wait completions after a new
message starts fresh work. A separate execution fixture chooses an explicit
`execution.terminate` action for a known process handle. The platform does not
automatically cancel operations or observe execution processes when a cancellation
input is admitted.

## Gateway dispatch

The operation dispatcher claims committed LLM, execution, LLM-cancellation, and
withdrawal actions independently of session handling. Gateway connections are
configured once by the server and selected by the opaque connection ID stored in a
harness action. Harnesses never receive gateway URLs or credentials.

`AGENT_GATEWAY_CONNECTIONS` is a JSON array. Each entry has `id`, `kind` (`llm` or
`execution`), a directory-style `base_url`, `bearer_token`, and optional
`timeout_ms`:

```json
[
  {
    "id": "llm-primary",
    "kind": "llm",
    "base_url": "https://llm-gateway.example.com/",
    "bearer_token": "replace-with-user-api-key",
    "timeout_ms": 30000
  }
]
```

The platform operation UUID is the gateway idempotency key and remains unchanged
across attempts and restarts. The full operation request exists only in
`session_operation_requests`; attempt rows contain status and safe errors, not a
copy of prompts or commands. Execution start/input/interrupt identities remain
separate protocol-level identities supplied by the harness.

A connection ID denotes one stable logical gateway and gateway-user identity.
Deployments may rotate its credential, but must not repoint the same ID to another
gateway or user: uncertain recovery looks up the original operation UUID within
that identity.

A valid gateway acknowledgement stores the gateway job UUID and moves the action
to `accepted`. It does not create an `operation_completed` event. Result polling and
callback delivery are handled by separate runtime work. A definitive admission rejection on a
first submission instead completes the operation as `failed` and creates its durable
completion event. A timeout, malformed success response, or server error leaves the
action in `submitting`; recovery resubmits exactly the same body and idempotency key.
Once acceptance has become uncertain, a later rejection cannot downgrade it to a
definite failure.

`idempotency_conflict` is an integrity failure, not an invitation to retry with a
replacement key or adopt the returned gateway state. On a first definitive
submission it fails the platform operation. If an earlier attempt was already
uncertain, the operation remains uncertain and keeps recovering only under its
original key; the dispatcher never adopts a potentially different-input job.

LLM continuations resolve their previous platform operation to a successful LLM
gateway job on the same connection. Outstanding parents defer the continuation;
terminal unsuccessful parents become a dependency failure. The LLM gateway remains
authoritative about parent-request retention. LLM cancellation is a separate durable
operation: `accepted` reports whether the gateway recorded a cancellation request,
not the final status of the original job. Execution observation and controls are
explicit operations; a successful `execution.start` is never observed automatically.

Local withdrawal races atomically with dispatch. It succeeds only while the target
is pending and unclaimed. Once dispatch owns or may have submitted the target, the
withdrawal result is `withdrawn: false`.

Dispatcher settings are:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `AGENT_DISPATCHER_ENABLED` | `true` | Run in-process operation dispatch |
| `AGENT_MAX_CONCURRENT_DISPATCHES` | `32` | Maximum active gateway calls |
| `AGENT_DISPATCH_POLL_MS` | `100` | PostgreSQL operation-scan interval |
| `AGENT_OPERATION_LEASE_MS` | `30000` | Operation claim lifetime |
| `AGENT_OPERATION_LEASE_RENEWAL_MS` | `10000` | Claim renewal interval |
| `AGENT_OPERATION_RETRY_DELAYS_MS` | `1000,5000,30000` | Gateway retry delays; the last value caps continued backoff |
| `AGENT_OPERATION_DEPENDENCY_DELAY_MS` | `250` | Delay while a parent action is outstanding |
| `AGENT_OPERATION_REQUEST_RETENTION_MS` | `604800000` | Request retention after an immediate terminal result |
| `AGENT_OPERATION_RESULT_CHECK_DELAY_MS` | `1000` | Earliest later result-delivery check after acceptance |

## Gateway callbacks and result delivery

The platform accepts signed lightweight notifications at
`POST /v1/callbacks/llm/{connectionId}` and
`POST /v1/callbacks/execution/{connectionId}`. These routes authenticate with the
gateway signature rather than the backend bearer token. LLM callbacks use the LLM
gateway headers and execution callbacks use the execution gateway headers. The
signature covers `timestamp.eventId.rawBody`, uses HMAC-SHA256, and must be within
the configured five-minute window. Multiple secrets may be configured for credential
rotation. Execution callbacks use payload version 2.

`AGENT_GATEWAY_CALLBACKS` is a JSON array whose IDs and kinds must match entries in
`AGENT_GATEWAY_CONNECTIONS`:

```json
[
  {
    "id": "llm-primary",
    "kind": "llm",
    "secrets": ["current-webhook-secret", "previous-webhook-secret"]
  }
]
```

A verified callback is persisted before `202 Accepted` is returned and redelivery
is deduplicated by connection and gateway event ID. A callback that arrives before
the dispatcher stores the gateway job mapping remains pending and is matched later.
Once matched, it makes the accepted operation immediately eligible for retrieval.

Callbacks are hints: the result worker always reads the authoritative job detail
from the configured gateway. Accepted operations are also checked every 30 seconds
when a callback is missed. A nonterminal job is scheduled for another check;
retrieval failures keep the operation accepted and never resubmit it. Terminal
result persistence and creation of the single `operation_completed` event happen in
one database transaction. Execution `unknown` remains distinct, and a failed
execution response is preserved alongside its failure metadata. Starting a process
still does not schedule an automatic observation.

Result-delivery settings are:

| Environment variable | Default | Meaning |
| --- | ---: | --- |
| `AGENT_GATEWAY_CALLBACKS` | `[]` | Callback connection IDs, kinds, and current/previous secrets |
| `AGENT_CALLBACK_TOLERANCE_MS` | `300000` | Accepted callback timestamp skew |
| `AGENT_MAX_CONCURRENT_RESULTS` | `32` | Shared callback-reconciliation and result-retrieval capacity |
| `AGENT_RESULT_FALLBACK_MS` | `30000` | Interval between fallback checks for nonterminal jobs |
| `AGENT_RESULT_RETRY_MS` | `5000` | Delay after result-retrieval failure |
| `AGENT_UNMATCHED_CALLBACK_RETENTION_MS` | `86400000` | Time to retain an unmatched callback before blocking it |

## HTTP server

The server exposes unauthenticated `GET /healthz` and `GET /readyz` probes. Session,
wait, and harness routes under `/v1` require a configured bearer service token;
gateway callback routes use their signed callback contract. The implemented API
supports harness discovery; session creation, listing, detail, and metadata updates;
durable message and cancellation admission; and wait listing, detail, and external
resolution. A session is created idle after its exact harness version validates the
configuration and initializes saved state. The API never exposes saved state or
processing leases, and list responses omit configuration as well.

Message, cancellation, and wait-resolution POSTs require an `Idempotency-Key`
header. A successful request returns `202 Accepted` with the durable event ID,
per-session event sequence, and acceptance time. This only acknowledges persistence:
it does not invoke or wait for a harness. Inputs remain admissible while another
event is running, while processing is paused, and in any activity status.

Wait replies use `{"response": ...}`. The server validates replies against an
optional Draft 2020-12 JSON Schema stored with the wait, allowing only local schema
references. Resolving the wait and inserting its `wait_resumed` event is one
transaction. Database wall-clock checks decide expiration races; an exact retry of
an already accepted reply returns its original acknowledgement.

The production registry includes `basic-codex` version `1`. Its server-owned
gateway routes default to `llm-primary` and `execution-primary`; override them
with `AGENT_LLM_CONNECTION_ID` and `AGENT_EXECUTION_CONNECTION_ID`. Tests can
still inject fixture registries without shipping those fixtures as production
harnesses.

HTTP errors use `{"error":{"code","message","request_id"}}`. The same request ID
is returned in `x-request-id`; request logs record that ID, method, matched route,
status, and duration. Unknown routes, disallowed methods, extractor failures, and
application errors use this envelope. Harness validation and initialization details
are intentionally not returned to callers.

Apply migrations explicitly, then start the server:

```bash
cargo run -p agent-server -- --database-url postgresql:///agent_platform migrate

AGENT_DATABASE_URL=postgresql:///agent_platform \
AGENT_SERVICE_TOKENS=replace-with-a-service-token \
cargo run -p agent-server -- serve
```

`AGENT_LISTEN` defaults to `127.0.0.1:8080`. Multiple service tokens can be supplied
as a comma-separated value during credential rotation. Session IDs are supplied by
the trusted backend; duplicate IDs return a conflict rather than making session
creation idempotent. Platform request fields use `snake_case`.

The database pool defaults to 10 connections and a 5-second acquisition timeout.
Configure these with `AGENT_DATABASE_MAX_CONNECTIONS` and
`AGENT_DATABASE_ACQUIRE_TIMEOUT_MS`. `AGENT_MAX_BODY_BYTES` defaults to 2 MiB, and
`AGENT_SHUTDOWN_GRACE_MS` defaults to 30 seconds. Startup rejects zero limits and
invalid service tokens before opening a database connection. `migrate` does not
require service tokens. On shutdown, readiness fails and new authenticated API work
is rejected while accepted requests drain up to the configured grace period.

## Persistence semantics fixed in Part 2

- The caller supplies a session UUID. `create_session` is deliberately not an
  idempotent create operation: reusing an existing UUID is a uniqueness error.
- `patch_session_metadata` changes only supplied fields. A supplied name may clear
  the name, and supplied metadata replaces the complete metadata object rather than
  merging keys.
- External-input idempotency is scoped by
  `(session_id, event_type, idempotency_key)`. The fingerprint covers the exact
  UTF-8 bytes of the raw JSON payload, so differently formatted JSON is treated as
  different input when the same key is reused.
- Event handling is strict FIFO within a session. A delayed or blocked head event
  prevents later events in that session from being claimed, while other sessions
  can continue.
- Operation inspection reads metadata and results without loading the large request.
  The dispatcher loads a request explicitly through `operation_request` or receives
  it with a submission/cancellation claim.
- `submitting` means gateway acceptance may still be unresolved and its request must
  be retained. `unknown` is a terminal outcome, emits one completion event, and may
  have its request removed after the explicitly assigned retention deadline.
- Submission, result retrieval, cancellation, and local withdrawal are distinct
  claim phases. Retrying result retrieval never makes an accepted gateway job
  eligible for submission again.
- Lease and wait-deadline decisions use the database wall clock after the relevant
  row locks are acquired. Expired workers therefore cannot commit after waiting on
  a lock.

Run `cargo test --workspace` from this directory to verify the public contracts and
the independent fixture harness. PostgreSQL integration tests create disposable
databases and are explicit because they require database-creation permission:

```bash
TEST_DATABASE_URL=postgresql:///postgres \
  cargo test -p agent-store --test postgres -- --ignored --test-threads=1

TEST_DATABASE_URL=postgresql:///postgres \
  cargo test -p agent-runtime --test postgres -- --ignored --test-threads=1

TEST_DATABASE_URL=postgresql:///postgres \
  cargo test -p agent-runtime --test dispatch -- --ignored --test-threads=1

TEST_DATABASE_URL=postgresql:///postgres \
  cargo test -p agent-runtime --test completion -- --ignored --test-threads=1

TEST_DATABASE_URL=postgresql:///postgres \
  cargo test -p agent-server --test api -- --ignored --test-threads=1
```
