# Configuration and local operation

The `agent-server` binary has `serve` and `migrate` subcommands. This page
describes its implemented settings and local verification; it is **not** a
deployment topology or production migration procedure. The CLI uses long flags
corresponding to the environment variables below. `Config::validate` checks
numeric settings, token format, gateway/callback JSON, and matching connection
kinds before `serve` starts. `serve` also requires the database to have the exact
expected schema.

## Core settings

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `AGENT_DATABASE_URL` | required | PostgreSQL connection URL |
| `AGENT_DATABASE_MAX_CONNECTIONS` | `10` | SQLx pool maximum connections per process |
| `AGENT_DATABASE_ACQUIRE_TIMEOUT_MS` | `5000` | Pool acquisition timeout |
| `AGENT_LISTEN` | `127.0.0.1:8080` | Bind address for `serve` |
| `AGENT_IMAGE_BACKEND` | `local` | Image publisher: `local` or `gcs` |
| `AGENT_IMAGE_BUCKET` | unset | GCS bucket name; required when the backend is `gcs` |
| `AGENT_IMAGE_PUBLIC_BASE_URL` | `http://127.0.0.1:8080` | Public origin for the `local` backend only |
| `AGENT_SERVICE_TOKENS` | required | Comma-separated backend bearer tokens; permits rotation |
| `AGENT_MAX_BODY_BYTES` | `2097152` | Maximum request body, including callbacks |
| `AGENT_MAX_CONCURRENT_SSE` | `200` | Simultaneous update streams **per process** (1–10000) |
| `AGENT_SHUTDOWN_GRACE_MS` | `30000` | Maximum graceful drain after signal or supervisor stop |

Service tokens must be nonempty and contain no whitespace. They are checked in
constant time. Keep service tokens, gateway bearer tokens, and callback secrets
out of source files and logs. The CLI hides environment values for credential
fields; gateway connection `Debug` redacts bearer tokens. The trusted backend
remains responsible for user/project authorization.

`view_image` publishes prepared images through a platform-owned asset publisher.
Images are limited to 5 MiB and content-addressed as `{sha256}.{extension}` so a
replayed handler safely targets the same immutable object. With `local`, the
server stores bytes in memory and serves unauthenticated
`GET /media/images/{sha256}.{extension}`; these objects do not survive restart.
With `gcs`, the server uploads to `AGENT_IMAGE_BUCKET` with a create-only
generation precondition and returns
`https://storage.googleapis.com/{bucket}/{sha256}.{extension}`. The bucket must
allow unauthenticated reads of known objects because the model fetches the URL.
It should deny public bucket listing. Do not expire these objects while retained
conversation history can still reference them.

The GCS client uses Google Application Default Credentials. Local development
can use `gcloud auth application-default login`; deployed workloads should
attach a service account with only `storage.objects.create` on the bucket. The
provided dev script defaults to `gcs` and bucket
`agent-platform-images-361197090477`; set `AGENT_IMAGE_BACKEND=local` to use the
in-memory fallback.

## Session scheduler and handler execution

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `AGENT_SCHEDULER_ENABLED` | `true` | Run the session scheduler |
| `AGENT_MAX_CONCURRENT_HANDLERS` | `32` | Active handler invocations per process (1–1024) |
| `AGENT_SCHEDULER_POLL_MS` | `100` | Scan interval when work is available |
| `AGENT_HANDLER_LEASE_MS` | `30000` | Session claim lifetime |
| `AGENT_HANDLER_LEASE_RENEWAL_MS` | `10000` | Lease renewal interval |
| `AGENT_HANDLER_RETRY_DELAYS_MS` | `1000,5000` | Comma-separated harness-error delays; delays plus one = attempt budget |
| `AGENT_HANDLER_COMMIT_RETRY_DELAYS_MS` | `10,50` | Safe prepared-outcome transaction retries |
| `AGENT_INFRASTRUCTURE_RETRY_DELAY_MS` | `1000` | Delay after a platform-infrastructure attempt |
| `AGENT_OWNERSHIP_LOSS_GRACE_MS` | `100` | Cooperative stop grace after claim loss |

No general handler execution deadline is enforced yet. A handler should monitor
`context.stop_requested()` and avoid irreversible direct I/O unless it has its
own replay-safe identity. Handler-error exhaustion blocks the session's head
event; `/v1/sessions/{sessionId}/processing/retry` is the guarded recovery path.

## Operation dispatch and result retrieval

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `AGENT_DISPATCHER_ENABLED` | `true` | Run gateway dispatch |
| `AGENT_MAX_CONCURRENT_DISPATCHES` | `32` | Active gateway dispatch calls per process (1–1024) |
| `AGENT_DISPATCH_POLL_MS` | `100` | Operation scan interval; also used by result completion |
| `AGENT_OPERATION_LEASE_MS` | `30000` | Operation claim lifetime |
| `AGENT_OPERATION_LEASE_RENEWAL_MS` | `10000` | Operation claim renewal interval |
| `AGENT_OPERATION_RETRY_DELAYS_MS` | `1000,5000,30000` | Comma-separated dispatch retry delays |
| `AGENT_OPERATION_DEPENDENCY_DELAY_MS` | `250` | Check interval for operations awaiting parent/target mapping |
| `AGENT_OPERATION_RESULT_CHECK_DELAY_MS` | `1000` | First accepted-job check delay |
| `AGENT_MAX_CONCURRENT_RESULTS` | `32` | Combined callback reconciliation/result capacity per process (1–1024) |
| `AGENT_RESULT_FALLBACK_MS` | `30000` | Fallback authoritative-result check without callback |
| `AGENT_RESULT_RETRY_MS` | `5000` | Retry after result retrieval failure |
| `AGENT_UNMATCHED_CALLBACK_RETENTION_MS` | `86400000` | Retention horizon for unmapped callback receipts |
| `AGENT_OPERATION_REQUEST_RETENTION_MS` | `604800000` | Retain an operation's request seven days after a terminal outcome |

These concurrency limits are per server process. PostgreSQL claims and lease
fencing keep multiple processes from making the same authoritative commit. The
stable gateway submission identity survives a lost HTTP response; request payload
retention must not remove unresolved submissions.

## Waits, cleanup, and updates

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `AGENT_WAIT_EXPIRATION_ENABLED` | `true` | Process due `expiration`/`either` waits |
| `AGENT_WAIT_EXPIRATION_POLL_MS` | `100` | Wait scan interval |
| `AGENT_WAIT_EXPIRATION_BATCH_SIZE` | `100` | Waits examined per bounded sweep (1–1000) |
| `AGENT_REQUEST_CLEANUP_ENABLED` | `true` | Physically remove eligible operation requests |
| `AGENT_REQUEST_CLEANUP_POLL_MS` | `60000` | Request cleanup scan interval |
| `AGENT_REQUEST_CLEANUP_BATCH_SIZE` | `100` | Requests examined per keyset sweep (1–1000) |
| `AGENT_UPDATE_RETENTION_MS` | `604800000` | Retain replayable UI updates seven days |
| `AGENT_UPDATE_CLEANUP_POLL_MS` | `60000` | Update pruning scan interval |

Request cleanup is deliberately conservative. An expiration is assigned when
work settles. Cleanup deletes only the request for a terminal `succeeded`,
`failed`, `cancelled`, or `unknown` operation after that deadline, and records
`request_removed_at` plus an `operation.changed` update atomically. Pending,
submitting, accepted, and uncertain work retains its sole request regardless of
nominal age. Locked candidates are skipped and revisited. Operation outcome and
attempt records are retained. The update log is separate and its expired cursor
returns `410 resnapshot_required`.

## Gateway connections and signed callbacks

`AGENT_GATEWAY_CONNECTIONS` is a JSON array of outbound HTTP connections, and
`AGENT_GATEWAY_CALLBACKS` is a JSON array of inbound verification configurations.
Both default to `[]`. Every callback config must match an outbound connection with
the same `id` **and** `kind`; a callback is optional for a connection because
fallback result checking still works.

```json
[
  {
    "id": "llm-primary",
    "kind": "llm",
    "base_url": "https://llm-gateway.example/api/",
    "bearer_token": "secret-outbound-token",
    "timeout_ms": 30000
  }
]
```

```json
[
  {
    "id": "llm-primary",
    "kind": "llm",
    "secrets": ["active-callback-secret", "previous-callback-secret"]
  }
]
```

Kinds are `llm` or `execution`. Connection IDs must be 1–200 non-whitespace
characters and unique within each array. Base URLs must be HTTP(S) bases ending
in `/`, with no userinfo, query, or fragment. Gateway bearer tokens must be
nonempty; callback configs need at least one nonempty secret. Multiple callback
secrets support rotation. `AGENT_CALLBACK_TOLERANCE_MS` defaults to `300000`
(five minutes). Harnesses reference these configured connection IDs; they do not
receive or choose credentials at runtime.

## Logging, metrics, and readiness

The binary emits structured JSON logs by default. `RUST_LOG` overrides the
default filter (`agent_server=info,agent_runtime=info,agent_store=warn`). Request
logs include a generated `request_id`, method, route, status, and duration.
Worker logs carry session/operation/wait correlation fields where available and
should not contain request payloads or credentials.

Authenticated `GET /v1/metrics` returns Prometheus text. HTTP status-class counts,
latency, and live SSE subscriptions are **per process**. Pending events,
operations, active leases, overdue waits, cleanup-ready requests, and removed
request counts are derived from PostgreSQL and represent shared state. Do not sum
the shared gauges across replicas. A metrics scrape requires a database read;
database failure is reported as an API error.

`/healthz` checks process liveness. `/readyz` checks that the process is still
accepting work and that PostgreSQL has the exact expected migration set and core
tables. It does not guarantee that each gateway account, machine, or callback
destination is usable. When shutdown starts, readiness turns off before workers
drain and new API/callback admission receives `503 shutting_down`.

## Local verification

From the `agent-platform` workspace, compile and run ordinary tests with
`cargo test --workspace`. The PostgreSQL integration tests are marked ignored
and create/drop disposable databases. Point `TEST_DATABASE_URL` at a PostgreSQL
admin database where the test role can create databases, then run:

```sh
TEST_DATABASE_URL=postgresql:///postgres sh scripts/verify.sh
```

That script checks Rust formatting, ordinary tests, and all ignored database
tests serially. The fixture harness and mock gateways exercise duplicate input,
lost submission response, early callback, expired lease, stale handler, wait
races, cancellation, restart recovery, cleanup, metrics, and graceful shutdown.
The production registry includes the `basic-codex` harness; see
[harness integration](harness-integration.md).
