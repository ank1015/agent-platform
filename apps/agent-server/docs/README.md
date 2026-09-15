# Agent server documentation

These pages describe the **implemented** `agent-server` application, not a proposed
API. The server exposes a trusted-backend HTTP API, receives signed gateway
callbacks, and supervises the workers that advance durable sessions. A real harness
is not bundled with the production registry yet; a server build must register the
harness crates it intends to serve.

| Page | Read it for |
| --- | --- |
| [Architecture](architecture.md) | Component boundaries, event ordering, transactions, gateway delivery, waits, cancellation, recovery, and updates |
| [Persistence](persistence.md) | The current tables, relationships, sequence and lease invariants, and transaction boundaries |
| [HTTP API](api.md) | Every route, request and response shapes, authentication, pagination, SSE, callbacks, and errors |
| [Configuration and operation](configuration.md) | CLI and environment settings, gateway connections, retention, observability, shutdown, and local verification |
| [Harness integration](harness-integration.md) | Registering a harness crate and the contract it must implement |

The server source and tests are the final authority if a page becomes stale. Start
with [`src/lib.rs`](../src/lib.rs) for routing and supervision, and
[`src/routes/`](../src/routes/) for the HTTP handlers.

## The shortest useful mental model

Create a session with an immutable harness/version/configuration. API inputs are
persisted as ordered session events and acknowledged before a harness runs. One
worker claims one event for a session, invokes the harness, and atomically commits
its new state and actions. Other workers dispatch those actions, resolve waits,
retrieve gateway results, and create new events. Conversation history and
application updates are separate from the internal event inbox. There is **no run
resource** and no automatic observation of an execution process.

This documentation does not include deployment topology or a production migration
runbook.
