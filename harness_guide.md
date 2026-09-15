# Writing a harness crate

Each harness is a Rust crate implementing the public `agent_contracts::Harness`
trait. It owns its `Config` and `State` types and exposes a stable ID and version
through `describe()`. Register it in `build_registry()` in `apps/agent-server/src/lib.rs`.
The server uses the exact registered version named by a session. Keep older
versions registered while their sessions still exist.

`validate_config()` checks the fixed configuration supplied at session creation.
`initialize()` creates the first checkpoint. A handler receives one event per
invocation and returns a `HandlerOutcome` through `OutcomeBuilder`. The platform
commits state, history entries, activity status, operations, waits, progress, and
event acknowledgement atomically. The handler can read bounded history and current
operation/wait outcomes through `HarnessContext`.

Handle `UserMessage`, `OperationCompleted`, `CancellationRequested`, and
`WaitResumed` as separate cases. Record operation and wait IDs in state so late
results can be correlated with the work that created them. A new user message can
arrive while older work is still outstanding. Session cancellation is an event;
your harness decides which requests to withdraw, which gateway jobs to cancel,
which waits to cancel, and whether to send execution controls. Execution process
observation is an explicit operation if your policy needs it.

For LLM and execution work, stage platform operations with stable IDs through the
builder. Gateway credentials and callback matching remain server responsibilities.
Use `create_wait()` for an external response, expiration, or either mode and
`cancel_wait()` for a selected wait. A cancelled wait emits a later `WaitResumed`
event. `emit_progress()` stores a small UI update with the outcome; it does not
stream progress while the handler is still executing.

Handler errors may retry the same event. A crash before commit also leaves the event
available. Any direct external call made inside `handle()` must therefore have its
own replay-safe identity. Platform operations avoid that issue because they are
submitted after the outcome commits. Watch `context.stop_requested()` during long
work and finish promptly if ownership is lost.

Treat serialized state as a versioned application format. Make schema changes in a
new harness version when old sessions cannot deserialize or behave consistently.
The separate `tests/harness-fixture` crate and the end-to-end mock-gateway tests
provide working examples of registration, state, event correlation, and durable
outcomes.
