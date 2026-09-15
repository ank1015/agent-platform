# Integrating a harness with the server

The production `build_registry()` in [`src/lib.rs`](../src/lib.rs) currently creates
an empty `HarnessRegistry`. Before the server can create useful sessions, add the
real harness crate as a dependency of `agent-server` and register its versioned
adapter there. The fixture in
[`tests/harness-fixture`](../../../tests/harness-fixture/) is a working example; it
is used by tests, not automatically loaded by the production binary. Harness code
is compiled into the server. There is no install/upload endpoint or dynamic loader.

Implement `agent_contracts::Harness` with two owned serialized types:
`Config` (the session-lifetime immutable settings) and `State` (a durable
checkpoint). The trait exposes `describe`, optional `validate_config`,
`initialize`, and async `handle`. `Config` must also implement `JsonSchema`, so
the discovery endpoint can publish a configuration schema. Initialization runs
once on creation and must not stage platform operations. `handle` receives the
fixed config, prior state, one `SessionEvent`, and a session-scoped
`HarnessContext`; it returns `HandlerOutcome<State>` or a handler error.

`OutcomeBuilder` is the normal way to construct that result. It can append
conversation messages, set the session status, stage LLM requests, execution
start/observe/control requests, LLM cancellation and withdrawal, create or cancel
waits, and emit a small progress update. Save returned operation and wait IDs in
your state so future completion/resumption events can be matched with the action
that caused them. The platform atomically validates and commits the complete
outcome. A new input may have arrived while the handler was executing, but it will
be delivered in a later invocation; the current handler does not receive a live
message feed.

The context offers bounded history reads up to the invocation's captured
`history_through_sequence`, plus operation and wait reads in the current session.
It also exposes `stop_requested()`. Use that signal to stop long work if the
claim is lost or the server is draining. Handler errors can replay the same event;
direct external I/O inside `handle` therefore needs its own replay-safe identity.
Platform-staged gateway operations are dispatched only after the outcome commits.

Keep the exact harness ID/version registered while sessions of that version still
exist. A code or serialized-state change that cannot process those sessions
consistently should use a new version rather than silently replacing the old
handler. Session detail exposes its registered version and fixed configuration,
while `/v1/harnesses/.../versions/...` exposes the advertised schema.

The [architecture guide](architecture.md) explains cancellation, observation, and
retry behavior. The Rust contracts live in
[`agent-contracts`](../../../packages/agent-contracts/src/).
