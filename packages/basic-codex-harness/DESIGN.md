# Basic Codex Harness Design

## Purpose

`basic-codex-harness` is the default coding-agent harness for the GPT-5.6
model family. It reproduces the important Codex turn behavior on top of the
agent platform's durable event and operation model while deliberately omitting
permissions, sandboxing, subagents, and JavaScript code mode.

This document records the implemented behavior and scope of the harness.

## Supported models

The harness accepts only:

| Model | Default reasoning effort | Allowed reasoning efforts |
| --- | --- | --- |
| `gpt-5.6-sol` | `low` | `low`, `medium`, `high`, `xhigh`, `max` |
| `gpt-5.6-terra` | `medium` | `low`, `medium`, `high`, `xhigh`, `max` |
| `gpt-5.6-luna` | `medium` | `low`, `medium`, `high`, `xhigh`, `max` |

`ultra` is not supported because its Codex behavior includes automatic task
delegation. This harness has no subagent system.

All supported models use parallel tool calls.

## Session configuration

Configuration is immutable for the lifetime of a session.

```rust
pub struct BasicCodexConfig {
    pub account_id: Uuid,
    pub provider: BasicCodexProvider,
    pub model: CodexModel,
    pub reasoning_effort: ReasoningEffort,
    pub machine_id: Uuid,
    pub cwd: PathBuf,
    pub shell: Option<String>,
    pub platform: Option<String>,
    pub additional_instructions: Option<String>,
}
```

Validation at session creation will ensure that:

- `provider` is either `openai` or `chatgpt`;
- the model belongs to the supported GPT-5.6 family;
- the reasoning effort is supported by that model;
- `cwd` is absolute and otherwise satisfies the execution contract;
- optional `shell` and `platform` labels are nonempty, NUL-free, and bounded;
- strings and identifiers satisfy defined size and shape limits.

The harness instance, rather than each session configuration, will contain the
platform's LLM and execution gateway connection IDs. They select server-owned
gateway connections and are not choices a session caller needs to make.

The following are not session configuration fields:

- **Service tier:** the provider default is used.
- **Execution generation ID:** it is learned from execution responses and
  retained in durable state for later operations against an existing process.
- **Environment variables:** commands inherit the execution host's configured
  environment. Additional environment configuration is not included initially.

## Conversation history

Conversation history is append-only. The harness appends every model-relevant
or audit-relevant item in causal order, including:

- every user message, including steering messages;
- every ordinary assistant response, including assistant responses containing
  tool calls and the final assistant response;
- every tool result, including errors and unknown outcomes;
- harness-created custom messages such as provider-native compaction
  checkpoints.

The final assistant message is not the only assistant message stored. It is only
the assistant message that ends the active turn.

Internal platform inbox events and operation records remain separate from
conversation history. The harness converts an operation result into a
conversation tool result only when that operation belongs to a model tool call.

Compaction never deletes or rewrites the complete stored conversation. It
changes the bounded projection selected for subsequent model requests.

For now every LLM operation is fresh and carries the complete active
model-visible projection. The harness persists that causal projection in its
state separately from chronological platform history. This distinction matters
when steering is admitted while an LLM operation is in flight: the steering is
recorded in platform history immediately, but appears after that operation's
assistant response in the next model request.

## Durable state

The initial durable state design contains:

- a harness-state schema version;
- the current phase;
- an optional active logical turn ID;
- the current and last successful LLM operation IDs;
- pending steering messages in admission order;
- the current ordered tool-call plan;
- tool call IDs, operation IDs, status, results, and original ordinal positions;
- execution handles and their runtime generation IDs where needed;
- the current compaction checkpoint and active context window;
- context/token estimates;
- cancellation information;
- retry counters and the information needed to classify/retry failures.

Expected phases are:

- `idle`
- `awaiting_llm`
- `executing_tools`
- `compacting`
- `cancelling`

There is no `waiting_for_user` phase in the first version because the harness
does not expose `request_user_input`.

State must contain enough correlation information to make every event
idempotent. Replaying a completion event or restarting between an outcome commit
and the next claim must not submit the same logical action twice.

## Turn lifecycle

A session has at most one active logical turn. A platform `run` record is not
introduced.

When a user message arrives while the session is idle:

1. Append the user message to conversation history.
2. Create an active turn in state.
3. Build and stage a fresh LLM request.
4. Set the session status to `running`.

When the LLM completes:

1. Read and validate the correlated operation outcome.
2. Preserve the native provider response items required for replay.
3. If it is an ordinary assistant response, append it to history.
4. Extract and validate all tool calls.
5. If tool calls exist, build and begin the ordered tool plan.
6. If no tool calls exist and the response is terminal, end the active turn and
   set the session status to `idle`.

The provider-native assistant message is the conversation item saved and
replayed. The response stop reason remains operation metadata: it is not
converted into a synthetic custom message and is not sent back to the model.
`stop` and `refusal` finish the turn when no steering is pending; otherwise they
stage another fresh full-history request containing the steering. `pause_turn`
always stages another fresh full-history request. `length` and
`content_filter` preserve any returned assistant item and fail the session,
matching Codex's treatment of incomplete responses as errors. Until the tool
phase is implemented, `tool_use` also fails rather than silently discarding
calls.

After a tool plan finishes, append all tool results in original model order and
stage one fresh request containing the complete active projection. The loop
continues until a terminal assistant response is produced with no pending
steering, or the session is cancelled or failed. Provider continuation
operations are intentionally deferred.

There is no initial hard limit on the number of model/tool cycles. Context
compaction bounds model input. Enforced handler deadlines and a turn-step limit
remain deferred unless operating experience shows they are needed.

## Event handling and ordering

The platform invokes the harness with one event at a time. The harness never
mutates its state concurrently.

If an operation finishes or another user message arrives while a handler is
executing, the platform persists that event. It is handled only after the
current invocation commits.

The order visible to the harness is the session event sequence allocated by the
platform. The harness uses operation and tool-call IDs for correlation rather
than assuming that parallel operations finish in dispatch order.

## Steering

A user message received while a turn is active is steering for that turn. The
trusted caller owns ordinary input queueing and submits queued ordinary messages
only after the session is idle; the harness has no separate generic queued-input
concept.

The message is appended to conversation history immediately and placed in the
durable pending-steering queue. It does not change an LLM request or execution
operation that has already been dispatched.

At the next safe model boundary, pending steering is drained in event order and
included after the just-produced assistant response in the next fresh,
full-projection LLM request. A safe boundary is reached after the current
LLM response and its required tool-result batch have been processed, or before
the next sample when no tool work is outstanding.

Several steering messages may be delivered together. They remain distinct user
messages rather than being concatenated into an undocumented synthetic message.

## Parallel tool calls

The harness preserves Codex-style ordered parallel execution:

- parallel-safe calls may execute concurrently;
- an exclusive call executes alone and is ordered against all other calls;
- model call order determines tool-result order;
- physical operation completion order does not determine history order;
- one failed tool normally produces an error tool result and does not discard
  the results of sibling calls;
- a fatal harness inconsistency aborts the turn instead of being presented as a
  normal tool failure.

The harness converts the model's ordered call list into execution segments. A
contiguous group of parallel-safe calls forms a parallel segment. An exclusive
call forms a segment by itself. Only the current segment is dispatched; the
next segment begins after the current one has settled.

All results for the assistant response are buffered durably. They are appended
to conversation history in original call order before the next fresh LLM
request is staged.

## Initial tools

The first version exposes four direct tools:

| Tool | Scheduling behavior | Purpose |
| --- | --- | --- |
| `exec_command` | parallel-safe | Start a command and return its initial observation |
| `write_stdin` | parallel-safe | Write to or explicitly observe an existing execution |
| `apply_patch` | exclusive | Apply a structured patch on the execution machine |
| `view_image` | parallel-safe | Read a local image and return it as model input |

The model-facing definitions and descriptions match the current Codex tools for
a single environment. `exec_command`, `write_stdin`, and `view_image` are JSON
function tools; `apply_patch` is a custom tool using Codex's Lark patch grammar.
Every fresh LLM request advertises all four definitions and enables parallel
tool calls. Execution-protocol mapping, output truncation, and handle allocation
are settled with each concrete implementation.

The first version does not expose:

- `request_user_input`;
- JavaScript code mode;
- subagent tools;
- MCP, apps, plugins, or skills;
- web search or image generation;
- permissions or approval tools.

Completion of a platform execution operation and completion of the process it
started are separate. The `exec_command` tool call completes when its execution
operation returns the initial bounded observation. That observation can contain
a session ID while the process remains alive. Long-running command completion
is not observed or delivered to the harness automatically. The model must later
call `write_stdin` to poll the process or supply input; that new tool call stages
a new platform execution operation.

An execution response's generation ID is stored with a running handle. Later
write, observe, interrupt, or terminate operations use that generation as an
optimistic guard so a handle cannot accidentally target a replacement runtime.

Provider-native tool calls are parsed into a durable ordered plan before any
execution is attempted. Calls are correlated by unique call ID, validated
against their advertised kind and schema, and split into contiguous
parallel-safe or exclusive segments. Assistant content is preserved verbatim.
Parallel results may settle in any order but tool-result messages are buffered
and appended in original call order. Known tools with invalid arguments receive
a model-visible error result. Missing or duplicate correlation IDs, unknown
tools, kind mismatches, and stop-reason contradictions are provider protocol
failures.

All four tools are implemented through the execution gateway. `apply_patch`
first reads every affected path, applies Codex patch matching in memory, and
then submits a sequential batch of content-addressed writes/removes. The batch
is fenced to the runtime generation returned by the reads and uses per-file
SHA-256 or missing-file preconditions. Mutation IDs are stable in durable
harness state. An unknown mutation outcome is reconciled by reading the intended
final paths instead of blindly resubmitting.

`view_image` reads at most 5 MiB through the execution filesystem protocol,
validates and bounds the image, and uploads the prepared bytes through a
server-owned image bucket. The model-visible tool result contains the returned
public HTTP(S) URL with `high` or `original` detail; data URLs are not used.

### Shell execution

`exec_command` maps to `execution.start`. Its stable runtime `start_id` derives
from the logical turn and tool ordinal. The immutable session working directory
is used by default; model-provided relative directories resolve against it
without interpreting remote paths through the agent server's operating system.
Commands use the runtime's default shell unless the model supplies a recognized
sh, bash, zsh, PowerShell, or cmd executable. This basic harness is deliberately
unrestricted: permission-related arguments remain wire-compatible with Codex
but do not trigger approval or sandbox transitions.

Every start requests the execution runtime's shell snapshot using the platform
session ID as its stable cache scope. This restores the connected machine user's
interactive profile environment and shell state before the command, so profile-
managed tools are available even when the host daemon was launched by a GUI service.
Snapshot capture, caching, limits, and fail-open behavior belong to the execution
runtime; captured values never pass through the agent or execution gateways.

The initial observation is formatted as a Codex-style tool result. A completed
process reports its exit code. An active process receives a durable numeric
session ID beginning at 1000; the harness privately retains its UUID handle,
runtime generation, terminal mode, and opaque output cursor. Numeric IDs are
never reused during the agent session.

`write_stdin` with empty input maps to an explicit `execution.observe`. Nonempty
input maps to a sequential batch containing idempotent `execution.write_input`
followed by `execution.observe`. Ctrl-C against a non-terminal process uses
`execution.interrupt`. All interactions are fenced to the handle's runtime
generation. Calls against distinct process IDs may run concurrently, while
calls against one process are serialized in model order.

One model tool call may stage multiple zero-wait observations to drain output
pages. This is bounded to 16 pages and occurs only while fulfilling that
explicit call; the harness never watches the process after returning a running
session ID. Cursor progress, a bounded head/tail output accumulator, the chunk
ID, and the active execution operation are all durable across restarts.

Model-visible output includes chunk ID, durable end-to-end wall time, exit code
or running session ID, approximate original token count, and bounded output.
Runtime retention gaps, incomplete capture, and remaining paginated output are
reported explicitly. Nonzero command exits are successful tool invocations;
launch loss, protocol/gateway failures, cancellation, and unknown outcomes are
tool errors. Unknown work is never automatically replayed.

## Provider-native compaction

Compaction uses the same provider-native Responses compaction mechanism as
Codex. A normal model-written summary is not an acceptable replacement.

For Responses compaction v2, the harness:

1. Select the current bounded model-visible history.
2. Add the provider-native `compaction_trigger` request item after that history.
3. Send the request with the same model, base instructions, tool definitions,
   and parallel-tool setting used for the active turn.
4. Require and validate the provider's native compaction output, including its
   encrypted content and response identity.
5. Construct the next active context from the provider compaction item and the
   retained context required by the Codex compaction algorithm.
6. Record a custom compaction checkpoint in complete conversation history.
7. Start the following model step as a fresh LLM request containing the compacted
   projection. It must not continue a gateway parent whose request still
   contains the pre-compaction history.

The existing message contract can carry provider-native controls and outputs as
custom messages. The provider adapters recognize their provider-specific custom
item tags and expand their `data.content` directly into Responses input:

- `openai_custom_item` for the OpenAI provider;
- `chatgpt_custom_item` for the ChatGPT provider.

The native compaction checkpoint preserves enough information to rebuild
the compacted prompt exactly after a restart, including:

- provider identity;
- native compaction output item;
- provider response ID;
- source context boundary;
- retained user messages and their order;
- context-window generation;
- relevant usage measurements.

Complete conversation history remains available to the application. The active
prompt window is a projection and may omit pre-compaction entries.

Initial automatic compaction follows the GPT-5.6 Codex profile:

- a 272,000-token active context budget;
- automatic compaction at approximately 90 percent of that budget;
- compaction only at a safe model boundary;
- no splitting of an assistant tool-call item from its corresponding tool
  result;
- the same pre-turn versus mid-turn context placement rules as Codex.

Ordinary and compaction requests set `store: false`, include encrypted reasoning,
and advertise `remote_compaction_v2`. A compaction response must stop normally
and contain exactly one `compaction` (or compatibility alias
`compaction_summary`) item with nonempty encrypted content. The active projection
then becomes up to 64,000 approximate tokens of the most recent user messages,
followed by the encrypted native item and any post-compaction input.

Pre-turn compaction holds the newly admitted user message outside the request
being compacted. Mid-turn compaction runs after an assistant/tool-result boundary;
steering admitted while it is in flight remains outside the compacted source and
is appended only after the new checkpoint. The source projection is fingerprinted,
and the full compacting phase is persisted, so restart recovery cannot install a
result over a different projection. An unknown compaction result is reissued once;
definitive failure, a second unknown result, or malformed native output fails the
session instead of falling back to ordinary summarization.

## Cancellation

The existing platform cancellation endpoint remains terminal for the session.

On `cancellation_requested`, the harness will:

- enter `cancelling`;
- withdraw every correlated operation before choosing gateway-specific cleanup;
- request cancellation when an LLM withdrawal loses the submission race;
- stop every tracked process owned by the session, including processes retained
  from earlier turns, by interrupting first and then terminating with a bounded
  grace period when it remains active;
- stop scheduling undispatched tool-plan segments;
- discover active processes by session/turn labels after an unknown execution
  outcome, without repeating the command;
- preserve cleanup targets, controls, attempts, and failures in durable state so
  any completion can resume cancellation after a restart;
- consume late outcomes for cleanup and audit without adding model-visible
  assistant/tool messages or allowing them to reactivate work;
- finish the session as `cancelled` after required cleanup settles.

Cancellation controls and process discovery receive at most one replacement
attempt after a terminal unknown/failure. Cleanup failures remain recorded in
the cancellation tombstone. Repeated cancellation events are no-ops and the
first cancellation reason remains authoritative. User messages already admitted
behind cancellation are saved for audit but cannot restart the session.

A future non-terminal "interrupt current turn" operation may be designed
separately. It is not part of this harness version.

## Failure behavior

- Invalid tool names or arguments become model-visible error tool results.
- Command and patch failures become tool results with useful, bounded details.
- Admission, gateway, and execution failures for tool operations become tool
  error results when the model can reasonably react to them.
- Execution `unknown` is preserved as unknown and is never reported as success.
- An ordinary or compaction LLM `unknown` outcome is retried exactly once with a
  fresh operation and the same full model projection. Commands are never
  automatically repeated.
- A definitive LLM failure, a second unknown outcome, unexpected cancellation,
  terminal length/content-filter stop, or malformed provider output fails the
  session with a durable failure record.
- Invalid durable state and impossible event/operation correlations remain
  handler errors so the platform retry/blocking policy exposes implementation or
  state-corruption defects instead of silently resetting state.

## Prompt and workspace context

Every request uses the versioned adapted Codex instructions in
`assets/base_instructions.md`. Unsupported Codex behavior is excluded:
permissions, sandboxing, subagents, plan/collaboration modes, plugins, skills,
and user-input tools. Immutable `additional_instructions` follow the base prompt
under a distinct session-instructions heading.

The harness deliberately performs no workspace bootstrap and does not discover
or interpret `AGENTS.md`. Its stable model projection prepends one synthetic user
message containing XML-shaped environment context. `cwd` and the current turn's
date are always present; immutable `shell` and `platform` labels are included
only when configured. This synthetic message is not written to public
conversation history. It is rebuilt identically for retries, tool boundaries,
and compaction recovery, while the date remains fixed for the active turn.

Every ordinary and compaction request uses the platform session UUID as
`prompt_cache_key`. Base instructions, additional instructions, tool definitions,
and environment context therefore form the same deterministic prefix throughout
an active turn. The provider adapters own transport-specific behavior: ChatGPT
forces streaming transport, `store: false`, and encrypted reasoning inclusion;
the OpenAI request explicitly carries `store: false` and encrypted reasoning
inclusion while remaining non-streaming at the harness boundary.

## Streaming

The harness does not expose LLM token or output-item streaming. LLM operations
produce terminal responses through the existing gateway lifecycle.

Parallel tool calls begin after the terminal LLM response is available. The
harness preserves Codex's concurrency, exclusivity, and ordered replay semantics
but does not attempt to reproduce the latency optimization of launching tools
while provider output items are still streaming.

## Proposed module structure

```text
src/
  lib.rs
  config.rs
  model.rs
  state.rs
  handler.rs
  prompt.rs
  response.rs
  steering.rs
  compaction.rs
  tool_plan.rs
  tool_output.rs
  tools/
    mod.rs
    exec_command.rs
    write_stdin.rs
    apply_patch.rs
    view_image.rs
```

The public surface of the crate should primarily expose the harness type,
configuration schema, supported model/reasoning enums, and a constructor that
accepts server-owned gateway connection routing.

## Deferred scope

The following are explicitly outside the initial implementation:

- permissions, approvals, and sandboxing;
- subagents and `ultra` reasoning;
- model or reasoning changes after session creation;
- user-input waits;
- JavaScript code mode;
- live LLM streaming;
- MCP, plugins, skills, hooks, and memory;
- review and plan modes;
- web search and image generation.
