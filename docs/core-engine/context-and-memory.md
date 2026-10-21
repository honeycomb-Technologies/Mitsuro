# Context, Memory & Summarization

Every time you send a message to Mitsuro, the system builds a carefully structured context payload before the AI sees anything. That payload includes project instructions, environment details, persistent memories, active plans, available skills, and the conversation history itself. As conversations grow long, the system compresses older content in place to stay within the model's context window. Manual `/pinch` triggers the same in-place compaction pipeline on demand.

This document explains each of those mechanisms: what gets injected, how the system tracks what it has already told the model, how it decides what to keep and what to trim, and how it bridges the gap between sessions.

## The Context Problem

Large language models have a fixed context window. Even the largest models available today cap out at a few hundred thousand tokens. A coding conversation that spans several hours of exploration, editing, and debugging can easily produce hundreds of messages, each carrying file contents, tool outputs, diff previews, and reasoning traces. Without active management, the raw conversation would blow past the context limit long before the work is done.

Mitsuro addresses this at two levels. First, it controls what goes into the context at the start of every turn through context injection. Second, it compacts the conversation in place when it grows too large—or when the provider rejects an over-limit request—keeping the same session alive without starting over.

## Context Injection

Before every AI call, the `inject_context` function in `context.rs` builds a new message array by prepending system messages ahead of the actual conversation. The model never sees these as user messages or part of the chat history. They're framing: instructions, state, and environmental awareness that shape how the AI responds.

The injection follows a fixed order:

1. **Workspace context** -- Tells the model where it is. In project mode, this identifies the project root and execution directory. In neutral mode (no project selected), it instructs the model not to assume any particular codebase.

2. **Environment context** -- Runtime facts gathered at call time: the working directory, current git branch, file change counts, platform, shell, date, and which model is active. Git commands that fail (no repo, missing binary) are silently skipped rather than producing errors.

3. **Persistent memory** -- Memories that survive across sessions, pulled from the SQLite memory store. Short previews are selected by meaningful overlap with the latest user objective, organized by type (user context, feedback/guidance, project context, external references), and capped at three entries per type, 180 characters per entry, and 2 KiB total. Generic words such as “project” or “code” do not qualify by themselves.

4. **Project instructions** -- Content from instruction files discovered in the project directory. The system walks from the project root down to the current working directory, checking each level for files like `KRAB.md`, `AGENTS.md`, `CLAUDE.md`, `.cursorrules`, and others. If multiple levels contain instruction files, all of them are included, creating a layered instruction set where subdirectories can refine or extend the root rules.

5. **Project settings** -- An optional `system_prompt_append` field from the project's settings, allowing per-project prompt customization beyond what the instruction files provide.

6. **Plan state** -- If there's an active plan for the current session, the full plan context is injected. In build mode, this includes the plan markdown, progress counts, ready tasks, blocked tasks, and a workflow protocol for picking and completing tasks. In plan mode, it reminds the model that it can read but not write.

7. **Delegated runs** -- Recent sub-agent investigations (explore, build, planner, verifier runs) for the session. This context tells the model that prior delegated work exists and encourages reusing those results rather than re-exploring the same directories.

8. **Autonomous tasks** -- Any Hive tasks associated with the session, grouped by status (pending, in progress, completed).

9. **Reports** -- Recent investigation reports for the project, shown as title/date/summary with a pointer to use the `ReadReport` tool for full content.

10. **Coordinator context** -- For Hive sessions specifically, a specialized coordinator system prompt is injected.

11. **Skills** -- A listing of all available skills with names, descriptions, and tags, plus instructions on how to invoke them.

Chat sessions get a stripped-down version of this: just a chat-specific system prompt and persistent memories. No workspace, no tools, no plans.

## The Context Ledger

The context ledger, defined in `context_ledger.rs`, is a bookkeeping structure that tracks the state of the conversation from a continuation standpoint. It answers two questions: "What has happened to this conversation's context?" and "Can this session be resumed?"

Every time the conversation changes, the ledger recomputes its counts by scanning all messages:

- **Canonical messages** -- Total messages in the conversation.
- **Pinned messages** -- System messages containing project instructions, which must survive compaction.
- **Summarized messages** -- Tool results tagged with `summarize_after_turn` retention, meaning they've already been condensed.
- **Dropped messages** -- Tool results tagged with `drop_after_compaction`, meaning they can be removed entirely during compaction.
- **Latest user objective** -- The text of the most recent user message, extracted by scanning the conversation in reverse.

The ledger uses this state to make a **continuation decision**: is this session resumable? A session is resumable if it has at least one canonical message and a non-empty latest user objective. It's non-resumable if the conversation is empty or there's no identifiable user goal (for example, if the most recent user message contained only a tool result with no text).

When compaction occurs, the ledger records a snapshot: the reason for compaction, the estimated token counts before and after, and how many messages were replaced. This creates an audit trail that makes context decisions traceable rather than opaque.

The ledger also produces a **continuation contract** -- a serializable record that can be persisted alongside the session, capturing the schema version, the resumability decision, and the latest objective. This is what downstream systems (like pinch) use to determine whether and how to create a continuation.

## Conversation Compaction

Compaction keeps a long-running session alive when the conversation approaches the model's context limit. It operates **in place**: the same session ID, database history, and UI thread continue after compaction. The pipeline lives in `crates/mitsuro-core/src/agent/compaction/`.

### Triggers

| Trigger | When it runs |
|---------|--------------|
| **Auto** | Estimated tokens cross the model's compaction threshold before a turn |
| **Manual** | User runs `/pinch` (TUI) or the pinch API route (server/mobile) |
| **Overflow** | Provider returns a context-length / HTTP 413 error; orchestrator compacts once and retries |

### Pipeline

1. **Microcompact** — strip old thinking blocks and compact stale tool results using history retention policies.
2. **Memory flush** — write a project-scoped compaction note to the memory store (equivalent to `/flush`) before summarization so durable facts survive even aggressive cuts.
3. **Cut + summarize** — choose a cut point that preserves a recent tail, LLM-summarize the dropped segment with ranked file context, and persist a checkpoint plus segment archive.
4. **Apply** — replace canonical session messages with a compaction boundary, structured summary, and preserved tail; emit `ContextCompacted` so clients refresh token counts in place.

`CompactionManager` configures per-model trigger/target thresholds. Checkpoints and segments are stored in `compaction_checkpoints` and `compaction_segments`. Agents can recover dropped detail with the `search_compaction_segments` tool.

## History Policies

The history policy system in `history_policy.rs` shapes how tool results are stored in conversation history from the moment they're created, before compaction ever runs. Every tool result passes through `build_history_tool_result`, which assigns it a retention classification and builds both a human-readable summary and a compact representation.

The three retention levels are:

- **Retain full** -- The complete result is kept in history, bounded to prevent extreme sizes (16K chars for file content, 6K for diffs, 3K for command output). This is the default, used for `read` results and other tools not explicitly classified.

- **Summarize after turn** -- The result is replaced with a structured summary after the current turn. Used for `grep`, `glob`, `list`, `write`, `edit`, `multiedit`, and `apply_patch`. The summary preserves key facts (match counts, file paths, line counts, diff previews) while discarding the bulk content.

- **Drop after compaction** -- The result can be entirely removed during compaction. Used for `bash`, `processes`, `web_search`, `web_fetch`, `explore`, and `build`. These produce outputs that are either ephemeral (process lists, web pages) or available through re-execution.

Each tool has a specialized summarizer: grep reports match/file counts, write reports line counts with a diff preview, edit reports replacement counts, bash reports exit codes. These summaries become the `summary` field in the history entry, which is what the model sees after compaction replaces the full output. This two-layer approach -- shape results at creation time, then compact them later -- means the conversation history is already leaner than raw tool output before compaction ever fires.

## Manual Pinch (`/pinch`)

`/pinch` is the user-facing name for **manual in-place compaction**. Running it compacts the current session immediately—no popup wizard, no session fork. The TUI shows a system message while summarization runs; the orchestrator and server routes use the same `run_compaction_pipeline` helper with `CompactionTrigger::Manual`.

The summarizer (`summarizer.rs`) still produces structured fields (work summary, key decisions, pending tasks, important files). Those fields are woven into the compaction summary message that replaces dropped history, not into a new session's opening prompt.

`PinchContext`, linked-session storage helpers, and historical parent-session readers remain for compatibility. The unused `create_pinched_session` orchestration helper has been retired; all current entry points use in-place compaction. Existing sessions, database migrations, API routes, and client events are unchanged.

## Skills Injection

Skills are Agent Skills-compatible instruction packages built around `SKILL.md`. Mitsuro discovers its native roots plus `.agents`, Pi, OpenCode, Claude, Codex, and registered package roots. Project roots are discovered upward through the worktree, with nearest-project definitions taking precedence over user and package definitions. Strict validation enforces the standard name/description limits and directory-name match; structured diagnostics explain invalid and shadowed definitions.

The `SkillsManager` in `manager.rs` handles discovery, precedence, policy, diagnostics, package-root registration, and cache fingerprints. The `build_skills_context` function reads only enabled, model-invocable metadata and formats a bounded listing for the system context. Per-skill `allow`/`ask`/`deny` policy is persisted in `.mitsuro/skills-policy.json`; `ask` requires a supervised parent for model-driven loading, while `deny` is a hard block. Nearest-project policy wins among project files, but can only narrow user policy, never re-enable or loosen it.

When the model executes the deferred `skill` target through `tool_search`, the manager loads the full markdown content of the requested skill and injects it into the conversation. This lazy loading means skill content doesn't consume context budget until it's actually needed. The listing in the system prompt is lightweight and capped -- just names, descriptions, and bounded tags -- so the model knows what's available without paying the token cost of every skill's full instructions.

Skills are cached in memory, but content fingerprints make normal catalog reads notice filesystem and policy changes automatically. Path traversal, absolute paths, and symlink escapes are blocked when loading bundled resources. An `allowed-tools` frontmatter hint never grants permission: downstream tools continue through the canonical parent `ToolContext` governance contract.

## Shared Build Context

When Mitsuro runs parallel operations -- specifically the builder swarm, where multiple sub-agents work on different parts of a codebase simultaneously -- the `SharedBuildContext` in `build_context.rs` provides the coordination layer.

This is a thread-safe shared state object using `DashMap` (concurrent hash maps) and atomics. It tracks coding conventions (style rules all builders follow), file locks (per-file locking with retry/timeout so agents don't clobber each other's edits), modified files (which agent touched what), line diffs (running add/remove totals), an interface registry (builders publish exported types/functions that others can depend on), and contention metrics (lock wait times per file, flagging hotspots where total wait exceeds one second).

The `generate_context_injection` method produces a text block that gets injected into each builder's prompt, listing current conventions, files in progress (and who's working on them), and available interfaces from other builders. This gives each parallel agent awareness of the swarm's state without requiring direct communication between agents.

## How It All Connects

The context system operates as a pipeline with feedback loops. At the start of each turn, context injection builds the full message array. The orchestrator sends it to the model. When the model calls tools, the history policy shapes the tool results before they enter the conversation. As the conversation grows, the context ledger tracks its state. When tokens cross the trigger threshold—or the provider rejects an over-limit request—compaction fires in place and the ledger records what happened. `/pinch` runs the same pipeline on demand.

Each layer is designed to be invisible when things are working well. The user doesn't think about context budgets or retention policies. They just have a conversation, and the system quietly ensures the model always has the most relevant context available within its limits.
