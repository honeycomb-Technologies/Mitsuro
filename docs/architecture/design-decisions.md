# Why We Built It This Way

This document covers the major architectural decisions behind Mitsuro — what we chose, what alternatives existed, and why we went the direction we did. These aren't abstract design principles; they're concrete choices that shaped the codebase and have real trade-offs.

## 1. Why Rust?

**The choice:** Build the entire backend — CLI, core library, server, and ACP agent — in Rust.

**Alternatives considered:** Go (fast compilation, good concurrency), TypeScript/Node (same language as frontend, large ecosystem), Python (fast prototyping, AI ecosystem).

**Why Rust won:** Mitsuro is a single binary that users install and run on their machines. Binary size, startup time, memory usage, and reliability all matter for a tool that sits in your terminal all day. Rust gives us:

- **Single static binary** — no runtime dependencies, no "install Python 3.11 first"
- **Memory safety without a garbage collector** — the TUI needs consistent frame timing; GC pauses would cause visible stuttering during streaming
- **Fearless concurrency** — the orchestrator juggles streaming AI responses, tool execution, sub-agents, background processes, and UI rendering simultaneously. Rust's ownership model catches race conditions at compile time
- **Performance where it matters** — grep/glob tools search codebases with hundreds of thousands of files; the markdown renderer processes streaming output at 60fps

**The trade-off:** Slower development iteration (compile times), steeper learning curve, smaller ecosystem for AI/ML tooling. We accept this because the product quality benefits outweigh the development speed cost for a tool that's meant to feel polished and professional.

## 2. Why SQLite?

**The choice:** Use SQLite as the sole persistence layer, accessed via rusqlite.

**Alternatives considered:** PostgreSQL (full-featured relational), sled/RocksDB (embedded key-value), plain JSON files, no persistence (in-memory only).

**Why SQLite won:** Mitsuro is local-first software. Users shouldn't need to install, configure, or manage a database server. SQLite gives us:

- **Zero configuration** — the database is a single file at `~/.mitsuro/mitsuro.db`
- **Ships with the binary** — no external dependency
- **Full SQL** — complex queries for session management, message pagination, credential lookups
- **WAL mode** — concurrent reads during writes, which matters when the TUI is reading session data while the orchestrator is writing tool results
- **Atomic transactions** — critical for session recovery when the process crashes mid-tool-execution

**The trade-off:** No multi-process write concurrency (only one writer at a time), no built-in replication, 2GB practical row size limit. These don't matter for a single-user local tool. If Mitsuro ever needs multi-user server deployment, the storage layer's manager pattern makes it possible to swap backends without rewriting business logic.

## 3. Why One Orchestrator Loop?

**The choice:** A single `AgenticOrchestrator` in mitsuro-core that all interfaces (TUI, server, ACP, Hive) use.

**Alternatives considered:** Separate loops per interface (each with their own streaming/tool logic), a shared library with per-interface wrappers.

**Why one loop won:** Early in development, the TUI and server had separate agent loops. They inevitably diverged — the TUI got a feature (like plan mode), the server didn't, bugs got fixed in one but not the other. Consolidating into one orchestrator was a painful refactor, but it means:

- **Feature parity by default** — any capability added to the orchestrator is immediately available in all four interfaces
- **One place to fix bugs** — a streaming edge case fix applies everywhere
- **Thin presentation layers** — the TUI, server, and ACP are just event mappers. They translate `LoopEvent`s into their display format and `LoopInput`s into the orchestrator's input format

**The trade-off:** The orchestrator is the most complex piece of code in the project. It handles streaming, tool execution, context injection, plan management, failure detection, session recovery, and title generation. This complexity is concentrated rather than distributed, which makes it harder to understand but easier to maintain.

## 4. Why Multi-Provider from Day One?

**The choice:** Build an AI provider abstraction layer that supports multiple LLM providers through one interface.

**Alternatives considered:** Anthropic-only (simplest), OpenAI-compatible API only (most common denominator).

**Why multi-provider won:** Provider lock-in is a real risk in AI tooling. Models improve at different rates, pricing changes, and different tasks suit different models. The format abstraction layer (with Anthropic, OpenAI, and Google format handlers) means:

- **Users choose their provider** — some prefer Claude, others GPT, others want cheap models via OpenRouter
- **Model switching mid-conversation** — start with a fast model for exploration, switch to a powerful one for complex refactoring
- **Future-proof** — new providers can be added by implementing the format handler trait without touching the orchestrator

**The trade-off:** Significant complexity. Each provider has different message formats, tool calling conventions, streaming protocols, and capability profiles. The format abstraction layer (~2000 lines across four format handlers) exists solely to paper over these differences. Testing is harder because you need accounts with multiple providers.

## 5. Why Expo for Mobile and Web?

**The choice:** Use Expo (React Native) for the mobile app, and reuse its web export as the server's embedded frontend.

**Alternatives considered:** Native iOS/Android (Swift/Kotlin), Flutter, separate web frontend (Next.js/SvelteKit), keep TUI-only.

**Why Expo won:** The core insight is that the web frontend and mobile app are the same product — a chat interface that talks to the Mitsuro server API. Building them separately would mean maintaining two codebases for the same UI. Expo lets us:

- **One codebase, three targets** — iOS, Android, and web from the same TypeScript/React code
- **Platform abstraction** — the `.native.ts` / `.web.ts` pattern lets us use platform-specific APIs (haptics, secure storage, push notifications) while sharing 90% of the code
- **Embed in the binary** — `expo export --platform web` produces static files that get compiled into the Rust binary via rust-embed. No separate frontend deployment

**The trade-off:** React Native has overhead compared to native. The web build uses Metro instead of a more optimized web bundler. Some platform features (iOS widgets, Live Activities) still need native code. But the development velocity gain from one codebase far outweighs these costs.

## 6. Why Native Canvas for Desktop?

**The choice:** Build the desktop product in `apps/desktop/canvas`, using GPUI and
GPUI Component for native spatial interaction. Expo owns mobile and web; the
CLI/TUI owns terminal interaction.

**Shared authority:** Canvas uses `mitsuro-client` and the Mitsuro server. Agent and
Hive execution, tool permissions, conversations, resource leases, and recovery
remain in shared core/server contracts. Native presentation belongs in Canvas.

**Retired alternatives:** The earlier dual-backend GPUI application and Tauri/Expo
desktop shell are abandoned. Their source is historical; new features must not
split back into those applications. This decision does not remove the GPUI
framework or the active Expo mobile/web app.

**The trade-off:** Native Canvas requires its own interaction and platform QA.
The current packaging path is a Linux alpha archive; primary product status does
not establish complete GUI acceptance or macOS/Windows desktop packaging.
See the [desktop transition contract](../frontends/desktop-transition.md).

## 7. Why WASM for Extensions?

**The choice:** Use WebAssembly (via Wasmtime) for isolated editor/language
extensions, with Zed-compatible WIT interfaces, and a visibly separate trusted
Bun worker for coding-agent extensions.

**Alternatives considered:** Dynamic libraries (.so/.dylib), Lua scripting, JavaScript/V8, no extension system.

**Why WASM won for the untrusted boundary:** Third-party editor extensions need
hard isolation. Safety is non-negotiable:

- **Sandboxed** — WASM extensions can't access the filesystem, network, or system APIs unless the host explicitly grants it
- **Portable** — one .wasm binary runs on every platform Mitsuro supports
- **Zed compatibility** — by adopting Zed's WIT interface definitions, extensions written for Zed can potentially run in Mitsuro. This gives us access to an existing and growing extension ecosystem
- **Deterministic resource limits** — epoch-based interruption prevents runaway extensions

**The trade-off:** WASM extensions are harder to write than scripts. The WIT
interface is powerful but verbose and the compile/load loop is slower. Agent
extensions therefore use persistent JavaScript/TypeScript workers for the same
low-friction tool, command, event, and context workflows offered by Pi and
OpenCode. Those workers are explicitly trusted local code: package permissions,
environment filtering, timeouts, and process isolation make authority visible,
but are not described as a sandbox. Skills and MCP remain the non-code and
remote-capability alternatives.

## 8. Why the Event-Driven Architecture?

**The choice:** The orchestrator communicates with presentation layers via `LoopEvent` (output) and `LoopInput` (input) channels.

**Alternatives considered:** Direct function calls with callbacks, trait-based presentation abstraction, shared mutable state.

**Why events won:** The orchestrator runs as a spawned tokio task. It can't block waiting for user input (the AI might be streaming), and the TUI can't block waiting for orchestrator output (it needs to render frames). Events solve this naturally:

- **Non-blocking** — both sides work at their own pace
- **Serializable** — `LoopEvent`s can be sent over SSE to web clients, over JSON-RPC to editors, or rendered directly in the TUI
- **Observable** — Hive's tick engine, runtime traces, and the attach command all work by consuming the same event stream
- **Testable** — you can test the orchestrator by feeding it `LoopInput`s and asserting on `LoopEvent`s without needing a real TUI or HTTP server

**The trade-off:** The event protocol has grown to 20+ event types. New features often need new event variants, and every presentation layer needs to handle (or ignore) them. The asymmetry between events (many types) and inputs (three types: approval, AskUser response, cancellation) reflects the reality that the orchestrator has much more to say than the user does during a turn.

## 9. Why MCP Instead of a Custom Tool Protocol?

**The choice:** Adopt the Model Context Protocol for external tool discovery rather than inventing a proprietary tool integration format.

**Alternatives considered:** Custom REST API for tool registration, plugin-based tool loading, hardcoded integrations only.

**Why MCP won:** MCP is becoming the standard for AI tool interoperability. Tools that implement MCP work with Claude Desktop, Cursor, and other AI assistants — not just Mitsuro:

- **Ecosystem leverage** — any MCP server works with Mitsuro out of the box
- **Standardized discovery** — tools declare their schemas, and Mitsuro registers them automatically
- **Two transports** — stdio for local tools, HTTP/SSE for remote ones

**The trade-off:** MCP adds a process management layer (the McpManager has to start, monitor, and restart MCP server processes). Tool schemas need sanitization to match the AI provider's expectations. And MCP tools bypass some of Mitsuro's built-in safety checks since they run in their own process.

## 10. Why Local-First?

**The choice:** Everything runs on the user's machine. No cloud service, no account required, no telemetry.

**Alternatives considered:** SaaS model (hosted backend), hybrid (local client + cloud API), cloud-only.

**Why local-first won:** AI coding assistants see everything — your code, your credentials in env files, your git history, your conversation about that security vulnerability you're fixing. Sending that to a third-party cloud service requires enormous trust:

- **Privacy by default** — your data never leaves your machine (except the LLM API calls you explicitly configure)
- **No accounts** — install the binary, add an API key, start working
- **Works offline** — the TUI, storage, and tool system work without internet. Only AI calls need connectivity
- **Self-hosted remote access** — if you want to access Mitsuro remotely, you run it on your own machine and connect via Tailscale. You control the infrastructure

**The trade-off:** No collaboration features, no shared sessions, no cloud-hosted Hive that runs while your laptop is closed. Users manage their own infrastructure. These trade-offs are acceptable because the primary use case is individual developers working on their own codebases.

## 11. Why a Single Binary?

**The choice:** Ship Mitsuro as one executable that contains the CLI, TUI, web server, embedded web frontend, and SQLite database engine.

**Alternatives considered:** Separate binaries (CLI + server + frontend), containerized deployment, platform-specific installers.

**Why single binary won:** Installation friction kills adoption. Every dependency, configuration step, or separate service is a reason someone doesn't try your tool:

- **`curl | sh` install** — download one file, put it in PATH, done
- **No Docker** — no container runtime needed, no compose files, no volume mounts
- **No separate frontend** — the web UI is embedded via rust-embed. `mitsuro serve` is literally one command
- **Portable** — copy the binary to another machine and it works

**The trade-off:** Larger binary size (~50MB with the embedded web frontend). Longer compilation times. The web frontend must be built before the Rust binary can compile (it gets embedded at compile time). Updates require replacing the entire binary rather than hot-swapping a frontend bundle.

## 12. Why the Permission Model?

**The choice:** Two modes — Supervised (approval required for write operations) and Autonomous (everything auto-approved) — with a safety hook layer underneath.

**Alternatives considered:** Always require approval (too slow), never require approval (too dangerous), fine-grained per-tool permissions, capability-based security.

**Why this model won:** The right level of autonomy depends on what you're doing. Exploring a codebase? The AI should read freely. Editing files? You probably want to review each change. Running a Hive task overnight? It needs to operate without you:

- **Supervised for interactive work** — you see every write operation before it happens
- **Autonomous for trusted workflows** — Hive and experienced users can skip the approval dialog
- **Safety hooks as a backstop** — even in autonomous mode, the SafetyHook blocks obviously dangerous commands (rm -rf, sudo, fork bombs). This prevents the worst outcomes without requiring human judgment for every tool call
- **Per-project overrides** — a project's `.mitsuro/settings.json` can set its own permission mode

**The trade-off:** Two modes is a coarse granularity. Some users want "approve bash but auto-approve file reads" or "approve writes to src/ but auto-approve writes to tests/". The current model doesn't support this. The tool policy system has the infrastructure for finer-grained control, but exposing it to users without making the UX confusing is an unsolved design problem.

---

These twelve decisions define what Mitsuro is — and equally, what it isn't. It's a local-first, single-binary, multi-provider AI coding assistant that prioritizes developer autonomy, presentation-layer flexibility, and safety through its permission model. Every choice involved trade-offs, and understanding those trade-offs is key to understanding why the codebase is shaped the way it is.
