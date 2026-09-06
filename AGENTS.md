# AGENTS Guide: Mitsuro

## Purpose
Repository-level engineering, product, validation, and release guardrails for Mitsuro: an AI coding assistant with mobile, web, desktop, CLI/TUI, ACP editor integration, and autonomous agent modes.

## AGENTS Strategy
This is the **only** AGENTS file in the repository. All module-specific invariants live in sections below rather than scattered across subdirectories.

## Product Language and Compatibility
- **Product name:** Mitsuro.
- **Company name:** Honeycomb Technologies.
- **Interactive assistant/session:** Agent. User-facing modes may be named Chat and Code.
- **Durable autonomous system:** Hive. An individual delegated worker is a Hive Worker.
- **Hive surfaces:** Workers, Groups, Activity, Calendar, and Memory.
- **Activity accent/state:** Pulse, only where the product design calls for that term.
- Public product copy, screenshots, package descriptions, release notes, and repository prose should use Mitsuro, Agent, Hive, Hive Worker, Hive Workers, Groups, Activity, Calendar, Memory, and Pulse consistently.
- The canonical public repository is `honeycomb-Technologies/Mitsuro`. Mobile launch URLs use `mitsuro://`.
- Canonical identifiers include `mitsuro`, `mitsuro-*`, `mitsuro-hive`, `@mitsuro/*`, `/api/hive/*`, `session_type = "hive"`, `~/.mitsuro`, and the corresponding Expo, native, database, and deployment names.
- Prior identifiers may appear only in dedicated, tested compatibility readers and migrations. Those boundaries read prior state and write canonical state; they are not current product language.
- Any compatibility retirement requires an explicit plan covering stored data, installed clients, deep links, deployments, rollback, and mixed-version behavior.
- The supported terminal command is `mitsuro`. The deprecated `krusty` command remains a tested compatibility alias for the announced transition window; removing it requires the migration cutover criteria to pass.
- If product language and an internal identifier differ, prefer clear translation at the UI/API boundary over leaking the internal name into the interface.

## Core Architecture
- `crates/mitsuro-cli`: Terminal client and TUI runtime. Entry point with command parsing.
  - `src/main.rs`: CLI entry point, parses commands, starts ACP server or TUI with logging/setup.
  - `src/tui/`: Terminal UI module with blocks, handlers, state, themes, plugins.
- `crates/mitsuro-core`: Shared runtime library.
  - `src/ai/`: AI provider layer with multi-provider clients, streaming support.
  - `src/agent/`: Agent system with event handling, hooks, sub-agents, in-place compaction.
  - `src/acp/`: Agent Client Protocol server for editor integration.
  - `src/mcp/`: Model Context Protocol client manager.
  - `src/tools/`: Tool registry and built-in tool implementations (read, write, edit, bash, grep, glob, etc.).
  - `src/storage/`: SQLite persistence for sessions, plans, preferences, credentials.
  - `src/plan/`: Database-backed planning system.
  - `src/skills/`: Filesystem-based skills system.
  - `src/extensions/`: Zed-compatible WASM extension system.
  - `src/process/`: Background process registry/management.
  - `src/auth/`: OAuth/auth flows and token storage helpers.
  - `src/updater/`: Auto-updater for dev/release modes.
- `crates/mitsuro-server`: Self-host API plus embedded web bundle for external clients.
- `apps/mobile`: Expo app that serves as the primary mobile client and React-based web surface.
- `apps/desktop/canvas`: Primary desktop GUI, using shared Mitsuro contracts.
- Desktop targets `apps/desktop/{gpui,shell,ui}` and `crates/mitsuro-desktop-backend`
  are abandoned historical references: do not develop, build, package, or release them.
- Interfaces: Canvas desktop, Expo mobile/web, CLI/TUI terminal; ACP for editors.

## Design Patterns
- **Event Bus**: AgentEventBus as central dispatcher.
- **Registry**: ToolRegistry, ThemeRegistry for centralized management.
- **Plugin Architecture**: Trait-based plugins (e.g., StreamBlock trait for renderable blocks).
- **Strategy/Polymorphism via traits**: Different providers, tool implementations.
- **Manager Pattern**: McpManager, PlanManager, SkillsManager, SessionManager.

## Cross-Cutting Standards
- Prefer clear module boundaries over cross-layer coupling.
- Write code that is composable, testable, and explicit about failure modes.
- Keep changes small and reversible.
- Avoid hidden side effects and global state sprawl.
- Preserve the existing UI appearance and interaction model during reliability or performance work unless a redesign is explicitly requested.
- Prefer measured proof over intuition: establish a baseline, name the suspected bottleneck, make a focused change, and compare the same workload afterward.
- Inspect current branches, worktrees, dirty files, processes, disk capacity, and runtime authority before consequential edits, cleanup, integration, deployment, or release work.
- Preserve unrelated dirty work and active processes. Never treat a dirty worktree as disposable or assume its branch tip contains its uncommitted work.
- Keep source state, committed state, built artifacts, installed clients, and running services distinct in both reasoning and status reports.
- Do not claim that a code change is deployed, an upload is processed, a TestFlight build is available, or a runtime is healthy without evidence for that exact state.
- Prefer focused fixes over broad rewrites. When a broad change is genuinely required, keep it staged, reviewable, and reversible.
- Error handling: `anyhow` + `thiserror` + custom error enums.
- Logging: `tracing` + `tracing_subscriber`.
- Async: `tokio` (no `async-std`).

## Task Execution Contract
- For reviews, audits, explanations, and diagnosis, inspect first and report evidence; do not infer authorization for unrelated mutations, deployments, messages, or releases.
- For an approved implementation, complete the scoped change, validate it in proportion to risk, review the resulting diff, and carry it to a clear terminal state instead of stopping at a plan.
- Make reasonable, reversible assumptions when they preserve intent. Ask before a choice that materially changes product behavior, public state, data, cost, security, or release scope.
- Keep progress updates concise and factual during long-running work. State what is proven, what is inferred, what is still running, and what remains.
- When blocked, exhaust safe in-scope diagnostics and alternatives before asking the user. Do not label slow, difficult, or merely uncertain work as blocked.
- Do not hide failed commands, flaky tests, warnings, or incomplete release states. Explain their scope and whether they invalidate the requested outcome.
- Never delete branches, worktrees, caches, build artifacts, databases, credentials, or generated native projects without first proving ownership, current use, and recovery impact. Destructive cleanup requires explicit authorization.
- End handoffs with the authoritative path/branch/commit, validations run, runtime or release state, preserved dirty work, and the exact remaining action if anything is unfinished.

## Quality Improve Loop
- The project skill is `.cursor/skills/quality-improve/` (Hive also loads `.mitsuro/skills/quality-improve`).
- Default is propose-only. Apply only when the user says apply and readiness is green.
- Write the deepest simple thing: small interface, parse once at the edge, effects in a thin shell, no unpaid seams.
- Do not use this loop to delete dead code, shrink contracts, change UI, or touch recovery/schema state.
- Fleet: `quality-scout`, `quality-modularize`, `quality-perf`, `quality-simplifier`, `quality-critic`. One sector, one slice, critic, validate, ledger.

## Evidence and Runtime Boundaries
- Treat static checks, unit/integration tests, a Debug simulator run, a Release simulator run, a locally installed device build, TestFlight processing, TestFlight installation, and production runtime behavior as separate evidence levels.
- Simulator testing is the default fast loop for navigation, rendering, stress gestures, stream behavior, and most performance regressions. Use Release configuration for performance conclusions.
- Physical-device testing is required for device-only behavior such as APNs, Live Activities, widgets, background execution, thermal pressure, real cellular transitions, and TestFlight-specific packaging.
- A successful App Store upload does not prove Apple processing completed. Processing does not prove tester assignment. Tester assignment does not prove the build installed or ran on a phone.
- A healthy server endpoint proves only that service layer. For chat failures, diagnose these layers independently:
  1. Honey service/process and route availability.
  2. Provider credentials and refresh state.
  3. Selected model and request outcome.
  4. Client attachment, SSE delivery, parsing, and recovery.
- Honey is the runtime authority when the client is connected to Honey. A local checkout, a dirty Honey source tree, or a successful local test is not evidence of the running Honey binary.
- For Honey runtime claims, verify the user service, `/health`, the running executable via `/proc/<pid>/exe`, release symlink or artifact identity, version/hash, and the relevant provider request when safe.
- Never silently substitute a different provider or model to make a failed request appear successful. Make fallbacks explicit in product behavior and diagnostics.

## Crate Boundaries
- `mitsuro-cli`: terminal UX only. Do not re-implement core runtime logic here.
- `mitsuro-core`: shared runtime. All shared business logic lives here.
- `mitsuro-server`: HTTP API. Keep route handlers thin; push shared logic into core.
- Move shared logic to `mitsuro-core`; avoid duplication across CLI/server.

## Module-Specific Invariants

### AI Provider Layer (`crates/mitsuro-core/src/ai/`)
- Keep provider-specific quirks isolated from shared response models.
- Keep model-family prompt behavior in shared profiles; streaming and simple/conversation calls must build the same instruction layers.
- Keep provider request/stream normalization in the shared AI transform layer; avoid scattering provider patches across individual transport call-sites.
- Streaming behavior must be robust to partial/malformed provider events.
- Parser changes must preserve existing tool/thinking/message semantics.
- Keep curated direct-provider model catalogs aligned with product-supported IDs; when a provider adds fast or effort variants, update static fallbacks and dynamic filtering together.

### Tool System (`crates/mitsuro-core/src/tools/`)
- Tool argument parsing and error surfaces are user-facing contracts.
- Keep permission/approval semantics explicit and conservative.
- Avoid hidden filesystem/network side effects in tool implementations.
- Prefer structured tool result envelopes (`ok`, `data`, `error`, optional `warnings`/`metadata`) over ad-hoc plain strings.
- Delegated tool surfaces (subagents, remote MCP wrappers) must carry inherited governance metadata and enforce parent permission constraints.
- Keep filesystem path policy owned by `ToolContext`/registry logic; do not reintroduce duplicate standalone path-validation helpers.

### Storage (`crates/mitsuro-core/src/storage/`)
- Migration safety first: schema changes must be forward-only and tested.
- Keep read/write behavior explicit and transaction-aware.
- Keep interrupted-turn recovery state separate from canonical conversation history.
- Keep runtime traces compact and structured; persist summarized diagnostic payloads rather than raw stream dumps unless exact replay fidelity is required by design.
- Linked-session persistence must preserve parent ownership metadata so pinch/continuation flows do not escape multi-tenant boundaries.
- For push reliability changes, keep `database.rs`, `push_subscriptions.rs`, and `push_delivery_attempts.rs` aligned.
- Never log sensitive credentials.

### Agent Core (`crates/mitsuro-core/src/`)
- Keep subsystem contracts explicit between AI, tools, storage, plugins, and protocols.
- Prefer typed boundaries over ad-hoc JSON passing.
- Keep live in-place compaction as the default overflow path; `/pinch` and the pinch API route trigger manual compaction in the same session, not a session fork.
- Flush durable notes to the memory store before compaction summarization; persist compaction checkpoints/segments for `search_compaction_segments` recovery.
- On provider context-overflow/413 errors, compact once with `CompactionTrigger::Overflow` and retry the turn before surfacing a terminal provider error.
- Keep loop budgets and streaming timeouts explicit and shared across callers; do not hide behavioral caps inside transport layers.
- Keep UI-facing tool output separate from model-facing history retention; long raw tool output should not be preserved in conversation history unless the exact payload is still needed for the next turn.
- Keep agent tool approval/retry/result policy centralized in the agent control layer rather than embedding it ad hoc in transport or tool implementation code.
- Keep delegated execution (subagents, MCP, extensions, skills) on explicit inherited governance contracts for permission mode and turn budget; delegated paths must not silently bypass parent policy.
- Resolve delegated turn budgets and permission mode from the shared contract at execution time; do not duplicate drift-prone defaults across subagent or route surfaces.
- Keep plan lifecycle state canonical in core helpers; active-vs-archived plan resolution and effective work mode must not be re-derived independently in UI or server layers.
- Capture runtime observability from the canonical `LoopEvent` boundary; do not add drift-prone provider/tool/UI-specific trace streams for the same execution path.

### Server Routes (`crates/mitsuro-server/src/routes/`)
- Keep request/response shapes synchronized with CLI, web, and mobile clients.
- Validate and sanitize all user inputs before side effects.
- Preserve streaming route stability and backpressure behavior.
- Chat routes must honor persisted session model unless an explicit per-request override is provided.
- Session routes must keep `working_dir` as runtime source-of-truth and treat `target_branch` as optional session intent metadata.
- Session creation, read, pinch, and approval routes must preserve multi-tenant ownership end-to-end.
- Tool approval routes must stay contract-aligned with mobile/web notification surfaces by accepting explicit session-targeted approvals and surfacing delivery failures.
- Session presence routes must stay ownership-checked, server-authored, and stale-aware.
- Tool execution routes must pass the same governance context as orchestrated runs (permission mode, delegated turn budget, and extensibility managers).
- Direct tool execution must keep `working_dir` scoped to the same allowed workspace root as the rest of the server file/path surfaces.
- Knowledge routes must keep reports and memories on shared typed contracts with project/user scoping preserved end-to-end; avoid shadow stores or client-only promotion state.
- Chat streaming must keep a bounded queue with explicit lag signaling; never let a slow SSE client silently stall or redefine core loop semantics.
- Auth and credential routes for dynamic-model providers should refresh shared model catalogs eagerly.
- Push endpoints (`/push/*`) must stay aligned with mobile/web diagnostics and test-send flows.
- Port proxy endpoints (`/ports/*`) must remain localhost-scoped and deny recursive self-proxy loops.

### TUI (`crates/mitsuro-cli/src/tui/`)
- Protect frame-time performance and input responsiveness. Avoid heavy allocations in render/event hot paths.
- Keep streaming updates idempotent and visually stable.
- Keep stream backpressure policy, queue telemetry, and interruption recovery messaging explicit in shared TUI state/handlers.
- Handle partial stream events safely; never panic on malformed chunks.
- Drain bursty stream output incrementally; do not let a single stream monopolize a frame and starve input/render.
- Plan/task UI state must come from persisted plan lifecycle or explicit loop events, not heuristic parsing of assistant prose.
- Keep contrast/readability strong in both dense and sparse views. Theme additions must update registry wiring and defaults intentionally. Avoid hardcoding colors outside theme primitives.

### TUI Handlers (`crates/mitsuro-cli/src/tui/handlers/`)
- Keep keyboard/mouse/render handling deterministic.
- Keep session/tool side effects explicit and traceable.
- Keep model selection and quick-toggle flows on a shared handler path so persistence, auth rebinds, and recent-model state do not drift.

### Extensions & WIT (`crates/mitsuro-core/src/extensions/`, `wit/`)
- Treat WIT and extension host changes as ABI-sensitive.
- Keep manifest parsing strict and error messages actionable.
- Preserve compatibility rules across extension API versions.
- Version contract changes intentionally; avoid silent breaking renames.
- Keep generated/runtime expectations synchronized across crates.

### Plugins (`crates/mitsuro-core/src/plugins/`)
- Treat plugin install/update flows as security-sensitive.
- Verify trust and signature requirements before writing plugin artifacts.
- Keep lockfile and on-disk state transitions atomic and recoverable.
- Error messages must clearly distinguish trust failures from IO failures.

### Apps (`apps/`)
- Preserve strict separation between app surfaces and core runtime internals.
- Do not duplicate business logic that already exists in `mitsuro-core` or `mitsuro-server`.
- Keep Canvas and Expo behavior aligned through shared server contracts where features overlap.
- Keep model-speed and reasoning controls driven by shared client state or server contracts rather than ad-hoc component-local mappings.
- Notification and Live Activity actions that mutate session state must carry explicit session context; never assume the currently focused chat is the correct target.
- Keep knowledge surfaces server-backed and shared across modes; reports and memories should behave like one project knowledge substrate rather than separate client-local feature stacks.
- Canvas uses `mitsuro-client` and shared core/server logic. Keep Expo independent of abandoned desktop packages.
- Preserve installed data, compatibility readers, and service artifacts during retirement.
- Canvas still requires GUI acceptance and deployment validation; see `docs/frontends/desktop-transition.md`.

### Mobile and Shared Client Performance (`apps/mobile`, `packages/state`, `packages/ui`)
- Keep the interaction path independent from background synchronization. Navigation, opening the composer, switching modes, and creating an optimistic chat shell must not await full catalogs, session lists, or transcript hydration.
- The active conversation surface owns its transcript subscription and lifecycle. App shells, inactive modes, hidden drawers, and secondary surfaces must not subscribe to or render the full active transcript.
- Isolate immutable historical turns from the live turn. Use stable keys, structural equality, memoized boundaries, and bounded updates so one token does not re-render the entire transcript or application shell.
- Coalesce duplicate loads and reconnects with single-flight behavior. Effects must be idempotent, cancelable where possible, narrowly dependent, and responsible for cleaning up listeners, timers, network requests, and subscriptions.
- Never start network work, subscriptions, persistence, or native side effects during render.
- Keep expensive secondary surfaces lazy and bounded. Browser and terminal processes may be kept alive after first use when that improves interaction, but hidden surfaces must freeze or stop costly work and tab/process counts must have explicit caps.
- Avoid mounting multiple chat transcripts or mutually exclusive mode trees at once. Hidden does not mean free.
- Treat nested virtualized lists, clipping, and React Native New Architecture interactions as correctness-sensitive. Validate list changes in a Release build under rapid navigation before enabling aggressive clipping or recycling behavior.
- Batch or throttle high-frequency streaming, stick-to-bottom, gesture, presence, widget, and Live Activity updates. Do not enqueue application-wide state updates for every token, pixel, or animation frame.
- Keep state selectors narrow. Context providers and external stores must not make unrelated screens re-render for chat tokens, timers, connection pings, or diagnostics events.
- Persist interrupted-turn and draft recovery independently from canonical transcript history. A force quit may lose only the bounded in-flight tail, never corrupt or recursively rewrite the durable draft/session cache.
- Changes to navigation, chat, mode switching, drawers, toolbox, settings, or session state require a repeatable stress pass that rapidly stacks those interactions and checks responsiveness, memory growth, warnings, and recovery.
- Performance changes must retain the same visual output unless the task explicitly authorizes a design change.

### Mobile Diagnostics and Privacy
- Capture client performance and lifecycle telemetry at a shared, structured boundary. Correlate JS and native events with install, app-run, session, trace, build, and platform identifiers without duplicating the server's canonical `LoopEvent` execution trace.
- Keep production/TestFlight diagnostics bounded, allowlisted, and privacy-safe. Never upload credentials, authorization headers, raw prompts, message bodies, tool output, filesystem contents, or unrestricted stack dumps.
- Prefer durations, counters, state transitions, dropped-frame/hang signals, memory summaries, lifecycle markers, request classifications, and symbolicated allowlisted frames.
- Persist bounded diagnostic batches incrementally. A freeze or force quit may prevent a final flush, so useful pre-freeze evidence must not depend on graceful termination.
- Uploads must be explicit in product behavior, retry-safe, size-limited, ownership-checked, and observable to the user. Server ingestion must be idempotent and apply retention limits.
- Debug-only tracing may be richer, but it must be compile-time or runtime gated and must not silently ship content-bearing traces in production.
- Instrumentation must not become the performance problem. Measure its overhead, sample high-frequency events, cap buffers, and drop diagnostics before blocking the UI or chat stream.
- Keep diagnostic schemas versioned and backward-compatible across mixed TestFlight/client and Honey server versions.

### CI/CD (`.github/`)
- Treat workflow changes as production-impacting.
- Keep CI reproducible and aligned with local developer commands.
- Avoid secret leakage in logs and artifact names.
- Keep release workflows backward-compatible for existing tags and packaging paths.
- Do not spend remote build minutes to discover failures that deterministic local checks or a Release simulator build can catch.

### Packaging (`aur/`)
- Keep PKGBUILD/install scripts aligned with released artifacts.
- Avoid distro-specific assumptions in core runtime logic.
- Any packaging change should preserve clean install/upgrade paths.

### Git Hooks (`.githooks/`)
- Keep hooks deterministic and quick.
- Prefer local checks; avoid network-dependent commands.
- Fail with clear, actionable error messages.
- Any new hook must not duplicate CI logic unnecessarily.

## Public Repository and Documentation Hygiene
- Keep the public README product-first, concise, accurate, and useful to a new user. Put deep implementation detail in maintained contributor or architecture documentation.
- Public documentation must clearly distinguish implemented behavior, compatibility behavior, experiments, proposals, and future plans.
- Do not commit internal research dumps, competitive notes, temporary handoffs, generated traces, profiling bundles, local reports, screenshots containing private data, or abandoned experiments to public-facing documentation paths.
- Add generated or local-only artifacts to `.gitignore` before they accumulate. If sensitive or misleading content was already committed, removing the current file does not remove Git history; history rewriting requires explicit, coordinated authorization.
- Never publish credentials, tokens, private host details, personal filesystem paths, customer/user data, or unredacted diagnostics.
- Avoid stale hard-coded claims about supported providers, models, versions, release availability, or infrastructure when the product can expose the current truth dynamically.
- Keep documentation links and commands executable from the repository state that contains them. Label platform-specific or compatibility-only commands accurately.
- Historical product names may appear only where needed to explain a current compatibility contract or migration.

## Build, Cache, and Disk Hygiene
- Check `df` before large Rust, native iOS, Android, or multi-architecture builds when disk pressure is plausible. Inspect the size and age of `target`, DerivedData, simulator data, downloaded runtimes, CocoaPods caches, and `node_modules` before cleanup.
- Reuse valid build artifacts and dependency caches. Do not repeatedly rebuild unchanged native layers for TypeScript-, JavaScript-, or documentation-only changes.
- Do not use `cargo clean`, delete DerivedData, remove `node_modules`, remove Pods, or wipe simulator data as a first response to a build problem. Identify the stale or corrupt layer and remove only the bounded artifact that needs regeneration.
- Prove reclaimed disk space with `df`; `du` only identifies where space is used.
- CocoaPods remains part of the Expo iOS native dependency graph. Adding Skia or another native package does not replace Pods or justify removing the Podfile/native project integration.
- Respect the checked-in lockfiles. When dependencies are stale or missing, prefer the repository's frozen install workflow before changing versions.
- Check for active `cargo`, Xcode, Metro, Expo, simulator, and package-manager processes before deleting their outputs.
- Record native dependency changes, generated-project regeneration, and cache invalidation reasons in the handoff or commit context so the next agent does not repeat them blindly.

## Default Dev Workflow
- Build and run current local code only; do not require `git pull` for day-to-day refinement.
- Rust builds inherit `TMPDIR` from `.cargo/config.toml`, pointing rustc temp files at the workspace `target/` directory instead of `/tmp`.
- **Rust backend**: `cargo run -p mitsuro` from repo root.
- **Expo web dev server**: `cd apps/mobile && npx expo start --web --port 5173`.
- Do active UI/web iteration at `http://localhost:5173` while the backend runs separately.
- Frontend edits hot-reload automatically; Rust backend edits require a restart.
- **ACP mode** (editor integration): `mitsuro acp`

## Release Integration Workflow
- The operator path is [docs/operations/release.md](docs/operations/release.md): promote staging → `main`, Version (Sampo) tags, Release binaries uploads GitHub Release assets, then Honey `sh scripts/honey-upgrade.sh v{version}`. A tag without the linux archive is not a release.
- Sampo only versions, changelogs, and tags from `.sampo/changesets/`. It does not publish Honey-installable archives. After a version bump, restore third-party `Cargo.lock` pins (`sh scripts/refresh-workspace-lock-versions.sh origin/main`) and sync Expo (`sh scripts/sync-product-version.sh`).
- `GITHUB_TOKEN` tag pushes do not start other workflows. Version (Sampo) and CI on `main` dispatch **Release binaries** with the `v{version}` tag. Re-run that workflow to retry a failed cut.
- Keep `main` as the last accepted release baseline. Cross-surface work intended for the next coordinated release belongs on the current dated `codex/release-staging-YYYYMMDD` branch.
- At the start of any Mitsuro task that may change or release the product, inspect the active branch, every registered worktree, staged and unstaged changes, active build/deploy processes, and the current release-staging branch before editing.
- Treat each worktree as an independent Git index. Commit a logical change on its source branch, then deliberately cherry-pick or otherwise reconcile it into release staging; files cannot be staged across worktrees through one shared index.
- Do not merge preservation, archive, or old rollup branches wholesale. Audit their commits against release staging and integrate only changes that are genuinely missing and still intended.
- Preserve concurrent dirty work. When a mixed dirty tree must be captured, place it on release staging as an explicit staging snapshot before reorganizing or squashing it for release.
- Build private Honey previews from the exact release-staging commit. A detached preview mirror is not a source branch and must not become the release authority.
- Do not push, merge to `main`, tag, publish artifacts, restart the production service, or issue a public release until release staging passes the required validation below and the user explicitly approves the release.
- One coordinated release may contain multiple logical commits. Prefer an auditable staging history over one unreviewable cross-worktree commit; squash only at the final release boundary when requested.
- Before a mobile release, reconcile every intended feature branch onto the current release-staging tip, then re-run mobile/native validation on that combined commit. A previously successful build from an older tip is not release evidence.
- Prefer local static checks, focused regressions, web export, and Release simulator/device validation before consuming EAS build credits.
- For TestFlight, record the source commit, marketing version, build number, build method, upload result, Apple processing result, tester assignment, and device installation separately.
- Do not state that a TestFlight build is ready for the user until App Store Connect shows it processed and available to the intended tester group, or the installation is directly confirmed.
- Production Honey deployment is a separate approval and validation boundary from mobile distribution. Never infer one from the other.

## Required Validation
All code must pass before commit:
```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all
```
Web/mobile validation:
```bash
cd apps/mobile
npx tsc --noEmit
npx expo export --platform web
```

Validation must be proportional to the changed surface in addition to the required gates:
- Run focused regressions for the behavior changed.
- For Expo dependency or native configuration changes, run Expo Doctor, regenerate native projects only when required, install Pods from the lockfile, and build the relevant Release simulator/device target.
- For visible UI changes, inspect and interact with the rendered result at the relevant phone and desktop/web sizes.
- For performance changes, capture before/after evidence using the same build configuration and workload; include memory, responsiveness, dropped-frame/hang, or trace evidence appropriate to the claim.
- For server/provider changes, test the relevant route and provider outcome without exposing credentials.
- Documentation-only changes may reuse the already-validated code commit when code and dependency inputs are unchanged, but must pass `git diff --check`, link/path checks, and any repository documentation audit scripts.

## Dependencies
Key runtime dependencies (check `Cargo.toml` for versions):
- tokio (async runtime)
- anyhow, thiserror (error handling)
- serde, serde_json, serde_yaml, toml (serialization)
- tracing, tracing-subscriber (logging)
- ratatui (TUI framework)
- rusqlite (database)
- reqwest (HTTP client)
- wasmtime (WASM extensions)

## Staging tip policy (unify)

All new product work intended for the next coordinated release starts from
`codex/release-staging-20260801` on `honeycomb-Technologies/Mitsuro`.

- Treat that branch tip as the only authority for multi-machine agents/worktrees.
- Capture unique dirty work on named branches first; integrate into staging deliberately.
- Do not merge to `main`, tag, or restart production without explicit approval after staging validation.
