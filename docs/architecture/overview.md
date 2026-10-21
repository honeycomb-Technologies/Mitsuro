# Mitsuro architecture and module inventory

Mitsuro combines a shared Rust agent runtime with terminal, editor, desktop,
and Expo clients. The HTTP server and the independently supervised Hive daemon
own different runtime responsibilities. A Rust module or shared package is not
necessarily a separate application or process.

## Applications and entry points

| Surface | Entry point | Responsibility |
| --- | --- | --- |
| CLI and terminal UI | [`crates/mitsuro-cli/src/main.rs`](../../crates/mitsuro-cli/src/main.rs) | `mitsuro` opens the TUI; subcommands start the server, ACP, Hive control, updates, and identity migration |
| Server and web client | [`crates/mitsuro-server/src/lib.rs`](../../crates/mitsuro-server/src/lib.rs) | `mitsuro serve` starts the API; release builds embed the Expo web export from `apps/mobile/dist` |
| Hive service | [`crates/mitsuro-hive/src/main.rs`](../../crates/mitsuro-hive/src/main.rs) | `mitsuro-hive` runs the Unix IPC service and durable background scheduler |
| Native Canvas desktop | [`apps/desktop/canvas/src/main.rs`](../../apps/desktop/canvas/src/main.rs) | `mitsuro-canvas` presents Agent, browser, terminal, and spatial workspace resources |
| Expo mobile/web | [`apps/mobile/app`](../../apps/mobile/app) | React Native client, web export, connection setup, chat, sessions, settings, and Hive surfaces |
| Editor integration | [`crates/mitsuro-core/src/acp`](../../crates/mitsuro-core/src/acp) | `mitsuro acp` exposes the agent over the editor protocol; it is a mode of the CLI |
| Product website | [`apps/website`](../../apps/website) | Separate static SvelteKit website and plugin catalog; it is not the server's web client |

The Expo project has iOS and Android configuration. Widgets, Live Activities,
MetricKit diagnostics, and Liquid Glass native integration are Apple-specific.
The repository's native mobile release workflow targets iOS/TestFlight; web
export or TypeScript checks do not establish Android native build acceptance.

## Rust workspace

The [workspace manifest](../../Cargo.toml) includes `crates/*` and
`apps/desktop/canvas`: eight packages, seven Mitsuro packages and `grok-auth`.
The `crates/mitsuro-cli` directory contains the package named `mitsuro`.

| Package | Source | Role |
| --- | --- | --- |
| `mitsuro` | [`crates/mitsuro-cli`](../../crates/mitsuro-cli) | CLI entry points and terminal presentation; depends on core and server |
| `mitsuro-core` | [`crates/mitsuro-core`](../../crates/mitsuro-core) | Shared agent, provider, tool, persistence, and integration logic |
| `mitsuro-server` | [`crates/mitsuro-server`](../../crates/mitsuro-server) | Axum server library linked into the CLI; also supplies the shared Hive execution host |
| `mitsuro-hive` | [`crates/mitsuro-hive`](../../crates/mitsuro-hive) | Supervised daemon, scheduler, recovery, deliveries, heartbeat, and group execution |
| `mitsuro-hive-protocol` | [`crates/mitsuro-hive-protocol`](../../crates/mitsuro-hive-protocol) | Versioned, authenticated Unix IPC contracts, framing, client, and event subscription |
| `mitsuro-client` | [`crates/mitsuro-client`](../../crates/mitsuro-client) | Typed Rust HTTP/SSE/WebSocket transport used by Canvas |
| `mitsuro-canvas` | [`apps/desktop/canvas`](../../apps/desktop/canvas) | Native desktop executable and local server supervision |
| `grok-auth` | [`crates/grok-auth`](../../crates/grok-auth) | Reusable Grok login, token refresh, credential storage, and authenticated HTTP client |

The deprecated command aliases are compatibility entry points,
not separate implementations. Expo uses the TypeScript `packages/state`; there
is no active Rust client-state crate.

## Core modules

These are all top-level modules declared by
[`mitsuro-core/src/lib.rs`](../../crates/mitsuro-core/src/lib.rs).

| Module | Responsibility |
| --- | --- |
| [`acp`](../../crates/mitsuro-core/src/acp) | Editor protocol server, sessions, event bridge, tool and workspace adapters |
| [`agent`](../../crates/mitsuro-core/src/agent) | Canonical orchestrator, loop events, execution, approvals/hooks, cancellation, context, compaction, learning, delegated agents, and autonomous Worker execution |
| [`ai`](../../crates/mitsuro-core/src/ai) | Provider clients/catalogs, model identity, request transforms, parsers, streaming, reasoning, retries, and usage |
| [`auth`](../../crates/mitsuro-core/src/auth) | Provider login, OAuth/device flows, credential resolution and refresh |
| [`constants`](../../crates/mitsuro-core/src/constants.rs) | Shared runtime constants |
| [`extensions`](../../crates/mitsuro-core/src/extensions) | WASM extension host plus local agent-extension runtime and manifests |
| [`git`](../../crates/mitsuro-core/src/git) | Shared Git commands, status, changes, and pull-request helpers |
| [`hive`](../../crates/mitsuro-core/src/hive) | Scheduling primitives: recurrence, retry, misfire, state transitions, and time |
| [`identity`](../../crates/mitsuro-core/src/identity.rs) | Canonical identifiers, bounded legacy readers, and offline state migration |
| [`mcp`](../../crates/mitsuro-core/src/mcp) | MCP server connections, discovery, OAuth, stdio/HTTP transports, and tool adapters |
| [`paths`](../../crates/mitsuro-core/src/paths.rs) | Shared configuration and data-path resolution |
| [`plan`](../../crates/mitsuro-core/src/plan) | Session-linked plans, lifecycle, persistence coordination, and plan-file handling |
| [`plugins`](../../crates/mitsuro-core/src/plugins) | Package catalogs, trust/signatures, installation, permissions, and lockfiles |
| [`ports`](../../crates/mitsuro-core/src/ports) | Local TCP listener discovery for preview/resource surfaces |
| [`process`](../../crates/mitsuro-core/src/process) | Background process registry, environments, completion events, and signals |
| [`server_instance`](../../crates/mitsuro-core/src/server_instance.rs) | Canonical server PID/lock ownership, health identity, and reuse |
| [`skills`](../../crates/mitsuro-core/src/skills) | Filesystem skill discovery, validation, loading, and package-root precedence |
| [`storage`](../../crates/mitsuro-core/src/storage) | SQLite migrations and durable sessions/messages, recovery, credentials, plans, delegation, Hive, memory/reports, diagnostics, push, and canvases |
| [`tailscale`](../../crates/mitsuro-core/src/tailscale.rs) | Tailnet device discovery and HTTPS serve integration |
| [`tools`](../../crates/mitsuro-core/src/tools) | Tool registry, context/governance, file and shell tools, search, browser/web, plans/workflows, skills, memory, reports, and delegation |
| [`updater`](../../crates/mitsuro-core/src/updater) | Update channels, release discovery, staged updates, and managed installation |
| [`workflow`](../../crates/mitsuro-core/src/workflow) | Durable Goals, plan revisions, dependencies, bounded attempts, acceptance, and Worker workflow lifecycle |

Within `agent`, `orchestrator` and `loop_kernel` coordinate provider and tool
turns; `executor` executes calls; `subagent` owns delegated isolation and
scheduling; `autonomy` owns classification/tick machinery; `worker_conversation`,
`worker_goal`, and `worker_introduction` implement the corresponding Hive work.
`plan` and `workflow` are distinct active boundaries, not interchangeable stores.

## Server and interface internals

The [server router](../../crates/mitsuro-server/src/routes/mod.rs) exposes sessions,
chat, models, tools, files/Git, projects, canvases/resources, browser, terminals,
processes, ports, Hive, memories/reports, credentials/OAuth, extensions/plugins,
MCP, skills, hooks, push/APNs, diagnostics, preview settings, and server control.
Server modules also own authentication, remote pairing/access, SSE delivery,
control leases, presence, notifications, and Hive execution/IPC integration.
The Hive daemon reuses the execution host without starting an HTTP listener.

The terminal's [`tui_v2`](../../crates/mitsuro-cli/src/tui_v2) contains app state,
reducer/effects, conversation projection, input/slash commands, layout, rendering,
themes/motion, and runtime services. [`tui_support`](../../crates/mitsuro-cli/src/tui_support)
provides service construction, auth, Markdown, and terminal graphics helpers.
Canvas has its own app/agent presentation, spatial model, resource journal,
server supervisor, browser/terminal surfaces, preferences, and themes.

## Frontend and native packages

| Path | Responsibility |
| --- | --- |
| [`packages/api`](../../packages/api) | TypeScript client, wire contracts, stream parsing, citations, and compatibility adapters |
| [`packages/state`](../../packages/state) | Zustand session/workspace/Git/plan state, streaming, persistence/recovery, performance, and diagnostic recording |
| [`packages/ui`](../../packages/ui) | Shared design tokens and theme definitions |
| [`apps/mobile/components`](../../apps/mobile/components) | Chat, Hive, settings, toolbox, layout, and reusable presentation |
| [`apps/mobile/hooks`](../../apps/mobile/hooks) | Connection, stores, navigation/deep links, notifications, widget and Live Activity lifecycles |
| [`apps/mobile/platform`](../../apps/mobile/platform) | Native/web adapters, storage identity, recovery boundaries, and browser stream auth |
| [`apps/mobile/diagnostics`](../../apps/mobile/diagnostics) | Client diagnostics collection/upload and native bridge coordination |
| [`apps/mobile/modules/mitsuro-diagnostics`](../../apps/mobile/modules/mitsuro-diagnostics) | Optional Apple MetricKit and performance bridge, including legacy native-name compatibility |
| [`apps/mobile/modules/mitsuro-liquid-glass`](../../apps/mobile/modules/mitsuro-liquid-glass) | Optional Apple glass view with non-Apple fallbacks |
| [`apps/mobile/widgets`](../../apps/mobile/widgets) | Chat/Hive widgets, legacy widget-kind compatibility, and chat Live Activity |
| [`apps/mobile/plugins`](../../apps/mobile/plugins) | Expo configuration hook for the Ghostty keyboard accessory |

`apps/mobile/components/desktop` contains the web terminal used by Expo. Its
name does not make it a retired desktop application.

## Integrations, delivery, and support

- [`wit`](../../wit) defines the WASM extension interface; [`examples/plugins`](../../examples/plugins)
  contains sample JavaScript/TypeScript and native Rust plugin packages.
- [`docs/extensions/catalog.json`](../extensions/catalog.json) feeds the static
  website catalog. Provider, ACP, MCP, extension, and skill adapters remain core
  integrations rather than standalone applications.
- [`deploy/systemd`](../../deploy/systemd) supplies server and Hive service/socket
  units; [`install.sh`](../../install.sh), [`aur`](../../aur), and
  [Homebrew templates](../../.github/homebrew) handle distribution.
- [CI/release workflows](../../.github/workflows), [Sampo configuration](../../.sampo/config.toml),
  [scripts](../../scripts), and [Canvas packaging](../../apps/desktop/canvas/packaging)
  manage checks, versioning, release artifacts, runtime smoke/evaluation, and operations.
- [`assets`](../../assets) and [`icons`](../../icons) hold shared visual resources;
  [`docs`](..) holds architecture, contracts, operating instructions, and compatibility records.

The old terminal v1, GPUI desktop, Tauri shell, Expo desktop application,
desktop-backend crate, GPUI mobile/iOS experiment, and its Rust client-state
crate are retired. Git history preserves them; they are not current product
modules. Installed-data readers, identity aliases, and widget compatibility
remain intentional boundaries with separate retirement requirements.
