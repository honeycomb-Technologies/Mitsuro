# What Is Mitsuro?

Mitsuro is an AI coding assistant that lives where you work. It runs as a single binary on your machine and gives you access to large language models — like Claude, GPT, and others — through whatever interface fits your workflow: a terminal, a web browser, a mobile app, or directly inside your code editor.

What makes Mitsuro different from a chatbot is that it can actually do things. It can read your files, edit your code, run shell commands, search your codebase, and manage background processes. It plans complex tasks, breaks them into phases, and executes them with your oversight. It's not just answering questions — it's an agent that takes action.

## The Problem It Solves

Modern AI coding tools tend to lock you into one interface and one AI provider. You use one tool in your terminal, another in your browser, and yet another in your editor — and they don't share context, sessions, or configuration. Switching between them means losing your conversation, re-explaining your project, and managing multiple API keys.

Mitsuro solves this by being a unified platform. One binary, one configuration, one set of sessions — accessible from anywhere. Start a conversation in the terminal, continue it from your phone, or hand it off to an autonomous agent that works while you sleep.

## The Four Interfaces

Mitsuro exposes the same core engine through four different surfaces. They all share the same agent orchestrator, tool system, storage, and configuration — the only difference is how you interact with them.

### 1. Terminal UI (TUI)

The flagship interface. Run `mitsuro` in your terminal and you get a full-featured chat interface built with Ratatui. It has syntax-highlighted code blocks, streaming responses, markdown rendering, 29 color themes, and a slash command system for everything from switching models to managing plugins. You can attach files, open an embedded terminal, scroll through conversation history, and toggle between plan mode (where the AI designs before it acts) and build mode (where it executes).

This is the interface for engineers who live in the terminal. It's fast, keyboard-driven, and designed for deep coding sessions.

### 2. Web Server & API

Run `mitsuro serve` and the binary launches an HTTP server with an embedded web frontend. The same React-based UI that powers the mobile app gets served directly from the binary — no separate frontend deployment needed. The API exposes REST endpoints and SSE streaming for chat, plus WebSocket support for terminal emulation.

This is the interface for when you want a graphical UI, or when you need to access Mitsuro from another device on your network. If Tailscale is installed, it automatically configures remote HTTPS access so you can reach your Mitsuro instance from anywhere.

### 3. Editor Integration (ACP)

Run `mitsuro acp` and it becomes an Agent Client Protocol server that communicates over JSON-RPC via stdin/stdout. This lets code editors — Zed, Neovim, JetBrains, and others — spawn Mitsuro as a subprocess and interact with it through a standardized protocol. The editor sends prompts and receives structured responses, including tool calls that the editor can render natively.

This is the interface for when you want AI assistance without leaving your editor.

### 4. Hive (Autonomous Agent)

Hive is Mitsuro's autonomous mode. You submit a task — "refactor the authentication module" or "write integration tests for the API" — and Hive works on it in the background. It has a tick-based execution engine that wakes up, does work, sleeps, and repeats. You can attach to its event stream, pause it, resume it, or cancel it from the CLI.

Hive operates through the same tool system as interactive sessions, but with an auto-classifier that evaluates each tool call for safety and appropriateness without requiring human approval for every step.

This is the interface for tasks that are well-defined enough to delegate but would take too long to babysit.

## How the Pieces Fit Together

At the heart of everything is the **agent orchestrator**. This is a loop that:

1. Takes a conversation (user messages plus any prior assistant responses)
2. Sends it to an AI provider (Anthropic, OpenAI, OpenRouter, or others)
3. Streams the response back
4. When the AI wants to use a tool (read a file, run a command, etc.), pauses the stream, executes the tool, and feeds the result back
5. Repeats until the AI is done or the user interrupts

The orchestrator doesn't know or care which interface is using it. It emits events (`LoopEvent`) and accepts inputs (`LoopInput`). The TUI maps those events to terminal rendering. The web server maps them to SSE messages. The ACP server maps them to JSON-RPC notifications. Hive maps them to its tick engine. Same brain, different faces.

Surrounding the orchestrator are the supporting systems:

- **AI Provider Layer** — Abstracts away the differences between LLM APIs. Anthropic uses one message format, OpenAI uses another, Google uses a third. The provider layer normalizes them so the orchestrator only sees one interface.

- **Tool Registry** — Manages 50+ built-in tools (file I/O, shell execution, code search, sub-agent spawning) plus any tools discovered via MCP servers. Each tool has permission policies, pre/post hooks, and truncation rules.

- **Storage** — SQLite database that persists everything: conversation sessions, user preferences, API credentials, plan state, push notification subscriptions, and runtime traces for Hive.

- **Extension System** — A WebAssembly runtime (using Wasmtime) that can load Zed-compatible extensions for language server support and other capabilities.

- **MCP Client** — Connects to Model Context Protocol servers to discover additional tools and resources at runtime.

## The Codebase Structure

The project is organized as a Rust workspace with three crates, plus TypeScript packages for the frontend:

```
crates/
  mitsuro-core/     The shared library. Everything the orchestrator needs: AI clients,
                   tool registry, storage, ACP server, MCP client, extensions, auth,
                   planning, skills, plugins. This is where the brain lives.

  mitsuro-cli/      The binary. Entry point (main.rs), TUI implementation, serve mode
                   setup. This is the executable you install and run.

  mitsuro-server/   The HTTP API. Axum-based web server with REST routes, WebSocket
                   terminal, push notifications, and Hive runtime management. Gets
                   compiled into the CLI binary.

apps/
  mobile/          Expo/React Native app. Serves as both the mobile app (iOS/Android)
                   and the web frontend that gets embedded into the server binary.

  desktop/canvas/  Canonical native desktop GUI. Consumes shared Mitsuro client/server
                   contracts for Agent, Hive, resources, and durable recovery.

  desktop/gpui/    Abandoned earlier desktop application, retained as historical source.
  desktop/shell/   Abandoned Tauri host; desktop/ui is also abandoned.

packages/
  api/             TypeScript API client shared between mobile and web.
  state/           State management (Zustand) shared between mobile and web.
  ui/              Design tokens and theme definitions shared between mobile and web.
```

The key insight is that `mitsuro-core` is the shared brain, and everything else is a presentation layer. The CLI, the server, the ACP server, and Hive all import `mitsuro-core` and use the same orchestrator, the same tools, and the same storage.

## Multi-Provider AI

Mitsuro doesn't tie you to one AI provider. It supports:

- **Anthropic** — Live Claude catalog via direct API or OAuth, with curated Opus/Fable/Sonnet/Haiku fallbacks
- **OpenAI** — GPT and Codex catalogs via API key or account-scoped ChatGPT OAuth
- **OpenRouter** — Live tool-capable catalog spanning multiple upstream providers
- **MiniMax** — Live catalog with curated MiniMax M3 and M2.7 capability overlays
- **Z.ai** — Static GLM 5.2, GLM 5 Turbo, and GLM 4.7 catalog
- **Grok** — Live Grok Build catalog through the X subscription CLI proxy

You can switch providers and models mid-conversation. The provider layer handles the format differences (Anthropic's message format vs OpenAI's vs Google's) so the rest of the system doesn't have to care.

## What Makes It Interesting

A few architectural choices worth noting:

**Single binary distribution.** The entire system — Rust backend, web frontend, SQLite database — ships as one executable. No Docker, no microservices, no separate database server. You install it and it works. The web frontend is literally embedded in the binary at compile time.

**Local-first.** Everything runs on your machine and your data stays on your machine. Sessions are stored in SQLite at `~/.mitsuro/`. API keys are encrypted locally. There's no cloud service in the middle (unless you choose to use OpenRouter or similar).

**Presentation-agnostic core.** The orchestrator loop is completely decoupled from how you interact with it. This means adding a new interface (say, a Slack bot or a VS Code extension) requires writing only the thin presentation layer — the entire agent capability comes for free.

**Real tool execution.** Unlike chatbots that can only generate text, Mitsuro actually executes tools in your environment. It reads and writes files, runs shell commands, searches codebases, and manages processes. The safety model is permission-based: in supervised mode, write operations require your approval; in autonomous mode, the AI operates freely.

**WASM extensions.** The extension system uses WebAssembly with the same interface definitions as Zed editor extensions. This means the growing ecosystem of Zed extensions — language servers, formatters, linters — can potentially run inside Mitsuro without modification.
