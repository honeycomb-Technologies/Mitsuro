# Mitsuro Documentation

This directory contains documentation for the current product and codebase.
Planning notes, competitive research, experiments, staging handoffs, and old
audits are intentionally kept out of the public repository.

## Start here

- [What is Mitsuro?](architecture/overview.md)
- [How a message moves through the system](architecture/data-flow.md)
- [Why the architecture is structured this way](architecture/design-decisions.md)
- [Building and deployment](operations/build-and-deploy.md)

## Core engine

| Document | Covers |
| --- | --- |
| [Agent orchestrator](core-engine/agent-orchestrator.md) | The conversation and tool-execution loop |
| [AI providers](core-engine/ai-providers.md) | Provider integration and model selection |
| [Tool system](core-engine/tool-system.md) | Tools, permissions, and execution |
| [Context and memory](core-engine/context-and-memory.md) | Conversation history, memory, and compaction |
| [Request efficiency](core-engine/agent-efficiency.md) | Prompt size, caching, and usage controls |
| [Sub-agents](core-engine/sub-agents.md) | Delegated work and team coordination |

## Apps and interfaces

| Document | Covers |
| --- | --- |
| [Mobile app](frontends/mobile-app.md) | Expo and React Native client |
| [Desktop app](frontends/desktop-app.md) | Native Canvas desktop GUI |
| [Shared frontend packages](frontends/shared-packages.md) | API, state, and UI packages |
| [Web server and API](interfaces/server-api.md) | HTTP, streaming, and client endpoints |
| [Terminal UI](interfaces/tui.md) | Terminal client behavior |
| [Editor integration](interfaces/acp-editor-integration.md) | ACP-compatible editors |
| [Hive](interfaces/hive.md) | Durable background work |

## Data and extensions

| Document | Covers |
| --- | --- |
| [Storage](storage/persistence-layer.md) | Sessions and durable application state |
| [MCP, plugins, plans, and skills](extensions/mcp-and-plugins.md) | Extension points |
| [Plugin packages](extensions/plugin-packages.md) | Package format and trust |
| [Agent extensions](extensions/agent-extensions.md) | Local JavaScript and TypeScript extensions |
| [WASM extensions](extensions/wasm-extensions.md) | WebAssembly extension support |
| [Game Boy Color example](extensions/gameboy-color.md) | A small extension example |

## Product references

- [Hive engineering notes](hive/README.md)
- [Mitsuro brand system](rebrand/README.md)
- [Build, CI, and packaging](operations/build-and-deploy.md)

When documentation and code disagree, treat the code and tests as the current
behavior and update the documentation in the same change.
