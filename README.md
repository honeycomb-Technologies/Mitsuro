<p align="center">
  <img src="assets/branding/mitsuro/mitsuro-lockup-horizontal.svg" alt="Mitsuro" width="360">
</p>

<p align="center">
  An AI workspace for interactive coding, project research, and long-running agent work.
</p>

<p align="center">
  <a href="#install">Install</a> ·
  <a href="docs/README.md">Documentation</a> ·
  <a href="https://github.com/honeycomb-Technologies/Mitsuro/releases">Releases</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

## What is Mitsuro?

Mitsuro brings conversations, code sessions, and autonomous work into one
connected workspace.

- **Agent** is the interactive experience for coding, questions, and project
  work.
- **Hive** runs durable tasks that can continue in the background and recover
  across restarts.
- **Your server** owns sessions, tools, and project access. Mobile, web,
  desktop, terminal, and editor clients connect to the same source of truth.

Mitsuro works with multiple AI providers and keeps provider credentials under
your control. Tool permissions remain explicit, and project data is stored by
the Mitsuro server rather than split across client-only copies.

## Ways to use it

| Surface | Best for |
| --- | --- |
| **iPhone** | Conversations, code sessions, Hive runs, reports, and remote tools |
| **Web** | The full Mitsuro workspace in a browser |
| **Desktop** | Native Canvas, the primary desktop GUI |
| **Terminal** | Interactive coding via the modern Mitsuro TUI (`mitsuro` / `krusty` alias) |
| **Editor** | Connecting compatible editors through ACP |

## Highlights

- Stream conversations and tool activity as work happens.
- Resume saved sessions and recover interrupted work.
- Switch between supported providers and models without changing clients.
- Review approvals, errors, reports, and background runs from the same
  workspace.
- Extend the agent with skills, plugins, MCP servers, and local extensions.
- Keep the server self-hosted and connect privately from your other devices.

## Install

Download the installer, inspect it if desired, and run it:

```bash
curl -fsSLO https://raw.githubusercontent.com/honeycomb-Technologies/Mitsuro/main/install.sh
sh install.sh
```

The installer verifies the published SHA-256 checksum before installing a
release.

Start the terminal app:

```bash
mitsuro
```

When a newer GitHub release exists, the TUI shows a notice. Press `Ctrl+U`
(or run `/update`) to quit, install the full release (TUI, Hive, and server
units), then relaunch. `mitsuro update` checks without applying;
`mitsuro update --apply` runs the same installer path from the shell.

You can also rerun `install.sh` or `brew upgrade BurgessTG/tap/mitsuro`.

Start the server and web workspace:

```bash
mitsuro serve
```

`mitsuro` is the primary command. The deprecated `krusty` command remains a
tested compatibility alias during the announced migration window.

### Build from source

```bash
git clone https://github.com/honeycomb-Technologies/Mitsuro.git
cd Mitsuro
cargo build --release
```

See [Building and deployment](docs/operations/build-and-deploy.md) for platform
requirements and client build instructions.

## Repository guide

| Path | Contents |
| --- | --- |
| [`crates/`](crates/) | Shared Rust engine, command-line app, and server |
| [`apps/mobile/`](apps/mobile/) | Expo app used by the iPhone and web clients |
| [`apps/desktop/canvas/`](apps/desktop/canvas/) | Native Canvas desktop GUI |
| [`packages/`](packages/) | Shared TypeScript API, state, and UI packages |
| [`docs/`](docs/README.md) | Stable architecture and contributor documentation |

## Development

The repository uses Rust for the core runtime and Expo/React Native for the
shared client.

```bash
cargo check --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --all
```

For the mobile and web client:

```bash
cd apps/mobile
bun install --frozen-lockfile
npx tsc --noEmit
npx expo export --platform web
```

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Detailed
engineering guidance lives in [AGENTS.md](AGENTS.md).

## Documentation

- [Project overview](docs/architecture/overview.md)
- [Architecture and data flow](docs/architecture/data-flow.md)
- [Mobile app](docs/frontends/mobile-app.md)
- [Hive](docs/interfaces/hive.md)
- [Extensions and integrations](docs/extensions/mcp-and-plugins.md)
- [Build and deployment](docs/operations/build-and-deploy.md)

## Project status

Mitsuro is under active development. Current source, release artifacts, and
documentation use Mitsuro for the harness and Hive for durable autonomous work.

## License

Mitsuro is available under the [MIT License](LICENSE).
