# Mitsuro Desktop — Native Canvas

Native Canvas (`canvas/`, package `mitsuro-canvas`) is the canonical desktop GUI
and the sole desktop development target. Expo in `apps/mobile` owns mobile and web;
the CLI/TUI owns the terminal interface.

Canvas renders a native spatial workspace and uses the shared Mitsuro server for
Agent, Hive, sessions, files, Git, browser, terminal, and durable resource authority.
Shared behavior belongs in core/server and the Rust client contracts.

## Development

```bash
cargo run -p mitsuro-canvas
```

Read [Canvas](canvas/README.md) for the product boundary, isolated validation,
packaging, and the remaining acceptance work.

## Abandoned implementations

`gpui/` (the earlier dual-backend client), `shell/` (Tauri), `ui/` (desktop Expo),
and `crates/mitsuro-desktop-backend` are abandoned. They are retained only as
historical source and compatibility reference, excluded from active desktop
builds and releases. Do not add features, repair their UI, or port new work there.
GPUI remains Canvas's native rendering framework; the abandoned target is the
older application in `gpui/`.

Existing installed applications, user data, compatibility readers, and running
service artifacts are preserved. Retirement does not automatically migrate or
uninstall an existing application. See [desktop transition](../../docs/frontends/desktop-transition.md).
