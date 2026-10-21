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

## Retired implementations

`gpui/` (the earlier dual-backend client), `shell/` (Tauri), `ui/` (desktop Expo),
and `crates/mitsuro-desktop-backend` were retired and removed. Their source and
compatibility readers remain recoverable from Git history at `07625972`.
GPUI remains Canvas's native rendering framework; the retired target was the
older application in `gpui/`.

Existing installed applications, user data, shared runtime compatibility readers,
and running service artifacts are preserved. Retirement does not automatically migrate or
uninstall an existing application. See [desktop transition](../../docs/frontends/desktop-transition.md).
