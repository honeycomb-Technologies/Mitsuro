# Desktop App — Native Canvas

Mitsuro's canonical desktop GUI is [`apps/desktop/canvas`](../../apps/desktop/canvas/).
Canvas is the sole desktop development and packaging target. Expo owns mobile and
web, and the CLI/TUI owns the terminal interface.

Canvas uses native GPUI presentation and the shared Mitsuro client/server
contracts. Agent and Hive execution, permissions, canonical history, resource
ownership, and recovery remain server-owned. New desktop work belongs in Canvas;
shared runtime improvements belong in core/server.

```bash
cargo run -p mitsuro-canvas
cargo test --locked -p mitsuro-canvas
```

Use the [Canvas guide](../../apps/desktop/canvas/README.md) for architecture and
acceptance boundaries, [isolated smoke](../../apps/desktop/canvas/acceptance/README.md)
for private companion testing, and [Linux packaging](../../apps/desktop/canvas/packaging/README.md)
for the portable alpha archive. Full GUI acceptance remains required; primary
product status is not a claim that it has passed or has been deployed.

The old GPUI dual-backend application, Tauri shell, and separate desktop Expo UI
are abandoned. Their retained source is historical, not an alternative feature
target. See the [transition contract](desktop-transition.md) for installed-client,
data, release, and rollback boundaries.
