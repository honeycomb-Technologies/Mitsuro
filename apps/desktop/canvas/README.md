# Mitsuro Native Canvas

Canvas is Mitsuro's primary desktop GUI. It is the desktop product being built,
with acceptance work still required before a release is declared ready.

## Architecture

The native GPUI workspace uses `mitsuro-client` and the shared Mitsuro server.
The server owns Agent/Hive execution, canonical conversations, tool permissions,
Canvas resources, browser/terminal control leases, and durable recovery. Canvas
owns native presentation and interaction. It does not use the abandoned
`mitsuro-desktop-backend` dual-backend adapter.

Expo (`apps/mobile`) remains the mobile and web interface. The `mitsuro` CLI/TUI
remains the terminal interface. All surfaces use shared execution contracts.

## Develop and validate

```bash
cargo run -p mitsuro-canvas
cargo test --locked -p mitsuro-canvas
apps/desktop/canvas/packaging/test-package-linux-alpha.sh
```

Normal startup can connect to the shared server. Use the
[isolated smoke runner](acceptance/README.md) for validation against a private
companion. Do not restart or replace a running shared service to test the GUI.

The full native interaction, recovery, scaling, and sustained-session acceptance
pass is still outstanding. Unit tests and the limited smoke do not establish that
full acceptance pass. The unused acceptance prototype and no-op telemetry were
removed; neither provided runtime validation.

## Packaging

[Linux packaging](packaging/README.md) produces a portable alpha archive containing
Canvas and its server companion, with source provenance and checksums. It is the
current desktop release target. macOS/Windows desktop packaging is not established
by this Linux path. Creating or publishing an artifact is separate from source
readiness and requires the normal release approval.

The previous desktop implementations are abandoned; see the
[transition contract](../../../docs/frontends/desktop-transition.md).
