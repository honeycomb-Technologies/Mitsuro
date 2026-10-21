# Isolated Native Canvas Linux smoke

This runner checks a matching Canvas and companion build in an isolated Linux
profile. It starts the companion on a free non-3000 loopback port, verifies its
health identity and executable, and optionally opens a Canvas window on the
existing Wayland display. It checks that the existing server remains untouched.

## Build and run

From the repository root, build both binaries using the same locked checkout:

```bash
cargo build --release --locked -p mitsuro-canvas -p mitsuro
apps/desktop/canvas/acceptance/run-isolated-linux-smoke.sh \
  --server-bin "$(pwd)/target/release/mitsuro" \
  --canvas-bin "$(pwd)/target/release/mitsuro-canvas" \
  --honey-pid "$(curl -fsS http://127.0.0.1:3000/health | jq -r .pid)" \
  --companion-only
```

Omit `--companion-only` to exercise window startup on the existing display.
The runner creates a private profile and evidence directory, records executable
identity, and cleans up only processes it launched whose executables still
match. Use `--help` for supported evidence, port, duration, and keep options.
Pass the current live server PID explicitly; no machine-specific PID is assumed.
Do not point its companion at the canonical service port.

The companion is the `mitsuro` CLI in `serve --desktop-managed` mode;
`mitsuro-server` is a library crate. The private profile allows Canvas to reuse
the isolated companion's PID record instead of the user's running server.

## Evidence limits

This is a small companion/window smoke, not the full 30-minute product
acceptance harness. It does not prove gesture behavior, all resource lifecycle
races, native performance, or production readiness. The unregistered acceptance
prototype and no-op product instrumentation have been removed; a full interactive
acceptance harness remains future work.

For the GUI check, use an available `WAYLAND_DISPLAY` or `DISPLAY` and the
existing compositor. The runner records whether Hyprland reports a mapped
window when that query is available. A process being alive is distinct from a
mapped window or a successful interactive session.

Packaging is a separate operation; see
[the Linux alpha packaging guide](../packaging/README.md).
