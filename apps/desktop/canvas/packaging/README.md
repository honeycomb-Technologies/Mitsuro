# Mitsuro Canvas Linux alpha package

This directory builds a self-contained, non-installing FHS archive for the
native Linux/Wayland alpha. It deliberately does not modify `/usr`, install a
desktop entry, start Mitsuro, activate `mitsuro-serve.service`, or replace any
existing Mitsuro desktop surface.

The archive carries exactly these runtime locations:

- `usr/bin/mitsuro-canvas`
- `usr/libexec/mitsuro/mitsuro` (the companion path used by `ServerSupervisor`)
- `usr/share/mitsuro-canvas/assets`
- desktop, AppStream, icon, license, provenance, and checksum metadata below
  `usr/share`

## Build and package

Create both binaries from one clean checkout and one locked dependency graph:

```bash
cargo build --release --locked -p mitsuro-canvas -p mitsuro
apps/desktop/canvas/packaging/package-linux-alpha.sh \
  --canvas-bin "$(pwd)/target/release/mitsuro-canvas" \
  --server-bin "$(pwd)/target/release/mitsuro" \
  --output-dir /absolute/path/to/output
```

The packager requires absolute executable inputs at this checkout's exact Cargo
`target/release` paths. It checks matching crate and binary versions, matching
Linux ELF architecture, a clean source tree, static metadata, and the complete
staging tree. A locked Release build embeds the exact full source commit before
staging, so Cargo either confirms those artifacts or rebuilds them from the
exact clean checkout. The archive records the same full source commit and
binary SHA-256 values and has an archive SHA-256 sidecar. Existing output is
never overwritten.

Run the cheap metadata and synthetic staging checks without building Release:

```bash
apps/desktop/canvas/packaging/package-linux-alpha.sh --check-metadata
apps/desktop/canvas/packaging/test-package-linux-alpha.sh
```

`desktop-file-validate` and `appstreamcli` are used when installed. The synthetic
test also proves that repeated archives are byte-identical and that an extracted
archive passes the staging validator. Packaging requires Bash, Cargo, Git, GNU
coreutils and tar, gzip, jq, and readelf.

## Scope and limitations

This is a portable archive, not an AppImage, Flatpak, signed repository package,
or dependency resolver. Its dynamically linked binaries still require a
compatible Linux userspace, Wayland/XWayland, graphics stack, and GPUI runtime
dependencies. Extracting the archive does not install it. Running the extracted
Canvas may reuse a compatible server, ask systemd to start an already-loaded
service, or launch the bundled companion according to `ServerSupervisor`; the
packaging command itself does none of those things.

For an isolated Linux/Wayland smoke against a private companion (never live
Honey on :3000), see `apps/desktop/canvas/acceptance/README.md` and
`run-isolated-linux-smoke.sh`. That runner is not part of packaging and does
not produce the alpha archive.
