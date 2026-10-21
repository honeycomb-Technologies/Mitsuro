# Building, CI/CD & Packaging

This document explains how Mitsuro is built, tested, and distributed across all of its targets: the Rust CLI, the self-hosted server, the Expo mobile app, and the Native Canvas desktop GUI. It covers the workspace layout, the quality gates enforced in CI, how releases are cut, and the various channels through which users can install Mitsuro.

## Rust workspace

The Rust side of Mitsuro is organized as a Cargo workspace with seven Mitsuro crates plus the `grok-auth` support crate. The primary runtime boundaries are:

- **mitsuro-cli** (`crates/mitsuro-cli`) -- The terminal application with the TUI. This is the default member of the workspace, so a bare `cargo build` compiles it. It depends on both `mitsuro-core` and `mitsuro-server`.
- **mitsuro-core** (`crates/mitsuro-core`) -- The core library containing AI provider integrations, tool implementations, the ACP/MCP protocol layers, WASM extension hosting, and local storage. Everything shared between the CLI and the server lives here.
- **mitsuro-server** (`crates/mitsuro-server`) -- The self-hosted API server built on Axum. It serves the REST/WebSocket APIs used by the mobile and desktop clients, and optionally embeds the web frontend (more on that below).
- **mitsuro-hive** (`crates/mitsuro-hive`) -- The independently supervised autonomous execution owner, durable scheduler, recovery loop, and event-log service.
- **mitsuro-hive-protocol** (`crates/mitsuro-hive-protocol`) -- The typed, authenticated local protocol shared by the server, daemon, and control clients.
- **mitsuro-client** (`crates/mitsuro-client`) -- Typed Rust HTTP/SSE/WebSocket client used by Canvas.
- **mitsuro-canvas** (`apps/desktop/canvas`) -- Native desktop workspace using the shared server contracts.
- **grok-auth** (`crates/grok-auth`) -- Provider authentication support library used by core.

Expo client state lives in `packages/state`. The unused Rust client-state
experiment has been retired with its former GPUI mobile consumers; it is not
part of the active workspace.

The workspace root `Cargo.toml` sets a few important release profile options: link-time optimization (`lto = true`), a single codegen unit (`codegen-units = 1`), and symbol stripping (`strip = true`). These produce smaller, faster release binaries at the cost of longer compile times. The workspace also defines shared lint rules so all workspace crates enforce the same code quality standards through Clippy.

The product-facing Mitsuro crates and desktop bundle share a coordinated
release version. The internal Hive daemon/protocol crates keep independent
package versions; release tags are validated dynamically against the `mitsuro`
package version.

## Build commands

Four commands must pass before any code is committed or released:

```bash
cargo check --workspace                 # Compile the workspace
cargo test --workspace                  # Run all tests
cargo clippy --workspace -- -D warnings # Lint with warnings as errors
cargo fmt --all                         # Format all crates
```

`cargo fmt` enforces consistent formatting. `cargo clippy` catches common mistakes and enforces the workspace lint configuration, which warns on dead code, unused imports, redundant clones, and unnecessary wraps. The build and test steps ensure everything compiles and the test suite passes.

For release builds, `cargo build --release` enables the LTO and stripping profile described above. Cross-compilation for other architectures uses the `cross` tool (see below).

## CI pipeline

The CI workflow (`.github/workflows/ci.yml`) runs on every push to `main` and on every pull request targeting `main`. It breaks the quality gates into parallel jobs so failures surface quickly:

| Job | What it does |
|---|---|
| **Canonical Identity** | Exact compatibility-name audit plus `install.sh` migration, publication, health, and rollback fixtures |
| **Rust Check** | `cargo check --workspace` -- fast compilation check without producing binaries |
| **Rust Test** | `cargo test --workspace` -- runs the full test suite |
| **Rust Clippy** | `cargo clippy --workspace -- -D warnings` -- lint pass with zero tolerance for warnings |
| **Rust Format** | `cargo fmt --all -- --check` -- verifies formatting without modifying files |
| **Windows Check** | `cargo check --locked --target x86_64-pc-windows-msvc -p mitsuro` -- checks the CLI and server with Windows dependency selection |
| **Client Quality Gates** | Calls the reusable client workflow: Bun API contract tests, Deno shared-state type-check/tests, mobile TypeScript, and Expo web export |
| **Desktop Linux Bundle** | Builds the Expo companion web surface and release Canvas/server binaries, then packages and validates their exact-source Linux archive; also checks metadata and deterministic staging |

Linux Rust jobs install the native headers required by the workspace. The
required `Desktop Linux Bundle` status builds Canvas using the release packaging
path and native dependencies, including desktop-file and AppStream validation.
It keeps the existing required status name without building the retired Tauri
application. The archive stays under `target/` in CI; this gate does not publish
or install it. Workspace tests remain in the separate Rust Test job.

All Rust jobs use `dtolnay/rust-toolchain@master` on the rolling stable channel.
Use the same stable Rust version for local validation; an older local Clippy
passing does not establish that the current CI toolchain passes. Keep shared
server dependencies outside target-specific tables so Windows checks include
the HTTP, WebSocket, and push notification clients. A PR cannot merge until
every required job is green.

### Mixed-version Code control

Client and server upgrades can arrive independently. Clients probe the Code
control route before mutations; an absent route or the preceding server's SPA
fallback allows the original mutation without a lease. Authentication, network, and malformed
JSON failures still stop the mutation. The next mutation probes again so a
server upgrade enables leases without restarting the client.

Previously shipped clients do not carry a stable device identity or lease token.
The server groups their mutations under one compatibility controller with a
15-second stale window. A live modern controller rejects legacy mutations;
modern clients must wait for the compatibility controller to expire or take
control explicitly. Invalid or stale supplied tokens never become legacy
requests. Existing session ownership checks apply to both paths.

### Default-branch and governance preflight

Before routing autonomous continuation, opening broad implementation work, or validating release/TestFlight governance, run the read-only default-branch preflight from the repository root:

```bash
scripts/check-default-branch-preflight.sh
```

The preflight compares the GitHub default branch reported by `gh repo view`, the authoritative remote HEAD from `git ls-remote --symref origin HEAD`, and the local tracking symref at `refs/remotes/origin/HEAD`. Mitsuro's expected default branch is `main`; if local `origin/HEAD` still points at a stale branch such as `origin/dev`, fix the local ref before using it to choose a base branch. The check is ruleset-aware: a classic branch-protection 404 is acceptable only when an active branch ruleset applies to `refs/heads/main`.

The preflight is safe for validation: it reads repository metadata, remote refs, classic branch protection, and repository rulesets only. It does not push, delete branches, trigger workflows, edit settings, create releases, or read secret values.

## Release automation

The operator path is [release.md](release.md): promote staging to `main`,
Version (Sampo) tags `v{version}`, and **Release binaries** uploads the GitHub
Release. A `v*` tag without archives is not a release; `install.sh` reads
assets, not tags. The packaging workflow still runs on a protected `v*` tag
or `workflow_dispatch` with that tag.
Before packaging, the release workflow calls
`.github/workflows/client-quality.yml`; no release build starts unless the API
streaming/error contracts, shared-state type-check/tests, mobile TypeScript
check, and Expo web export all pass. Its prerequisite canonical-identity job
also runs the exact-name policy and `sh install.sh --self-test`; the Rust test
job remains the authority for the SQLite migration implementation itself. The remaining release workflow
(`.github/workflows/release.yml`) has three artifact stages:

**1. Build matrix.** A matrix job compiles release binaries for five targets:

| Target | Runner |
|---|---|
| `x86_64-unknown-linux-gnu` | `ubuntu-latest` |
| `aarch64-unknown-linux-gnu` | `ubuntu-24.04-arm` (native ARM runner) |
| `x86_64-apple-darwin` | `macos-15-intel` |
| `aarch64-apple-darwin` | `macos-latest` |
| `x86_64-pc-windows-msvc` | `windows-latest` |

### Transition updater bridge

Canonical release archives are named `mitsuro-<target>.tar.gz` (or `.zip` on
Windows). During the announced command transition, release CI also publishes
`krusty-x86_64-pc-windows-msvc.zip` with an independently named checksum
manifest. The two ZIPs are intentionally not byte-identical: each updater
requires exactly one member with its expected filename. The canonical archive
contains only `mitsuro.exe`; the old archive contains only `krusty.exe`, whose
payload is byte-for-byte the full `mitsuro.exe` rather than the normal thin
command forwarder. This lets the old Windows single-binary updater install a
self-contained bridge binary without a canonical sibling. Homebrew, AUR,
documentation, and all new installs use the Mitsuro archive names.

Unix does not publish old archive aliases. Its prior self-update path already
fails closed and directs users to fetch the current `install.sh`; an old saved
installer would reject the canonical unit layout and cannot perform the offline
state-authority handoff safely. An upgrade originating from that deprecated
self-updater or shell-installer path must therefore download the current
canonical installer. Homebrew and AUR are separate package-manager paths and
must follow the explicit offline migration guidance below before first startup;
they do not execute `install.sh` implicitly.

The Windows transition archive alias and command forwarders may be retired only
after the announced support window has elapsed, the minimum installed version
is a bridge-aware release, updater compatibility tests no longer require the
old asset name, rollback and recovery instructions no longer depend on it, and
the release owner explicitly approves the removal. Removing an alias is a
release contract change, not routine naming cleanup.

Each job first builds the Expo web frontend from `apps/mobile` (so it can be embedded into the server and desktop bundles), then compiles Mitsuro in release mode for the target architecture. Unix builds are packaged as `.tar.gz` archives; the Windows build is packaged as a `.zip`.

**2. Desktop Linux archive.** A separate job tests and builds Native Canvas and
its Mitsuro server companion, then packages them with
`apps/desktop/canvas/packaging/package-linux-alpha.sh`. The output is a portable
Linux alpha archive plus checksum. Old GPUI Debian/RPM packages and the Tauri
shell are abandoned release targets.

**3. Create release.** Once both build stages complete, all artifacts are downloaded. CI requires the protected tag version to equal the `mitsuro` Cargo package version, verifies all five canonical platform archive checksum manifests and the Windows compatibility pair against their archives, renders `.github/homebrew/mitsuro.rb` with that version and the four Unix hashes, rejects remaining template tokens, and checks the generated formula with `ruby -c`. The publisher allowlists those six exact archive/manifest pairs, the exact version/commit-bound Canvas archive and checksum, and the formula. A GitHub Release is then created with auto-generated release notes. An existing protected-tag release is accepted only when every asset is byte-identical; CI never clobbers an asset at an immutable release URL.

The workflow fails before builds unless a tag push is protected, or a manual retry is dispatched from protected `main`, and the exact tag commit is contained in `main`. Every checkout and reusable quality gate is pinned to that authorized commit SHA, and the publish job rechecks the tag identity before creating assets. The repository also maintains an active `Protect release tags` ruleset. Do not push a `v*` tag merely to test the workflow; create one only for an approved, fully verified release.

## Distribution channels

### Install script

The fastest way to install Mitsuro is the one-liner:

```bash
curl -fsSL https://raw.githubusercontent.com/honeycomb-Technologies/Mitsuro/main/install.sh | sh
```

The script (`install.sh`) detects the host OS and architecture, fetches the latest release from the GitHub API, downloads the exact archive and its required SHA-256 manifest, verifies the manifest record and archive before extraction, and rejects unsafe archive paths or entry types. On Unix it stages an immutable release containing `mitsuro`, `mitsuro-hive`, and the service units, then switches one managed release pointer; a service reload/restart failure restores the prior binary and unit set. Existing direct and previous-identity installs are retained as immutable rollback releases, and releases are not pruned automatically. On default Linux installs the units are linked into `~/.config/systemd/user` and execute the selected `.mitsuro-current` binaries by exact path; custom `INSTALL_DIR` values deliberately skip automatic unit management. On Windows the installer requires the canonical one-entry `mitsuro.exe` archive, stages both command names before publication, and rolls the pair back together if either publication or verification fails. The script supports Linux (x86_64, aarch64), macOS (Intel and Apple Silicon), and Windows under MSYS/Cygwin. If the install directory is not already in `PATH`, it prints the line you need to add to your shell config.

The Windows direct-install path is deliberately binary-only: it does not move,
copy, or start state. If the previous state root exists, stop every Mitsuro CLI,
TUI, desktop, server, and Hive process from both generations and run
`mitsuro migrate-identity --confirm-offline` before the first normal startup.
The migration command holds the SQLite writer fence, creates and validates the
canonical snapshot, atomically publishes the new root, and preserves the old
root as rollback authority. Do not start either generation between installing
the binaries and completing that command.

On Linux, the shell installer performs that same explicit offline migration
only after it quiesces the supervised services and proves through same-user
procfs inspection
that no CLI, TUI, desktop, server, or Hive process from either generation is
still running and that no same-user process holds either the previous or
canonical database, WAL, or SHM file. Its preflight
records the source database and WAL digests without checkpointing either file;
the canonical migration binary owns the writer fence and verified online
backup. The installer accepts only the regular, at-most-16-KiB
`.identity-migration-v2` receipt with its exact five ordered LF-terminated
fields, exact previous-root path, bounded timestamps/sizes, lowercase SHA-256
authority fingerprints, and internally consistent WAL tuple. Installer
rollback quarantines the failed canonical root and reselects
the unchanged old root. After restart, activation requires the exact selected
executables in `/proc`, an authenticated Hive ping, and the server PID-bound
`/health` response, not merely `systemctl is-active`. Exercise the
offline installer fixture after changing installer logic:

```bash
sh install.sh --self-test
```

The preserved previous state root and previous release directories are
**recovery-only** after cutover. The installer redirects normal command aliases
and managed units, but no continuous lock can prevent the same user from
directly executing an archived binary or previous-generation desktop app.
Never launch one except as part of a coordinated rollback, even if canonical
Mitsuro appears stopped: an uncoordinated launch can create split authority or
mutate the preserved source so the canonical restart check fails closed. A
deliberate rollback must stop and prove quiescence of every canonical CLI, TUI,
desktop, server, and Hive process before selecting the previous release and
state together. Do not substitute an ad-hoc recursive `chmod`; a safe freeze
would require a versioned cross-platform permission, ACL, and xattr manifest
plus an atomic validated thaw transaction.

macOS has no Linux-compatible procfs authority proof, so the shell installer
fails closed instead of attempting an automatic state cutover. It retains the
verified staged release and prints that release's exact `mitsuro
migrate-identity --confirm-offline` command. Stop every process from both
generations, run the printed physical command, and then rerun the installer.
Do not start Mitsuro between those steps. Windows publishes the command pair
first and then requires the same explicit offline command through the installed
`mitsuro.exe`; it never migrates state during binary installation.

You can pin a specific version by setting `VERSION` before running the script:

```bash
curl -fsSL ... | VERSION=v0.8.2 sh
```

## Self-hosted systemd service

The checked-in portable user services run the HTTP control plane and the independent
Hive autonomous backend from the exact shell-installer authority at
`~/.local/bin/.mitsuro-current`; their `PATH` is available only to child tools
and never selects the service executable. The HTTP server requires
Hive's private socket and fails closed if it cannot complete an authenticated
daemon handshake. Use the current shell installer to publish that immutable
release pointer before installing or restarting the units:

```bash
install -d -m755 ~/.config/systemd/user
install -m644 deploy/systemd/mitsuro-hive.socket deploy/systemd/mitsuro-hive.service \
  deploy/systemd/mitsuro-serve.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now mitsuro-hive.socket mitsuro-serve.service
systemctl --user is-active mitsuro-hive.socket mitsuro-hive.service mitsuro-serve.service
curl --fail http://127.0.0.1:3000/health
```

`mitsuro-hive.socket` starts the daemon on demand. Do not run a second manual
daemon against the same database and socket. For deployment verification,
confirm the three tracked unit states, an authenticated Hive diagnostics/API
request through the server, and a restart-recovery test; a successful Cargo
build alone is not proof that autonomous work is live.

Hive-bearing release archives include these user units. The shell installer
links them into `~/.config/systemd/user`, and the future Hive-bearing AUR package
renders the service `ExecStart` values to exact `/usr/bin/mitsuro*` paths before
placing them in `/usr/lib/systemd/user`. Homebrew's service uses its exact
`opt_bin` path. The default hardening grants writes
under `~/Work` and
Mitsuro's state/cache directories; add a user-service drop-in with an additional
`ReadWritePaths=` entry when autonomous sessions use another project root.
Homebrew exposes `mitsuro-hive` through `brew services`; the HTTP server remains
an explicitly configured self-host service.

Hive resolves `.mitsuro/skills` independently for each run's frozen project
root. Autonomous daemon runs intentionally do not load project `.mcp.json`
servers yet, and MCP connections made through the HTTP `/mcp` API do not cross
the process boundary into the daemon. This is fail-closed: project MCP will be
enabled only with durable project/config trust and explicit MCP process
lifecycle ownership, rather than accidentally exposing the daemon launch
directory's tools to every project.

### Homebrew tap

macOS and Linux users with Homebrew can install from the tap:

```bash
brew install BurgessTG/tap/mitsuro
```

The formula template (`.github/homebrew/mitsuro.rb`) selects the correct binary archive based on CPU architecture. It supports macOS ARM64, macOS x86_64, Linux ARM64, and Linux x86_64. Release CI deterministically renders the version, URLs, and SHA-256 checksums from the protected tag and its four verified Unix archives, then attaches the fully rendered `mitsuro.rb` formula to the GitHub Release.

The binary/state migration above does not prove macOS desktop WebKit
continuity. Wry does not expose the authoritative WKWebView data directory, so
Mitsuro deliberately does not copy a guessed Application Support namespace.
Before retiring previous desktop data, validate cookies, localStorage-backed
settings, connection state, authentication, and preferences in a signed macOS
build. Keep the old namespace as recovery evidence until that manual gate
passes.

Homebrew installs binaries and does not run the shell installer's state handoff.
If the previous state root exists, keep every old and canonical CLI, TUI,
desktop, server, and Hive process stopped and run `mitsuro migrate-identity
--confirm-offline` before the first `mitsuro` startup. A failed or missing
receipt is not permission to start one generation temporarily.

This repository does not automatically write to `BurgessTG/homebrew-tap`, so the public tap can trail the latest GitHub Release until a maintainer publishes the generated formula as `Formula/mitsuro.rb`. That publication is a separate reviewed step. Any future cross-repository automation must be opted into with an explicitly configured token scoped to the tap; the release workflow does not require or assume that secret.

### AUR recipe

The repository contains an explicitly unreleased AUR template, and `mitsuro` is
not currently published in the AUR. No existing tag contains the canonical
crate/service layout, so `aur/PKGBUILD` deliberately has `pkgver=UNRELEASED`
with no source URL or checksum and fails before build preparation. This avoids
presenting an old tag as if it could build the renamed source. Once a new stable
tag actually exists, `aur/update-release.sh` validates the immutable archive's
canonical CLI, Hive, transition shims, and service files before writing the
version, URL, checksum, and generated `.SRCINFO`.

The generated recipe builds and installs both Mitsuro and Hive plus their
systemd user units. It verifies the pinned source archive, builds with Cargo's
stable toolchain, runs the test suite during `check()`, and installs the
license. It supports `x86_64` and `aarch64`; runtime dependencies are
`gcc-libs` and `openssl`, and the build dependency is `cargo`.

The AUR package also installs binaries and unit files without migrating user
state. When previous state exists, leave the new units stopped, quiesce every
process from both generations, and run `mitsuro migrate-identity
--confirm-offline` before enabling or starting `mitsuro-hive.socket` or
`mitsuro-serve.service`.

GitHub does not publish the versioned source archive until its tag exists, so
AUR metadata is updated after the protected release tag is published. Run:

```bash
bash aur/check-package-template.sh
./aur/update-release.sh <released-version>
cd aur
makepkg --verifysource -f
makepkg --printsrcinfo | diff -u .SRCINFO -
```

The updater accepts stable versions only, downloads over HTTPS, validates every
archive entry stays beneath the expected top-level directory, verifies the
source's `mitsuro` Cargo version matches the requested tag, computes SHA-256
locally, and updates both `PKGBUILD` and `.SRCINFO`. Review and publish those two
files to the AUR repository. Never replace the checksum with `SKIP`; if a
release tag or its archive changes, checksum verification must fail until a
maintainer explicitly reviews and updates the package metadata.

## Cross-compilation

The `Cross.toml` file configures the `cross` tool for building on architectures different from the host. Currently it defines a single target:

```toml
[target.aarch64-unknown-linux-gnu]
pre-build = [
    "dpkg --add-architecture arm64",
    "apt-get update && apt-get install -y libudev-dev:arm64",
]
```

This ensures the ARM64 udev headers are available inside the cross-compilation container. The `mitsuro-core` crate vendors both libgit2 and OpenSSL (via the `vendored-libgit2` and `vendored-openssl` features on the `git2` dependency) specifically to make cross-compilation reliable -- these libraries are compiled from source rather than linked against system copies that may not exist for the target architecture.

In the release workflow, the ARM64 Linux build runs on a native `ubuntu-24.04-arm` GitHub runner rather than using cross-compilation, which avoids the container overhead entirely.

## Mobile builds

The mobile app lives in `apps/mobile` and is built with Expo and EAS (Expo Application Services). The `eas.json` file defines three build profiles:

- **development** -- Builds a development client with internal distribution. On iOS, this targets the simulator.
- **preview** -- Builds for internal distribution (ad-hoc provisioning), used for testing on physical devices before a release.
- **production** -- Builds for App Store distribution with auto-incrementing iOS build numbers.

### TestFlight deployment

The `mobile-testflight.yml` workflow automates iOS builds and TestFlight submission. It starts when a GitHub Release is published and can also be triggered manually with `workflow_dispatch`. A manual release retry should select the exact protected release tag, such as `v0.10.0`, so every checkout and the submitted build remain bound to the released commit rather than a later `main` tip.

Manual dispatch has no path-filter shortcut: use it only for an intentional TestFlight build after reviewing the selected tag and passing the protected `testflight` environment approval. A GitHub Release created by another workflow may not recursively start this workflow, so verify that a run actually exists and dispatch the exact tag when it does not.

The job targets the GitHub Actions `testflight` environment (`environment: testflight`). Keep that environment configured with human approval/reviewer protection so EAS build and App Store Connect secrets are not exposed and the build is not submitted until the approval gate is satisfied.

The workflow uses concurrency control (`group: mobile-ios-build, cancel-in-progress: true`) so that a newer TestFlight run cancels any in-flight build rather than queueing up stale builds. It first calls the same reusable client-quality workflow used by CI and releases. The TestFlight build job cannot start until the API contract tests, shared-state type-check/tests, mobile TypeScript check, and Expo web export pass. The build and submission steps are:

1. Check out the repo and install Bun
2. Install dependencies with `bun install --frozen-lockfile`
3. Set up EAS with the `expo/expo-github-action`
4. Run `eas build --platform ios --profile production --non-interactive`
5. Submit the resulting build to TestFlight with `eas submit`

The App Store Connect app ID (`6761496828`) is configured in `eas.json`. Apple credentials are stored as GitHub secrets.

For a visible native or rendering change, a successful EAS build and upload are not the release gate. Wait for App Store Connect processing and tester availability, install the exact marketing version and build number from TestFlight on a physical phone, then exercise cold launch, background/foreground, rapid Chat/Code/Hive switching, keyboard focus, and the changed interaction. Skia FAB changes additionally require repeated first-open, open/close, swipe-close, and every pill path while checking for aborts, blank or stuck glass, lost touches, hangs, and unbounded memory growth. Record the source commit, EAS build and submission IDs, App Store version/build, tester group, installation, and device result separately.

For safe no-TestFlight validation of docs/tooling changes, inspect the workflow definition and repository metadata only (for example, `gh workflow view mobile-testflight.yml --repo honeycomb-Technologies/Mitsuro`, `scripts/check-default-branch-preflight.sh`, and `git diff --check`). Do not validate this path by pushing mobile changes to `main`, running `gh workflow run mobile-testflight.yml`, or submitting a build unless a release attempt has been explicitly approved.

## Desktop builds

Native Canvas (`apps/desktop/canvas`) is the sole desktop target. It uses the shared
Mitsuro server and ships with a companion binary in its Linux alpha archive.

```bash
cargo test --locked -p mitsuro-canvas
cargo build --release --locked -p mitsuro-canvas -p mitsuro
apps/desktop/canvas/packaging/package-linux-alpha.sh \
  --canvas-bin "$PWD/target/release/mitsuro-canvas" \
  --server-bin "$PWD/target/release/mitsuro" \
  --output-dir "$PWD/artifacts/canvas"
```

Packaging requires a clean source commit and verifies exact artifact provenance.
The archive does not install or replace an application. Full native GUI acceptance
is still a separate release requirement. See [Canvas packaging](../../apps/desktop/canvas/packaging/README.md)
and the [desktop transition contract](../frontends/desktop-transition.md).

## The embedded web frontend

The self-hosted server has a notable trick: the web frontend is compiled directly into the Rust binary at build time using the `rust-embed` crate. In `mitsuro-server/src/lib.rs`, the `WebAssets` struct is annotated with `#[derive(Embed)]` and pointed at `apps/mobile/dist`:

```rust
#[derive(Embed)]
#[folder = "../../apps/mobile/dist"]
#[prefix = ""]
#[allow_missing = true]
struct WebAssets;
```

The `allow_missing = true` attribute is key -- if the `dist` directory does not exist at compile time (for example, during a plain `cargo build` without running the frontend build first), the server compiles successfully but runs in API-only mode. When a browser hits the server, it receives a plain-text message instead of the web app.

For a full build (as done in the release workflow), the process is:

1. Run `bun install --frozen-lockfile` and `npx expo export --platform web` in `apps/mobile` to produce the static export in `apps/mobile/dist`.
2. Run `cargo build --release` -- rust-embed picks up the files and bakes them into the binary.

The server handles all non-API routes with SPA fallback: it tries to match the request path to an embedded file, and if nothing matches, serves `index.html` so client-side routing works. Static assets under `_expo/static/` get immutable cache headers (one year), while HTML files are served with `no-cache`.

## Development workflow

During development, you typically want the backend and frontend running simultaneously with hot reload.

### CLI-only development

If you are working on the Rust CLI or core library:

```bash
cargo run              # Runs mitsuro-cli (the default workspace member)
cargo run --release    # Release mode for performance testing
cargo test --workspace # Run all tests across all crates
```

### Server + web frontend

To work on the web frontend with live reload:

1. **Start the Expo dev server** in one terminal:
   ```bash
   cd apps/mobile && bun start --web
   ```

2. **Start the Rust server** in another terminal:
   ```bash
   cargo run -p mitsuro-server
   ```

The frontend dev server handles hot module replacement. The Rust server serves the API.

### Desktop development

```bash
cargo run -p mitsuro-canvas
```

Native Canvas owns desktop interaction and presentation. Expo in `apps/mobile`
owns mobile and web. Use the [isolated Canvas smoke](../../apps/desktop/canvas/acceptance/README.md)
when validation must not attach to the shared server. The earlier desktop GPUI,
Tauri, and desktop Expo applications are abandoned.

### Mobile development

For the iOS/Android app:

```bash
cd apps/mobile
bun start              # Start Expo dev server
bun run ios            # Build and run on iOS simulator
bun run android        # Build and run on Android emulator
```

The mobile app connects to a running Mitsuro server instance (either local or remote) for its backend.
