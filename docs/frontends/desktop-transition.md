# Desktop transition to Native Canvas

## Active interfaces

- Desktop: Native Canvas, `apps/desktop/canvas`, executable `mitsuro-canvas`.
- Mobile and web: Expo, `apps/mobile`.
- Terminal: CLI/TUI, `crates/mitsuro-cli`, executable `mitsuro`.
- ACP: retained editor protocol integration into the shared Agent runtime.

## Abandoned development targets

The earlier `apps/desktop/gpui` application, `apps/desktop/shell` Tauri host,
`apps/desktop/ui` Expo desktop UI, and `crates/mitsuro-desktop-backend` are abandoned.
They receive no new features and are excluded from active desktop build, version,
and release targets. Retained files are historical and compatibility references.
The old build/acceptance script entry points stop with a Canvas redirect.

Canvas still uses the GPUI framework. This retirement concerns the old application,
not the framework or the mobile/web Expo application.

## Installed clients and stored state

Source retirement does not uninstall an application, rewrite user data, replace a
service, or remove tested legacy-name compatibility readers. Canvas has its own
`mitsuro-canvas` executable and `io.mitsuro.canvas` desktop identity. The Linux alpha
archive is extracted separately; it does not overwrite the old `mitsuro-desktop`
launcher, Debian/RPM package, or Tauri installation.

Existing clients remain on their installed version. There is no automatic desktop
updater cutover or import of old client-local preferences in this change. Shared
sessions remain on the server; Canvas connects through current capability and
ownership contracts. Do not silently substitute a different backend or claim
Codex app-server parity through the abandoned adapter.

## Releases, mixed versions, and rollback

Future approved desktop releases target the Canvas Linux alpha archive and its
checksum. Old desktop Debian/RPM artifacts stop being built; immutable published
artifacts and tags remain available. CLI/Hive archives and their existing updater
compatibility contracts are unchanged. The new release workflow rejects tags that
predate Canvas before building; recovery of such a historical cut uses its original
release tooling under separate approval, never replacement assets at an old URL.

Before distribution, run the Canvas GUI acceptance work, verify the exact packaged
binaries, and test connection/recovery against the intended server. A mixed-version
client must honor advertised server capabilities and report unsupported operations.
A packaging success does not establish GUI readiness.

Rollback uses the previous immutable release or a preserved source checkpoint.
Keep old installed artifacts and user data until the operator explicitly approves
removal. Server deployment and database rollback are separate governed operations;
this source transition does not perform either.
