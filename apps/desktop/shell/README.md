# Desktop Shell (Tauri) — ABANDONED

Abandoned historical implementation. Do not develop, build, or release this target. Use Native Canvas (`apps/desktop/canvas`); the remaining text describes the former implementation.

## Dev Flow

1. From this folder: `bun install` then `bun run dev`.
2. Tauri runs the desktop UI web surface (see `tauri.conf.json` `beforeDevCommand` / `devUrl`).

## Linux Build Flow

1. Install dependencies:
   - Bun 1.3+
   - Rust stable
   - GTK/WebKit build dependencies for your distro (`webkit2gtk`, `gtk3`, `libayatana-appindicator`, `patchelf`)
2. Build packages:
   - `bun install`
   - `bun run build`

Build outputs:

- `src-tauri/target/release/bundle/deb/*.deb`
- `src-tauri/target/release/bundle/rpm/*.rpm`

Package names follow Tauri `productName` (**Mitsuro**, version from `tauri.conf.json`).

## Linux Install + Run

After install, launch the Mitsuro desktop app from your desktop environment or the
binary name produced by the bundle (typically `mitsuro-desktop` / product name Mitsuro).

If your Wayland compositor has dmabuf issues, force X11 fallback:

```bash
GDK_BACKEND=x11 WEBKIT_DISABLE_DMABUF_RENDERER=1 mitsuro-desktop
```

## Bundle Notes

- `bun run build` / `bun run build:linux` creates Linux `.deb` and `.rpm`.
- `bun run build:all` attempts all bundle formats.
- `bun run build:appimage` builds AppImage only and requires `linuxdeploy`.

## Identity

- Canonical desktop id: `io.mitsuro.desktop`
- Legacy web-data ids are handled only by the desktop identity compatibility path
  (`io.krusty.desktop` / early `dev.krusty.desktop`) for offline migration — not product branding.
