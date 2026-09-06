#!/bin/sh
# Fail when active interface versions drift from cargo/mitsuro.
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cli_toml="$root/crates/mitsuro-cli/Cargo.toml"
app_json="$root/apps/mobile/app.json"
mobile_pkg="$root/apps/mobile/package.json"

cli_version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$cli_toml" | head -n 1)
app_version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["expo"]["version"])' "$app_json")
pkg_version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "$mobile_pkg")

status=0
if [ -z "$cli_version" ]; then
  echo "Could not read crates/mitsuro-cli version" >&2
  exit 1
fi
if [ "$app_version" != "$cli_version" ]; then
  echo "apps/mobile/app.json expo.version ($app_version) != cargo/mitsuro ($cli_version)" >&2
  echo "Run: sh scripts/sync-product-version.sh" >&2
  status=1
fi
if [ "$pkg_version" != "$cli_version" ]; then
  echo "apps/mobile/package.json version ($pkg_version) != cargo/mitsuro ($cli_version)" >&2
  echo "Run: sh scripts/sync-product-version.sh" >&2
  status=1
fi
canvas_version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$root/apps/desktop/canvas/Cargo.toml" | head -n 1)
if [ "$canvas_version" != "$cli_version" ]; then
  echo "Canvas version ($canvas_version) != cargo/mitsuro ($cli_version); check the Sampo fixed group" >&2
  status=1
fi
if ! grep -Fq "<release version=\"$cli_version\"" "$root/apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml"; then
  echo "Canvas AppStream version is missing; run sh scripts/sync-product-version.sh" >&2
  status=1
fi
if [ "$status" -eq 0 ]; then
  echo "Product marketing version is $cli_version"
fi
exit "$status"
