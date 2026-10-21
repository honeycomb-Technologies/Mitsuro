#!/bin/sh
# Sync Expo marketing and Canvas AppStream metadata to the product version.
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cli_toml="$root/crates/mitsuro-cli/Cargo.toml"
app_json="$root/apps/mobile/app.json"
mobile_pkg="$root/apps/mobile/package.json"

version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' "$cli_toml" | head -n 1)
if [ -z "$version" ]; then
  echo "Could not read version from $cli_toml" >&2
  exit 1
fi

python3 - "$version" "$app_json" "$mobile_pkg" "$root/apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml" <<'PY'
import json
import datetime
import sys
from pathlib import Path

version, app_path, pkg_path = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])

app = json.loads(app_path.read_text())
app.setdefault("expo", {})["version"] = version
app_path.write_text(json.dumps(app, indent=2) + "\n")

pkg = json.loads(pkg_path.read_text())
pkg["version"] = version
pkg_path.write_text(json.dumps(pkg, indent=2) + "\n")
metadata = Path(sys.argv[4])
text = metadata.read_text()
if f'<release version="{version}"' not in text:
    date = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    entry = (f'\n    <release version="{version}" date="{date}" type="development">\n'
             '      <description><p>Mitsuro Native Canvas desktop update.</p></description>\n'
             '    </release>')
    if '<releases>' not in text:
        raise SystemExit("Canvas AppStream releases element is missing")
    metadata.write_text(text.replace('<releases>', '<releases>' + entry, 1))
print(f"Synced Expo and Canvas metadata to {version}")
PY
