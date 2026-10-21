#!/bin/sh
# Keep Cargo.lock third-party pins stable across Sampo version bumps.
#
# `sampo release` / `cargo update` may rewrite the lockfile and jump crates
# such as rmcp 1.3 -> 1.8. Restore pins from a git ref (usually origin/main),
# then rewrite only workspace package versions so they match Cargo.toml.
#
# Usage:
#   sh scripts/refresh-workspace-lock-versions.sh [base-ref]
#   sh scripts/refresh-workspace-lock-versions.sh --check
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
cd "$root"

check=0
base_ref=""
for arg in "$@"; do
  case "$arg" in
    --check) check=1 ;;
    --) ;;
    -*)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
    *)
      if [ -n "$base_ref" ]; then
        echo "Unexpected argument: $arg" >&2
        exit 2
      fi
      base_ref=$arg
      ;;
  esac
done

if [ "$check" -eq 1 ] && [ -n "$base_ref" ]; then
  echo "Use either --check or a base ref, not both" >&2
  exit 2
fi

if [ -n "$base_ref" ]; then
  git checkout "$base_ref" -- Cargo.lock
fi

python3 - "$root" "$check" <<'PY'
import pathlib
import re
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
check = sys.argv[2] == "1"
lock_path = root / "Cargo.lock"

workspace_versions: dict[str, str] = {}
manifests = [
    *sorted((root / "crates").glob("*/Cargo.toml")),
    root / "apps" / "desktop" / "canvas" / "Cargo.toml",
]
for manifest in manifests:
    data = tomllib.loads(manifest.read_text())
    package = data.get("package") or {}
    name = package.get("name")
    version = package.get("version")
    if name and version:
        workspace_versions[name] = version

text = lock_path.read_text()
blocks = re.split(r"(?=\n\[\[package\]\])", text)
changed = False
mismatches: list[str] = []

package_re = re.compile(
    r'^\[\[package\]\]\nname = "([^"]+)"\nversion = "([^"]+)"',
    re.M,
)

updated: list[str] = []
new_blocks: list[str] = []
for block in blocks:
    match = package_re.search(block)
    if not match:
        new_blocks.append(block)
        continue
    name, version = match.group(1), match.group(2)
    expected = workspace_versions.get(name)
    if expected is None or "\nsource = " in block:
        new_blocks.append(block)
        continue
    if version == expected:
        new_blocks.append(block)
        continue
    mismatches.append(f"{name} lock={version} toml={expected}")
    new_blocks.append(block.replace(f'version = "{version}"', f'version = "{expected}"', 1))
    updated.append(f"{name} {version} -> {expected}")
    changed = True

if check:
    if mismatches:
        print("Cargo.lock workspace versions do not match Cargo.toml:", file=sys.stderr)
        for item in mismatches:
            print(f"  {item}", file=sys.stderr)
        print("Run: sh scripts/refresh-workspace-lock-versions.sh origin/main", file=sys.stderr)
        sys.exit(1)
    print(f"Workspace lock versions match {len(workspace_versions)} Cargo.toml packages")
    sys.exit(0)

if changed:
    lock_path.write_text("".join(new_blocks))
    for item in updated:
        print(f"Updated Cargo.lock {item}")
else:
    print("Cargo.lock workspace versions already match Cargo.toml")
PY
