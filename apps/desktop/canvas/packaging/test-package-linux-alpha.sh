#!/usr/bin/env bash

set -euo pipefail
umask 022
export LC_ALL=C

script_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
test_parent="$repo_root/target/packaging-tests"
mkdir -p "$test_parent"
test_tmp="$(mktemp -d "$test_parent/mitsuro-canvas-packaging-test.XXXXXXXX")"

cleanup() {
    case "$test_tmp" in
        "$test_parent"/mitsuro-canvas-packaging-test.*) rm -rf -- "$test_tmp" ;;
        *) printf 'refusing to clean unexpected test path: %s\n' "$test_tmp" >&2 ;;
    esac
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

archive_root="mitsuro-canvas-synthetic"
stage="$test_tmp/$archive_root"
install -d -m 0755 \
    "$stage/usr/bin" \
    "$stage/usr/libexec/mitsuro" \
    "$stage/usr/share/applications" \
    "$stage/usr/share/metainfo" \
    "$stage/usr/share/icons/hicolor/scalable/apps" \
    "$stage/usr/share/licenses/mitsuro-canvas" \
    "$stage/usr/share/mitsuro-canvas/assets/themes/test" \
    "$stage/usr/share/doc/mitsuro-canvas"
install -m 0755 /bin/true "$stage/usr/bin/mitsuro-canvas"
install -m 0755 /bin/true "$stage/usr/libexec/mitsuro/mitsuro"
install -m 0644 "$script_dir/io.mitsuro.canvas.desktop" "$stage/usr/share/applications/io.mitsuro.canvas.desktop"
install -m 0644 "$script_dir/io.mitsuro.canvas.metainfo.xml" "$stage/usr/share/metainfo/io.mitsuro.canvas.metainfo.xml"
install -m 0644 "$repo_root/assets/branding/mitsuro/mitsuro-app-icon-master.svg" \
    "$stage/usr/share/icons/hicolor/scalable/apps/io.mitsuro.canvas.svg"
install -m 0644 "$repo_root/LICENSE" "$stage/usr/share/licenses/mitsuro-canvas/LICENSE"
install -m 0644 "$script_dir/README.md" "$stage/usr/share/doc/mitsuro-canvas/README.md"
install -m 0644 "$repo_root/apps/desktop/canvas/assets/themes/quiet-ink/theme.toml" \
    "$stage/usr/share/mitsuro-canvas/assets/themes/test/theme.toml"

canvas_sha256="$(sha256sum "$stage/usr/bin/mitsuro-canvas" | awk '{print $1}')"
server_sha256="$(sha256sum "$stage/usr/libexec/mitsuro/mitsuro" | awk '{print $1}')"
jq -n '{
    format: "mitsuro-canvas-linux-alpha-v1",
    app_id: "io.mitsuro.canvas",
    package: "mitsuro-canvas",
    version: "0.0.0-test",
    architecture: "test",
    source: {commit: "0000000000000000000000000000000000000000", clean: true, source_date_epoch: 0},
    binaries: [
        {path: "usr/bin/mitsuro-canvas", sha256: $canvas_sha256},
        {path: "usr/libexec/mitsuro/mitsuro", sha256: $server_sha256}
    ]
}' \
    --arg canvas_sha256 "$canvas_sha256" \
    --arg server_sha256 "$server_sha256" \
    >"$stage/usr/share/doc/mitsuro-canvas/PACKAGE-MANIFEST.json"
(
    cd "$stage"
    find usr -type f ! -path 'usr/share/doc/mitsuro-canvas/SHA256SUMS' -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 sha256sum \
        >usr/share/doc/mitsuro-canvas/SHA256SUMS
)

"$script_dir/package-linux-alpha.sh" --check-metadata
"$script_dir/package-linux-alpha.sh" --validate-staging "$stage"

find "$stage" -exec touch -h -d '@0' {} +
for copy in one two; do
    tar \
        --format=gnu \
        --sort=name \
        --mtime='@0' \
        --owner=0 \
        --group=0 \
        --numeric-owner \
        -C "$test_tmp" \
        -cf - "$archive_root" \
        | gzip -n >"$test_tmp/$copy.tar.gz"
done
cmp --silent "$test_tmp/one.tar.gz" "$test_tmp/two.tar.gz" \
    || { printf 'repeated synthetic archives are not byte-identical\n' >&2; exit 1; }
mkdir "$test_tmp/extracted"
tar -xzf "$test_tmp/one.tar.gz" -C "$test_tmp/extracted"
"$script_dir/package-linux-alpha.sh" --validate-staging "$test_tmp/extracted/$archive_root"

printf '\n# checksum tamper probe\n' >>"$stage/usr/share/applications/io.mitsuro.canvas.desktop"
if "$script_dir/package-linux-alpha.sh" --validate-staging "$stage" >"$test_tmp/tamper.log" 2>&1; then
    printf 'tampered staging tree unexpectedly passed validation\n' >&2
    exit 1
fi

printf 'Linux alpha packaging synthetic test passed.\n'
