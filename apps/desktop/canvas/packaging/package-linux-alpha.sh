#!/usr/bin/env bash

set -euo pipefail
umask 022
export LC_ALL=C

readonly APP_ID="io.mitsuro.canvas"
readonly PACKAGE_NAME="mitsuro-canvas"
readonly FORMAT_VERSION="mitsuro-canvas-linux-alpha-v1"

script_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel)"
canvas_bin=""
server_bin=""
output_dir=""
validate_root=""
metadata_only=false
package_tmp=""

usage() {
    cat <<'USAGE'
Usage:
  package-linux-alpha.sh --canvas-bin ABS --server-bin ABS --output-dir ABS
  package-linux-alpha.sh --validate-staging ABS
  package-linux-alpha.sh --check-metadata

Creates a deterministic, non-installing Linux alpha archive. The binary inputs
must be the exact target/release outputs for this checkout. The command never
installs files, starts a process, or changes a service.
USAGE
}

die() {
    printf 'package-linux-alpha: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    if [[ -n "$package_tmp" && -d "$package_tmp" ]]; then
        case "$package_tmp" in
            /tmp/mitsuro-canvas-package.*|"${output_dir}"/.mitsuro-canvas-package.*)
                rm -rf -- "$package_tmp"
                ;;
            *)
                printf 'package-linux-alpha: refusing to clean unexpected path: %s\n' "$package_tmp" >&2
                ;;
        esac
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

require_absolute_file() {
    local label="$1"
    local path="$2"
    [[ "$path" == /* ]] || die "$label must be an absolute path: $path"
    [[ -f "$path" ]] || die "$label is not a regular file: $path"
    [[ -x "$path" ]] || die "$label is not executable: $path"
}

elf_identity() {
    local path="$1"
    local class machine
    class="$(readelf -h -- "$path" | awk -F: '/^[[:space:]]*Class:/{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit}')"
    machine="$(readelf -h -- "$path" | awk -F: '/^[[:space:]]*Machine:/{sub(/^[[:space:]]+/, "", $2); print $2; exit}')"
    [[ -n "$class" && -n "$machine" ]] || die "not a readable ELF binary: $path"
    printf '%s|%s\n' "$class" "$machine"
}

package_architecture() {
    case "$1" in
        *"X86-64"*) printf 'x86_64\n' ;;
        *"AArch64"*) printf 'aarch64\n' ;;
        *) die "unsupported Linux ELF architecture: $1" ;;
    esac
}

validate_metadata() {
    local desktop="$script_dir/$APP_ID.desktop"
    local metainfo="$script_dir/$APP_ID.metainfo.xml"
    local icon="$repo_root/assets/branding/mitsuro/mitsuro-app-icon-master.svg"
    local license="$repo_root/LICENSE"

    [[ -f "$desktop" ]] || die "desktop metadata is missing: $desktop"
    [[ -f "$metainfo" ]] || die "AppStream metadata is missing: $metainfo"
    [[ -f "$icon" ]] || die "canonical Mitsuro icon is missing: $icon"
    [[ -f "$license" ]] || die "repository license is missing: $license"
    grep -Fqx "Exec=$PACKAGE_NAME" "$desktop" || die "desktop Exec must be $PACKAGE_NAME"
    grep -Fqx "TryExec=$PACKAGE_NAME" "$desktop" || die "desktop TryExec must be $PACKAGE_NAME"
    grep -Fqx "Icon=$APP_ID" "$desktop" || die "desktop Icon must be $APP_ID"
    grep -Fq "<id>$APP_ID</id>" "$metainfo" || die "AppStream id must be $APP_ID"
    grep -Fq "<binary>$PACKAGE_NAME</binary>" "$metainfo" || die "AppStream binary is inconsistent"

    if command -v desktop-file-validate >/dev/null 2>&1; then
        desktop-file-validate "$desktop"
    fi
    if command -v appstreamcli >/dev/null 2>&1; then
        appstreamcli validate --no-net "$metainfo"
    elif command -v xmllint >/dev/null 2>&1; then
        xmllint --noout "$metainfo"
    fi
}

validate_staging() {
    local stage="$1"
    local canvas="$stage/usr/bin/mitsuro-canvas"
    local server="$stage/usr/libexec/mitsuro/mitsuro"
    local desktop="$stage/usr/share/applications/$APP_ID.desktop"
    local metainfo="$stage/usr/share/metainfo/$APP_ID.metainfo.xml"
    local icon="$stage/usr/share/icons/hicolor/scalable/apps/$APP_ID.svg"
    local license="$stage/usr/share/licenses/mitsuro-canvas/LICENSE"
    local readme="$stage/usr/share/doc/mitsuro-canvas/README.md"
    local manifest="$stage/usr/share/doc/mitsuro-canvas/PACKAGE-MANIFEST.json"
    local sums="$stage/usr/share/doc/mitsuro-canvas/SHA256SUMS"
    local manifest_canvas_sha256 manifest_server_sha256

    [[ "$stage" == /* ]] || die "staging root must be absolute: $stage"
    [[ -d "$stage/usr" ]] || die "staging root has no usr directory: $stage"
    [[ -x "$canvas" ]] || die "staged Canvas binary is missing or not executable"
    [[ -x "$server" ]] || die "staged companion is missing or not executable"
    [[ -f "$desktop" ]] || die "staged desktop file is missing"
    [[ -f "$metainfo" ]] || die "staged AppStream metadata is missing"
    [[ -f "$icon" ]] || die "staged scalable icon is missing"
    [[ -f "$license" ]] || die "staged license is missing"
    [[ -f "$readme" ]] || die "staged package README is missing"
    [[ -f "$manifest" ]] || die "staged package manifest is missing"
    [[ -f "$sums" ]] || die "staged checksums are missing"
    find "$stage" -type l -print -quit | grep -q . && die "staging tree must not contain symbolic links"
    [[ "$(elf_identity "$canvas")" == "$(elf_identity "$server")" ]] || die "Canvas and companion ELF architectures differ"
    grep -Fqx "Exec=$PACKAGE_NAME" "$desktop" || die "staged desktop Exec is inconsistent"
    grep -Fq "<id>$APP_ID</id>" "$metainfo" || die "staged AppStream id is inconsistent"
    find "$stage/usr/share/mitsuro-canvas/assets/themes" -type f -name theme.toml -print -quit | grep -q . \
        || die "staged Canvas theme assets are missing"
    jq -e \
        --arg format "$FORMAT_VERSION" \
        --arg app_id "$APP_ID" \
        '.format == $format
            and .app_id == $app_id
            and .source.clean == true
            and (.source.commit | test("^[0-9a-f]{40}$"))
            and (.source.source_date_epoch | type == "number")
            and ([.binaries[].path] | sort == ["usr/bin/mitsuro-canvas", "usr/libexec/mitsuro/mitsuro"])' \
        "$manifest" >/dev/null || die "staged package manifest is invalid"
    manifest_canvas_sha256="$(jq -er '.binaries[] | select(.path == "usr/bin/mitsuro-canvas") | .sha256' "$manifest")"
    manifest_server_sha256="$(jq -er '.binaries[] | select(.path == "usr/libexec/mitsuro/mitsuro") | .sha256' "$manifest")"
    [[ "$manifest_canvas_sha256" == "$(sha256sum "$canvas" | awk '{print $1}')" ]] \
        || die "manifest Canvas SHA-256 does not match the staged binary"
    [[ "$manifest_server_sha256" == "$(sha256sum "$server" | awk '{print $1}')" ]] \
        || die "manifest server SHA-256 does not match the staged companion"

    if command -v desktop-file-validate >/dev/null 2>&1; then
        desktop-file-validate "$desktop"
    fi
    if command -v appstreamcli >/dev/null 2>&1; then
        appstreamcli validate --no-net "$metainfo"
    elif command -v xmllint >/dev/null 2>&1; then
        xmllint --noout "$metainfo"
    fi
    (
        cd "$stage"
        sha256sum --strict --check "usr/share/doc/mitsuro-canvas/SHA256SUMS"
    )
}

while (($#)); do
    case "$1" in
        --canvas-bin)
            (($# >= 2)) || die "--canvas-bin requires a value"
            canvas_bin="$2"
            shift 2
            ;;
        --server-bin)
            (($# >= 2)) || die "--server-bin requires a value"
            server_bin="$2"
            shift 2
            ;;
        --output-dir)
            (($# >= 2)) || die "--output-dir requires a value"
            output_dir="$2"
            shift 2
            ;;
        --validate-staging)
            (($# >= 2)) || die "--validate-staging requires a value"
            validate_root="$2"
            shift 2
            ;;
        --check-metadata)
            metadata_only=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *) die "unknown argument: $1" ;;
    esac
done

require_command git
require_command grep
require_command jq
require_command readelf
require_command sha256sum

if $metadata_only; then
    [[ -z "$canvas_bin$server_bin$output_dir$validate_root" ]] || die "--check-metadata cannot be combined with other modes"
    validate_metadata
    printf 'Static Linux package metadata is valid.\n'
    exit 0
fi

if [[ -n "$validate_root" ]]; then
    [[ -z "$canvas_bin$server_bin$output_dir" ]] || die "--validate-staging cannot be combined with package inputs"
    validate_root="$(realpath -- "$validate_root")"
    validate_staging "$validate_root"
    printf 'Staging tree is valid: %s\n' "$validate_root"
    exit 0
fi

[[ -n "$canvas_bin" ]] || die "--canvas-bin is required"
[[ -n "$server_bin" ]] || die "--server-bin is required"
[[ -n "$output_dir" ]] || die "--output-dir is required"
[[ "$output_dir" == /* ]] || die "output directory must be absolute: $output_dir"

validate_metadata
require_absolute_file "Canvas binary" "$canvas_bin"
require_absolute_file "server binary" "$server_bin"
require_command cargo
require_command gzip
require_command tar
require_command awk
require_command find
require_command install
require_command realpath
require_command sort
require_command touch
require_command xargs
mkdir -p -- "$output_dir"
output_dir="$(realpath -- "$output_dir")"
canvas_bin="$(realpath -- "$canvas_bin")"
server_bin="$(realpath -- "$server_bin")"

metadata_json="$(cargo metadata --manifest-path "$repo_root/Cargo.toml" --no-deps --format-version 1)"
target_dir="$(jq -er '.target_directory' <<<"$metadata_json")"
canvas_version="$(jq -er '.packages[] | select(.name == "mitsuro-canvas") | .version' <<<"$metadata_json")"
server_version="$(jq -er '.packages[] | select(.name == "mitsuro") | .version' <<<"$metadata_json")"
[[ "$canvas_version" == "$server_version" ]] || die "crate versions differ: Canvas $canvas_version, server $server_version"
grep -Fq "<release version=\"$canvas_version\"" "$script_dir/$APP_ID.metainfo.xml" \
    || die "AppStream release version does not match Cargo metadata: $canvas_version"

expected_canvas="$(realpath -- "$target_dir/release/mitsuro-canvas")"
expected_server="$(realpath -- "$target_dir/release/mitsuro")"
[[ "$canvas_bin" == "$expected_canvas" ]] || die "Canvas binary is not this checkout's exact target/release output: $canvas_bin"
[[ "$server_bin" == "$expected_server" ]] || die "server binary is not this checkout's exact target/release output: $server_bin"
[[ "$($canvas_bin --version)" == "mitsuro-canvas $canvas_version" ]] || die "Canvas binary version does not match Cargo metadata"
[[ "$($server_bin --version)" == "mitsuro $server_version" ]] || die "server binary version does not match Cargo metadata"

canvas_elf="$(elf_identity "$canvas_bin")"
server_elf="$(elf_identity "$server_bin")"
[[ "$canvas_elf" == "$server_elf" ]] || die "Canvas and server ELF architectures differ"
architecture="$(package_architecture "${canvas_elf#*|}")"

source_status="$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)"
[[ -z "$source_status" ]] || die "source checkout is dirty; commit the exact alpha source before packaging"
source_commit="$(git -C "$repo_root" rev-parse HEAD)"
source_commit_short="$(git -C "$repo_root" rev-parse --short=12 HEAD)"
source_epoch="$(git -C "$repo_root" show -s --format=%ct HEAD)"
[[ "$source_commit" =~ ^[0-9a-f]{40}$ ]] || die "source commit is not a full Git object id"
[[ "$source_epoch" =~ ^[0-9]+$ ]] || die "source commit timestamp is invalid"

# A locked no-op build is intentional: Cargo's fingerprints either confirm the
# supplied Release artifacts or rebuild them from this exact clean checkout.
MITSURO_CANVAS_SOURCE_COMMIT="$source_commit" cargo build \
    --manifest-path "$repo_root/Cargo.toml" \
    --release \
    --locked \
    -p mitsuro-canvas \
    -p mitsuro
[[ "$($canvas_bin --version)" == "mitsuro-canvas $canvas_version" ]] || die "rebuilt Canvas version does not match Cargo metadata"
[[ "$($server_bin --version)" == "mitsuro $server_version" ]] || die "rebuilt server version does not match Cargo metadata"

archive_root="mitsuro-canvas-${canvas_version}-alpha-${architecture}-${source_commit_short}"
archive_name="$archive_root.tar.gz"
archive_path="$output_dir/$archive_name"
checksum_path="$archive_path.sha256"
[[ ! -e "$archive_path" && ! -e "$checksum_path" ]] || die "output already exists: $archive_path"

package_tmp="$(mktemp -d "$output_dir/.mitsuro-canvas-package.XXXXXXXX")"
stage="$package_tmp/$archive_root"
install -d -m 0755 \
    "$stage/usr/bin" \
    "$stage/usr/libexec/mitsuro" \
    "$stage/usr/share/applications" \
    "$stage/usr/share/metainfo" \
    "$stage/usr/share/icons/hicolor/scalable/apps" \
    "$stage/usr/share/licenses/mitsuro-canvas" \
    "$stage/usr/share/mitsuro-canvas/assets" \
    "$stage/usr/share/doc/mitsuro-canvas"
install -m 0755 "$canvas_bin" "$stage/usr/bin/mitsuro-canvas"
install -m 0755 "$server_bin" "$stage/usr/libexec/mitsuro/mitsuro"
install -m 0644 "$script_dir/$APP_ID.desktop" "$stage/usr/share/applications/$APP_ID.desktop"
install -m 0644 "$script_dir/$APP_ID.metainfo.xml" "$stage/usr/share/metainfo/$APP_ID.metainfo.xml"
install -m 0644 "$repo_root/assets/branding/mitsuro/mitsuro-app-icon-master.svg" \
    "$stage/usr/share/icons/hicolor/scalable/apps/$APP_ID.svg"
install -m 0644 "$repo_root/LICENSE" "$stage/usr/share/licenses/mitsuro-canvas/LICENSE"
install -m 0644 "$script_dir/README.md" "$stage/usr/share/doc/mitsuro-canvas/README.md"
cp -R -- "$repo_root/apps/desktop/canvas/assets/." "$stage/usr/share/mitsuro-canvas/assets/"
find "$stage/usr/share" -type d -exec chmod 0755 {} +
find "$stage/usr/share" -type f -exec chmod 0644 {} +

canvas_sha256="$(sha256sum "$stage/usr/bin/mitsuro-canvas" | awk '{print $1}')"
server_sha256="$(sha256sum "$stage/usr/libexec/mitsuro/mitsuro" | awk '{print $1}')"
jq -n \
    --arg format "$FORMAT_VERSION" \
    --arg app_id "$APP_ID" \
    --arg package "$PACKAGE_NAME" \
    --arg version "$canvas_version" \
    --arg architecture "$architecture" \
    --arg commit "$source_commit" \
    --argjson epoch "$source_epoch" \
    --arg canvas_sha256 "$canvas_sha256" \
    --arg server_sha256 "$server_sha256" \
    '{
        format: $format,
        app_id: $app_id,
        package: $package,
        version: $version,
        architecture: $architecture,
        source: {commit: $commit, clean: true, source_date_epoch: $epoch},
        binaries: [
            {path: "usr/bin/mitsuro-canvas", sha256: $canvas_sha256},
            {path: "usr/libexec/mitsuro/mitsuro", sha256: $server_sha256}
        ]
    }' >"$stage/usr/share/doc/mitsuro-canvas/PACKAGE-MANIFEST.json"

(
    cd "$stage"
    find usr -type f ! -path 'usr/share/doc/mitsuro-canvas/SHA256SUMS' -print0 \
        | LC_ALL=C sort -z \
        | xargs -0 sha256sum \
        >usr/share/doc/mitsuro-canvas/SHA256SUMS
)

find "$stage" -exec touch -h -d "@$source_epoch" {} +
validate_staging "$stage"

archive_tmp="$package_tmp/$archive_name"
tar \
    --format=gnu \
    --sort=name \
    --mtime="@$source_epoch" \
    --owner=0 \
    --group=0 \
    --numeric-owner \
    -C "$package_tmp" \
    -cf - "$archive_root" \
    | gzip -n >"$archive_tmp"
archive_sha256="$(sha256sum "$archive_tmp" | awk '{print $1}')"
printf '%s  %s\n' "$archive_sha256" "$archive_name" >"$package_tmp/$archive_name.sha256"
mv -- "$archive_tmp" "$archive_path"
mv -- "$package_tmp/$archive_name.sha256" "$checksum_path"

printf 'Created %s\n' "$archive_path"
printf 'SHA-256 %s\n' "$archive_sha256"
printf 'Source %s (clean)\n' "$source_commit"
