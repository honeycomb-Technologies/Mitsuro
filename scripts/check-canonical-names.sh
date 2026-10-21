#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"

allowlist="scripts/canonical-name-compatibility.tsv"
audit_script="scripts/check-canonical-names.sh"
local_orchestrator_file="docs/plans/_orch-verify-matrix.txt"
legacy_pattern='[Kk][Rr][Uu][Ss][Tt][Yy]|[Mm][Aa][Kk][Oo]'
key_separator=$'\034'
failures=0

audit_tmp_dir="$(mktemp -d)"
cleanup_audit_tmp() {
  rm -rf -- "$audit_tmp_dir"
}
trap cleanup_audit_tmp EXIT

fail() {
  printf 'FAIL  %s\n' "$1"
  failures=$((failures + 1))
}

pass() {
  printf 'PASS  %s\n' "$1"
}

require_file() {
  if [[ -f "$1" ]]; then
    pass "$1 exists"
  else
    fail "$1 is missing"
  fi
}

require_text() {
  local label="$1"
  local pattern="$2"
  local path="$3"
  if rg -q -- "$pattern" "$path"; then
    pass "$label"
  else
    fail "$label"
  fi
}

reject_matches() {
  local label="$1"
  local pattern="$2"
  shift 2
  local output status=0

  output="$(rg -n --hidden \
    --glob '!target/**' \
    --glob '!node_modules/**' \
    --glob '!dist/**' \
    --glob '!build/**' \
    --glob '!.git/**' \
    --glob '!**/*.lock' \
    --glob '!**/test/**' \
    --glob '!**/tests/**' \
    --glob '!**/*.test.*' \
    --glob "!$audit_script" \
    --glob "!$allowlist" \
    --glob "!$local_orchestrator_file" \
    -- "$pattern" "$@" 2>&1)" || status=$?

  case "$status" in
    0)
      fail "$label"
      printf '%s\n' "$output"
      ;;
    1)
      pass "$label"
      ;;
    *)
      fail "$label (ripgrep audit failed with exit $status)"
      printf '%s\n' "$output"
      ;;
  esac
}

declare -A allowed_paths=()
declare -A allowed_content=()
declare -A seen_rules=()
declare -a declared_rules=()

load_allowlist() {
  local line_number=0
  local raw kind remainder path source_line expected key

  while IFS= read -r raw || [[ -n "${raw:-}" ]]; do
    line_number=$((line_number + 1))
    [[ -z "${raw:-}" || "$raw" == \#* ]] && continue

    if [[ "$raw" != *$'\t'* ]]; then
      fail "$allowlist:$line_number is missing its path field"
      continue
    fi
    kind=${raw%%$'\t'*}
    remainder=${raw#*$'\t'}

    case "$kind" in
      path)
        path=$remainder
        if [[ -z "$path" || "$path" == *$'\t'* ]]; then
          fail "$allowlist:$line_number path rules take exactly two fields"
          continue
        fi
        if [[ "$path" == *"$key_separator"* || "$path" == *$'\r'* ]]; then
          fail "$allowlist:$line_number path contains an unsupported control byte"
          continue
        fi
        key="path"$'\034'"$path"
        allowed_paths["$path"]=1
        ;;
      content)
        # Format: content\tpath\tline\texact source line
        # Matching is by path + exact source text only. The line number is
        # documentation for humans and does not participate in the match, so
        # edits that only shift lines no longer break CI.
        if [[ "$remainder" != *$'\t'* ]]; then
          fail "$allowlist:$line_number content rules require a path, line number, and exact source line"
          continue
        fi
        path=${remainder%%$'\t'*}
        remainder=${remainder#*$'\t'}
        if [[ "$remainder" != *$'\t'* ]]; then
          fail "$allowlist:$line_number content rules require a line number and exact source line"
          continue
        fi
        source_line=${remainder%%$'\t'*}
        expected=${remainder#*$'\t'}
        if [[ -z "$path" || ! "$source_line" =~ ^[1-9][0-9]*$ || -z "$expected" ]]; then
          fail "$allowlist:$line_number content rules require a path, positive line number, and exact source line"
          continue
        fi
        if [[ "$path" == *"$key_separator"* || "$path" == *$'\r'* || "$expected" == *"$key_separator"* ]]; then
          fail "$allowlist:$line_number content rule contains an unsupported control byte"
          continue
        fi
        key="content"$'\034'"$path"$'\034'"$expected"
        allowed_content["$path"$'\034'"$expected"]=1
        ;;
      *)
        fail "$allowlist:$line_number has unknown rule type '$kind'"
        continue
        ;;
    esac

    # Content rules may list the same path+text at multiple historic line
    # numbers; keep a single logical rule so text-based matching stays stable.
    if [[ -n "${seen_rules[$key]+present}" ]]; then
      continue
    fi
    seen_rules["$key"]=0
    declared_rules+=("$key")
  done < "$allowlist"
}

audit_legacy_paths() {
  local paths_file="$audit_tmp_dir/tracked-paths"
  local path key display

  if ! git ls-files -z --cached --others --exclude-standard > "$paths_file"; then
    fail "git ls-files failed while discovering legacy identity paths"
    return
  fi

  while IFS= read -r -d '' path; do
    [[ -e "$path" || -L "$path" ]] || continue
    [[ "$path" != "$audit_script" && "$path" != "$allowlist" && "$path" != "$local_orchestrator_file" ]] || continue
    [[ "$path" =~ $legacy_pattern ]] || continue

    printf -v display '%q' "$path"
    if [[ "$path" == *$'\t'* || "$path" == *$'\n'* || "$path" == *$'\r'* || "$path" == *"$key_separator"* ]]; then
      fail "legacy path cannot be represented safely in the compatibility TSV: $display"
    elif [[ -n "${allowed_paths[$path]+present}" ]]; then
      key="path"$'\034'"$path"
      seen_rules["$key"]=1
    else
      fail "legacy identity remains in tracked path: $display"
    fi
  done < "$paths_file"
}

audit_legacy_content() {
  local matches_file="$audit_tmp_dir/content-matches"
  local grep_status=0
  local path line_number source_line lookup key display

  git grep -z --untracked -n --no-column --no-color --no-heading --no-break -I -E -e "$legacy_pattern" -- \
    ":(exclude)$audit_script" \
    ":(exclude)$allowlist" \
    ":(exclude)$local_orchestrator_file" > "$matches_file" || grep_status=$?
  if (( grep_status > 1 )); then
    fail "git grep failed while discovering legacy identity content (exit $grep_status)"
    return
  fi

  while IFS= read -r -d '' path; do
    if ! IFS= read -r -d '' line_number; then
      fail "git grep returned a truncated line-number record"
      break
    fi
    if ! IFS= read -r source_line && [[ -z "$source_line" ]]; then
      fail "git grep returned a truncated source-line record"
      break
    fi

    printf -v display '%q' "$path"
    if [[ ! "$line_number" =~ ^[1-9][0-9]*$ ]]; then
      fail "git grep returned an invalid line number for $display"
      continue
    fi
    if [[ "$path" == *$'\t'* || "$path" == *$'\n'* || "$path" == *$'\r'* || "$path" == *"$key_separator"* || "$source_line" == *"$key_separator"* ]]; then
      fail "legacy content cannot be represented safely in the compatibility TSV at $display:$line_number"
      continue
    fi

    # Match allowlist by exact path + source text (line numbers are advisory).
    lookup="$path"$'\034'"$source_line"
    if [[ -n "${allowed_content[$lookup]+present}" ]]; then
      key="content"$'\034'"$lookup"
      seen_rules["$key"]=1
    else
      fail "legacy identity remains at $display:$line_number"
      printf '      %s\n' "$source_line"
    fi
  done < "$matches_file"
}

audit_stale_rules() {
  local key kind payload path source_line expected
  for key in "${declared_rules[@]}"; do
    [[ "${seen_rules[$key]}" == 1 ]] && continue
    kind=${key%%$'\034'*}
    payload=${key#*$'\034'}
    if [[ "$kind" == path ]]; then
      fail "stale compatibility path rule: $payload"
    else
      path=${payload%%$'\034'*}
      expected=${payload#*$'\034'}
      fail "stale compatibility content rule for $path (exact line no longer present)"
      printf '      %s\n' "$expected"
    fi
  done
}

printf 'Mitsuro canonical identity audit\n'

require_file "$allowlist"
require_file .github/homebrew/mitsuro.rb
require_file deploy/systemd/mitsuro-hive.service
require_file deploy/systemd/mitsuro-hive.socket
require_file deploy/systemd/mitsuro-serve.service
require_file crates/mitsuro-cli/Cargo.toml
require_file crates/mitsuro-core/Cargo.toml
require_file crates/mitsuro-server/Cargo.toml
require_file crates/mitsuro-hive/Cargo.toml
require_file crates/mitsuro-hive-protocol/Cargo.toml
require_file scripts/identity-env.sh
require_file aur/check-package-template.sh

for asset in \
  assets/branding/mitsuro/mitsuro-cell-flat.svg \
  assets/branding/mitsuro/mitsuro-cell-mono.svg \
  assets/branding/mitsuro/mitsuro-cell-dimensional.svg \
  assets/branding/mitsuro/mitsuro-wordmark.svg \
  assets/branding/mitsuro/mitsuro-lockup-horizontal.svg \
  assets/branding/mitsuro/mitsuro-hive.svg \
  assets/branding/mitsuro/mitsuro-app-icon-master.svg \
  assets/branding/mitsuro/mitsuro-app-icon-tinted.svg \
  assets/branding/mitsuro/mitsuro-adaptive-foreground.svg \
  apps/mobile/assets/animations/splash.json \
  apps/mobile/assets/icons/mitsuro-notification.png; do
  require_file "$asset"
done

for retired in \
  assets/branding/krusty-k.png \
  icons/krusty-k.svg \
  icons/krusty-k-theme.svg \
  icons/mako-shark.svg \
  apps/mobile/app/navigation-preview.tsx; do
  if [[ -e "$retired" || -L "$retired" ]]; then
    fail "$retired must remain retired"
  else
    pass "$retired is retired"
  fi
done

require_text "installer publishes the Mitsuro binary" '^BINARY="mitsuro"$' install.sh
require_text "installer publishes the Hive daemon" '^DAEMON_BINARY="mitsuro-hive"$' install.sh
require_text "CLI compatibility shim remains explicit" '^name = "krusty"$' crates/mitsuro-cli/Cargo.toml
require_text "Hive compatibility shim remains explicit" '^name = "krusty-mako"$' crates/mitsuro-hive/Cargo.toml
require_text "installer uses explicit offline identity migration" 'migrate-identity --confirm-offline' install.sh
require_text "release workflow emits Mitsuro archives" 'mitsuro-.*\.(tar\.gz|zip)' .github/workflows/release.yml
require_text "release workflow verifies compatibility forwarding" 'Verify compatibility command forwarding' .github/workflows/release.yml
require_text "Homebrew formula is Mitsuro" '^class Mitsuro < Formula$' .github/homebrew/mitsuro.rb
require_text "AUR package is Mitsuro" '^pkgname=mitsuro$' aur/PKGBUILD
require_text "AUR declares the transition provide" "^provides=\\('krusty'\\)$" aur/PKGBUILD
require_text "Hive API is canonical" '/api/hive/' docs/interfaces/server-api.md
require_text "canonical launch scheme is documented" 'mitsuro://' AGENTS.md
require_text "Expo display name is Mitsuro" '"name"[[:space:]]*:[[:space:]]*"Mitsuro"' apps/mobile/app.json
require_text "Expo registers multiple deep-link schemes" '"scheme"[[:space:]]*:[[:space:]]*\[' apps/mobile/app.json
require_text "Mitsuro is a registered deep-link scheme" '"mitsuro"' apps/mobile/app.json
require_text "Expo notification icon is Mitsuro" 'mitsuro-notification\.png' apps/mobile/app.json
require_text "desktop product name is Mitsuro Canvas" '^Name=Mitsuro Canvas Alpha$' apps/desktop/canvas/packaging/io.mitsuro.canvas.desktop
require_text "desktop launcher uses Canvas" '^Exec=mitsuro-canvas$' apps/desktop/canvas/packaging/io.mitsuro.canvas.desktop
require_text "desktop identity is canonical" '<id>io\.mitsuro\.canvas</id>' apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml
require_text "desktop publisher is Honeycomb Technologies" '<name>Honeycomb Technologies</name>' apps/desktop/canvas/packaging/io.mitsuro.canvas.metainfo.xml
require_text "canonical GitHub repository is Mitsuro" 'github\.com/honeycomb-Technologies/Mitsuro' README.md
require_text "preferred autonomous CLI is Hive" 'name[[:space:]]*=[[:space:]]*"hive"' crates/mitsuro-cli/src/main.rs
require_text "legacy autonomous CLI alias is retained" '(visible_)?alias[[:space:]]*=[[:space:]]*"mako"' crates/mitsuro-cli/src/main.rs
require_text "shared accent is mineral violet" '#75617e' packages/ui/src/tokens.ts
require_text "shared foundation is graphite" '#0e0e11' packages/ui/src/tokens.ts
require_text "shared foreground is wax" '#e8e5ea' packages/ui/src/tokens.ts
require_text "Hive service metadata is branded" 'Description=Mitsuro Hive' deploy/systemd/mitsuro-hive.service

# The old v1 audit froze the legacy TUI tree. This full identity migration moves
# that tree under the canonical crate; path/content enforcement below supersedes
# the old-path diff assertion while leaving presentation output unchanged.

reject_matches \
  "production frontends contain no retired mascot entry points" \
  '(CrabIcon|MakoSharkIcon|KrustyLogo|krusty-k|mako-shark)' \
  apps/mobile apps/desktop apps/website packages

reject_matches \
  "production frontends contain no retired exact display labels" \
  '("(Krusty|Mako)"|'\''(Krusty|Mako)'\''|>(Krusty|Mako)<)' \
  apps/mobile apps/desktop apps/website packages

reject_matches \
  "production frontends contain no retired orange, navy, or OAuth-gradient colors" \
  '(#ff6b35|#FF6B35|#e17a30|#E17A30|#1a1f2e|#151b2b|#101827|#667eea|#764ba2)' \
  apps/mobile apps/desktop apps/website packages \
  crates/mitsuro-core/src/auth/browser_flow/callback_server.rs \
  crates/mitsuro-server/src/routes/oauth/callback.rs

reject_matches \
  "active public copy contains no retired mascot slogans" \
  '(Always Swimming|Set course|Schedule course|Krusty the Krab|Horseshoe Crab|Mantis Shrimp)' \
  apps/mobile apps/desktop apps/website packages README.md docs \
  crates/mitsuro-cli/src/main.rs crates/mitsuro-cli/src/serve.rs

reject_matches \
  "active sources contain no retired GitHub repository URL" \
  '(github\.com|raw\.githubusercontent\.com|api\.github\.com/repos)/honeycomb-Technologies/Krusty' \
  .

for identity_env_consumer in \
  scripts/agent-parity-report.sh \
  scripts/cache-efficiency-report.sh \
  scripts/core-eval.sh \
  scripts/mobile-macbook-ssh-check.sh \
  scripts/mobile-runtime-smoke.sh \
  scripts/server-audit.sh \
  scripts/server-smoke.sh; do
  require_text \
    "$identity_env_consumer imports the identity environment bridge" \
    'source .*identity-env\.sh' \
    "$identity_env_consumer"
done

if bash scripts/identity-env.sh --self-test >/dev/null; then
  pass "identity environment bridge preserves canonical precedence"
else
  fail "identity environment bridge self-test failed"
fi

if bash aur/check-package-template.sh >/dev/null; then
  pass "AUR metadata and canonical source strategy are coherent"
else
  fail "AUR package template check failed"
fi

if [[ -f "$allowlist" ]]; then
  load_allowlist
  audit_legacy_paths
  audit_legacy_content
  audit_stale_rules
fi

if (( failures > 0 )); then
  printf '\nMitsuro canonical identity audit failed with %d issue(s).\n' "$failures"
  exit 1
fi

printf '\nMitsuro canonical identity audit passed.\n'
