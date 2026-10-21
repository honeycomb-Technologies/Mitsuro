#!/usr/bin/env bash
# Isolated Native Canvas Linux/Wayland smoke.
# Never restarts, replaces, or binds live Honey on the canonical port.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE' >&2
Usage:
  run-isolated-linux-smoke.sh --server-bin ABS --canvas-bin ABS --honey-pid N [options]

Required:
  --server-bin ABS     Worktree-built mitsuro companion
  --canvas-bin ABS     Worktree-built mitsuro-canvas
  --honey-pid N        Current live server PID to verify before and after

Optional:
  --project-dir ABS    Companion cwd (default: repo root)
  --evidence-dir ABS   Fresh log directory (default: <repo>/.canvas-smoke/<id>)
  --home ABS           Private HOME (default: <evidence>/isolated-home)
  --port N             Loopback port; never 3000 (default: free 39000-39999)
  --companion-only     Health + canvas --version only; do not start the GUI
  --keep               Leave the companion running on EXIT
  --gui-seconds N      How long to keep the GUI alive (default: 8)
  --honey-port N       Live Honey port that must stay untouched (default: 3000)
USAGE
}

die() {
  printf 'isolated-linux-smoke: %s\n' "$*" >&2
  exit 1
}

script_dir="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || true)"
[[ -n $repo_root ]] || die "could not resolve repository root from $script_dir"

server_bin=""
canvas_bin=""
project_dir=""
evidence_dir=""
run_home=""
port=""
companion_only=false
keep=false
gui_seconds=8
honey_pid=""
honey_port=3000

while (($#)); do
  case "$1" in
    --server-bin) server_bin=${2-}; shift 2 ;;
    --canvas-bin) canvas_bin=${2-}; shift 2 ;;
    --project-dir) project_dir=${2-}; shift 2 ;;
    --evidence-dir) evidence_dir=${2-}; shift 2 ;;
    --home) run_home=${2-}; shift 2 ;;
    --port) port=${2-}; shift 2 ;;
    --companion-only) companion_only=true; shift ;;
    --keep) keep=true; shift ;;
    --gui-seconds) gui_seconds=${2-}; shift 2 ;;
    --honey-pid) honey_pid=${2-}; shift 2 ;;
    --honey-port) honey_port=${2-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

[[ -n $server_bin && -n $canvas_bin ]] || { usage; exit 2; }
[[ $server_bin == /* && $canvas_bin == /* ]] || die "server and canvas binaries must be absolute paths"
[[ -f $server_bin && -x $server_bin ]] || die "server binary missing or not executable: $server_bin"
[[ -f $canvas_bin && -x $canvas_bin ]] || die "canvas binary missing or not executable: $canvas_bin"
[[ $gui_seconds =~ ^[1-9][0-9]*$ ]] || die "gui-seconds must be a positive integer"
[[ $honey_pid =~ ^[1-9][0-9]*$ ]] || die "pass --honey-pid with the current live server PID"
[[ $honey_port =~ ^[1-9][0-9]*$ ]] || die "honey-port must be a positive integer"
[[ $honey_port -ne 0 ]] || die "honey-port must be a positive integer"

for command in curl jq ss readlink git awk; do
  command -v "$command" >/dev/null 2>&1 || die "required command is unavailable: $command"
done

server_bin="$(readlink -f -- "$server_bin")"
canvas_bin="$(readlink -f -- "$canvas_bin")"

if [[ -z $project_dir ]]; then
  project_dir="$repo_root"
fi
[[ $project_dir == /* ]] || die "project-dir must be absolute"
[[ -d $project_dir ]] || die "project-dir is not a directory: $project_dir"
project_dir="$(readlink -f -- "$project_dir")"

if [[ -z $evidence_dir ]]; then
  evidence_dir="$repo_root/.canvas-smoke/$(date +%Y%m%dT%H%M%S)-$$"
fi
[[ $evidence_dir == /* ]] || die "evidence-dir must be absolute"
[[ ! -e $evidence_dir ]] || die "evidence-dir already exists; pass a fresh directory: $evidence_dir"
mkdir -p -- "$evidence_dir"
evidence_dir="$(readlink -f -- "$evidence_dir")"
chmod 700 "$evidence_dir"

if [[ -z $run_home ]]; then
  run_home="$evidence_dir/isolated-home"
fi
[[ $run_home == /* ]] || die "home must be absolute"
[[ ! -e $run_home ]] || die "isolated home already exists: $run_home"
mkdir -m 700 -- "$run_home"
run_home="$(readlink -f -- "$run_home")"
runtime_dir="$run_home/runtime"
mkdir -m 700 -- "$runtime_dir"

real_user_home="$(getent passwd "$(id -u)" | awk -F: '{print $6}')"
if [[ -n $real_user_home ]]; then
  case "$run_home" in
    "$real_user_home"|"$real_user_home/"|"$real_user_home/.mitsuro"|"$real_user_home/.mitsuro"/*)
      die "refusing to use the live user HOME or ~/.mitsuro as the isolated home ($run_home)"
      ;;
  esac
fi

honey_exe=""
if [[ -d /proc/$honey_pid ]]; then
  honey_exe="$(readlink -f -- "/proc/$honey_pid/exe" 2>/dev/null || true)"
fi
if [[ -n $honey_exe && $server_bin == "$honey_exe" ]]; then
  die "server-bin is the live Honey executable; pass a worktree-built companion"
fi

choose_port() {
  local candidate offset seed
  if [[ -n $port ]]; then
    [[ $port =~ ^[1-9][0-9]*$ ]] || die "port must be a positive integer"
    [[ $port -ne $honey_port && $port -ne 3000 ]] || die "refusing to bind live/canonical port $port"
    if [[ -n $(ss -H -ltn "sport = :$port" 2>/dev/null) ]]; then
      die "requested port $port is already in use"
    fi
    return
  fi
  seed=$((39000 + $$ % 800))
  for offset in $(seq 0 199); do
    candidate=$((seed + offset))
    if ((candidate == honey_port || candidate == 3000)); then
      continue
    fi
    if [[ -z $(ss -H -ltn "sport = :$candidate" 2>/dev/null) ]]; then
      port=$candidate
      return
    fi
  done
  die "could not reserve an isolated loopback port"
}
choose_port

process_is_live() {
  local pid=${1-}
  [[ -n $pid && -d /proc/$pid ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  [[ $(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true) != Z ]]
}

honey_snapshot() {
  local pid_alive=0 exe="" listen="" health="null"
  if process_is_live "$honey_pid"; then
    pid_alive=1
    exe="$(readlink -f -- "/proc/$honey_pid/exe" 2>/dev/null || true)"
  fi
  listen="$(ss -H -ltnp "sport = :$honey_port" 2>/dev/null || true)"
  if health_json=$(curl -fsS --max-time 2 "http://127.0.0.1:$honey_port/health" 2>/dev/null); then
    if jq -e . >/dev/null 2>&1 <<<"$health_json"; then
      health=$health_json
    fi
  fi
  jq -n \
    --argjson pid "$honey_pid" \
    --argjson alive "$pid_alive" \
    --arg exe "$exe" \
    --arg listen "$listen" \
    --argjson health "$health" \
    '{pid:$pid,alive:($alive==1),exe:$exe,listen:$listen,health:$health}'
}

assert_honey_untouched() {
  local before after
  before=$(cat "$1")
  after=$(honey_snapshot)
  if ! jq -e --argjson before "$before" '
      .pid == $before.pid
      and .alive == $before.alive
      and .exe == $before.exe
      and (.health.pid // null) == ($before.health.pid // null)
      and (.health.identity // "") == ($before.health.identity // "")
      and (.health.version // "") == ($before.health.version // "")
    ' <<<"$after" >/dev/null; then
    printf '%s\n' "$after" >"$evidence_dir/honey-after-mismatch.json"
    die "live Honey pid $honey_pid / port $honey_port changed; aborting"
  fi
}

honey_snapshot >"$evidence_dir/honey-before.json"
chmod 600 "$evidence_dir/honey-before.json"

canvas_version=$("$canvas_bin" --version || true)
printf '%s\n' "$canvas_version" >"$evidence_dir/canvas-version.txt"
expected_canvas_version=$(awk '/^version = / { gsub(/"/, "", $3); print $3; exit }' "$repo_root/apps/desktop/canvas/Cargo.toml")
[[ -n $expected_canvas_version && $canvas_version == "mitsuro-canvas $expected_canvas_version" ]] || die "canvas version does not match this checkout: $canvas_version"

server_pid=""
canvas_pid=""
cleanup() {
  local running_exe
  if [[ -n ${canvas_pid:-} ]] && process_is_live "$canvas_pid"; then
    running_exe=$(readlink -f -- "/proc/$canvas_pid/exe" 2>/dev/null || true)
    if [[ $running_exe == "$canvas_bin" ]]; then
      kill -TERM "$canvas_pid" 2>/dev/null || true
      wait "$canvas_pid" 2>/dev/null || true
    fi
  fi
  if $keep; then
    return
  fi
  if [[ -n ${server_pid:-} ]] && process_is_live "$server_pid"; then
    running_exe=$(readlink -f -- "/proc/$server_pid/exe" 2>/dev/null || true)
    if [[ $running_exe == "$server_bin" ]]; then
      kill -TERM "$server_pid" 2>/dev/null || true
      wait "$server_pid" 2>/dev/null || true
    else
      printf 'isolated-linux-smoke: refused to stop changed PID %s (%s)\n' "$server_pid" "$running_exe" >&2
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

server_log="$evidence_dir/server.log"
(
  cd -- "$project_dir"
  exec env -u MITSURO_SERVER_URL HOME="$run_home" XDG_RUNTIME_DIR="$runtime_dir"     "$server_bin" serve --desktop-managed --port "$port"
) >"$server_log" 2>&1 &
server_pid=$!
printf '%s\n' "$server_pid" >"$evidence_dir/companion.pid"

health_url="http://127.0.0.1:$port/health"
health_file="$evidence_dir/server-health.json"
healthy=0
for _ in $(seq 1 150); do
  if curl -fsS --max-time 1 "$health_url" >"$health_file.tmp" 2>/dev/null; then
    health_pid=$(jq -r '.pid // 0' "$health_file.tmp")
    health_identity=$(jq -r '.identity // ""' "$health_file.tmp")
    health_status=$(jq -r '.status // ""' "$health_file.tmp")
    health_protocol=$(jq -r '.api_protocol // 0' "$health_file.tmp")
    if [[ $health_pid == "$server_pid" && $health_identity == mitsuro-server && $health_status == ok && $health_protocol == 1 ]]; then
      mv -- "$health_file.tmp" "$health_file"
      healthy=1
      break
    fi
  fi
  if ! process_is_live "$server_pid"; then
    die "isolated companion exited during startup; see $server_log"
  fi
  sleep 0.2
done
[[ $healthy == 1 ]] || die "isolated companion did not publish matching health on $health_url; see $server_log"

sleep 0.2
curl -fsS --max-time 2 "$health_url" >"$evidence_dir/server-health-stable.json"
if [[ $(jq -cS '{pid,status,identity,version,api_protocol,capabilities}' "$health_file") != $(jq -cS '{pid,status,identity,version,api_protocol,capabilities}' "$evidence_dir/server-health-stable.json") ]]; then
  die "isolated companion health authority was not stable across two samples"
fi

running_server_exe=$(readlink -f -- "/proc/$server_pid/exe" 2>/dev/null || true)
[[ $running_server_exe == "$server_bin" ]] || die "companion PID executes $running_server_exe, not $server_bin"
[[ $server_pid != "$honey_pid" ]] || die "companion PID unexpectedly equals live Honey PID"

for capability in desktop_canvases:1 terminal_sessions:2 browser_sessions:1 agent_streams:1; do
  name=${capability%:*}
  required=${capability#*:}
  actual=$(jq -r --arg name "$name" '.capabilities[$name] // 0' "$health_file")
  if ((actual < required)); then
    die "isolated companion lacks $capability (has $actual)"
  fi
done

resource_code=$(curl -sS -o "$evidence_dir/canvas-resources.body" -w '%{http_code}' --max-time 2 \
  "http://127.0.0.1:$port/api/canvases/smoke/resources" || true)
printf '%s\n' "$resource_code" >"$evidence_dir/canvas-resources.status"
if [[ $resource_code != 401 && $resource_code != 404 && $resource_code != 400 && $resource_code != 422 && $resource_code != 200 ]]; then
  die "unexpected canvas resource route status $resource_code"
fi
if head -c 15 "$evidence_dir/canvas-resources.body" | grep -q '<!DOCTYPE html>'; then
  die "companion served SPA HTML for canvas resource routes; those routes are missing"
fi

assert_honey_untouched "$evidence_dir/honey-before.json"
honey_snapshot >"$evidence_dir/honey-after-companion.json"

jq -n \
  --arg server_bin "$server_bin" \
  --arg canvas_bin "$canvas_bin" \
  --arg run_home "$run_home" \
  --arg evidence_dir "$evidence_dir" \
  --arg canvas_version "$canvas_version" \
  --argjson companion_pid "$server_pid" \
  --argjson companion_port "$port" \
  --argjson honey_pid "$honey_pid" \
  --argjson honey_port "$honey_port" \
  '{schema_version:1,companion:{path:$server_bin,pid:$companion_pid,port:$companion_port,home:$run_home},canvas:{path:$canvas_bin,version:$canvas_version},honey:{pid:$honey_pid,port:$honey_port},evidence_dir:$evidence_dir}' \
  >"$evidence_dir/run-provenance.json"

printf 'companion health ok pid=%s port=%s version=%s\n' "$server_pid" "$port" "$(jq -r '.version' "$health_file")"
printf 'canvas version: %s\n' "$canvas_version"
printf 'evidence: %s\n' "$evidence_dir"

if $companion_only; then
  printf 'companion-only complete; GUI not started\n'
  if $keep; then
    printf 'companion left running: HOME=%s %s serve --desktop-managed --port %s\n' "$run_home" "$server_bin" "$port"
  fi
  exit 0
fi

if [[ -z ${WAYLAND_DISPLAY:-} && -z ${DISPLAY:-} ]]; then
  die "no WAYLAND_DISPLAY or DISPLAY; rerun unsandboxed or pass --companion-only"
fi

canvas_log="$evidence_dir/canvas.log"
env -u MITSURO_SERVER_URL \
  HOME="$run_home" \
  MITSURO_SERVER_BIN="$server_bin" \
  "$canvas_bin" \
  >"$canvas_log" 2>&1 &
canvas_pid=$!
printf '%s\n' "$canvas_pid" >"$evidence_dir/canvas.pid"

mapped=0
for _ in $(seq 1 $((gui_seconds * 5))); do
  if ! process_is_live "$canvas_pid"; then
    break
  fi
  if command -v hyprctl >/dev/null 2>&1; then
    if hyprctl -j clients 2>/dev/null | jq -e --argjson pid "$canvas_pid" 'any(.[]; .pid == $pid and .mapped == true)' >/dev/null; then
      mapped=1
      break
    fi
  fi
  sleep 0.2
done

if ! process_is_live "$canvas_pid"; then
  wait "$canvas_pid" 2>/dev/null || true
  die "canvas exited before smoke window evidence; see $canvas_log"
fi

if grep -Eiq 'failed to open Mitsuro Canvas|Port 3000 is already in use|cannot host Native Canvas' "$canvas_log"; then
  die "canvas reported a bootstrap/window failure; see $canvas_log"
fi

if command -v hyprctl >/dev/null 2>&1; then
  hyprctl -j clients 2>/dev/null | jq --argjson pid "$canvas_pid" '[.[] | select(.pid == $pid)]' \
    >"$evidence_dir/hypr-canvas-clients.json" || true
fi

{
  printf 'canvas_pid=%s\n' "$canvas_pid"
  printf 'mapped=%s\n' "$mapped"
  printf 'WAYLAND_DISPLAY=%s\n' "${WAYLAND_DISPLAY:-}"
  printf 'companion_port=%s honey_port=%s\n' "$port" "$honey_port"
  if [[ -s $canvas_log ]]; then
    printf '%s\n' '--- canvas.log ---'
    cat "$canvas_log"
  fi
} >"$evidence_dir/gui-smoke.txt"

assert_honey_untouched "$evidence_dir/honey-before.json"

printf 'GUI process alive pid=%s mapped=%s (existing compositor WAYLAND_DISPLAY=%s)\n' \
  "$canvas_pid" "$mapped" "${WAYLAND_DISPLAY:-}"
printf 'companion pid=%s port=%s (honey pid=%s port=%s untouched)\n' \
  "$server_pid" "$port" "$honey_pid" "$honey_port"

if process_is_live "$canvas_pid"; then
  running_exe=$(readlink -f -- "/proc/$canvas_pid/exe" 2>/dev/null || true)
  if [[ $running_exe == "$canvas_bin" ]]; then
    kill -TERM "$canvas_pid" 2>/dev/null || true
    wait "$canvas_pid" 2>/dev/null || true
  fi
fi
canvas_pid=""

if $keep; then
  printf 'companion left running: pid=%s port=%s HOME=%s\n' "$server_pid" "$port" "$run_home"
fi
