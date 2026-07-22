#!/usr/bin/env bash
set -euo pipefail

repro_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "$repro_dir/../.." && pwd)"
config_path="$repro_dir/service-config.yaml"
compose_file="$repro_dir/compose.yaml"

usage() {
  cat <<'EOF'
Usage: ./repro.sh COMMAND

Commands:
  backend-up       Start Postgres and MongoDB and wait for initialization
  backend-down     Stop the databases, preserving their volumes
  reset            Stop the databases and delete this harness's volumes
  build            Build the local PowerSync runner and test client
  replication      Run only the replication runner; stop it manually with Ctrl-C
  api              Run only the API runner
  api-profile      Run the API with Node heap profiling and RSS/VSZ sampling
  api-clinic       Run the API under the globally installed Clinic.js Doctor
  clients          Spawn concurrent streaming clients

Client environment variables:
  CLIENTS=70       Number of concurrent clients
  MODE=http        http or websocket
  ONCE=1           Set to 0 to keep clients connected after the first checkpoint

Profiling environment variables:
  MAX_OLD_SPACE_PERCENTAGE=80  Optional V8 old-space percentage for API commands
  MAX_OLD_SPACE_MB=576         Fixed V8 old-space limit (default: 576 MiB)
  SAMPLE_SECONDS=1             RSS/VSZ sample interval
  CLINIC_COLLECT_ONLY=0        Set to 1 to collect Clinic data without generating HTML
EOF
}

compose() {
  docker compose -f "$compose_file" "$@"
}

build() {
  (cd "$repo_dir" && corepack pnpm --filter @powersync/service-image build)
  (cd "$repo_dir" && corepack pnpm --filter test-client build)
}

run_service() {
  local runner="$1"
  exec node "$repo_dir/service/lib/entry.js" start --runner-type "$runner" --config-path "$config_path"
}

node_memory_args=()
configure_node_memory_args() {
  if [[ -n "${MAX_OLD_SPACE_PERCENTAGE:-}" && -n "${MAX_OLD_SPACE_MB:-}" ]]; then
    echo "Set only one of MAX_OLD_SPACE_PERCENTAGE or MAX_OLD_SPACE_MB." >&2
    exit 1
  fi

  if [[ -n "${MAX_OLD_SPACE_PERCENTAGE:-}" ]]; then
    node_memory_args+=("--max-old-space-size-percentage=$MAX_OLD_SPACE_PERCENTAGE")
  else
    node_memory_args+=("--max-old-space-size=${MAX_OLD_SPACE_MB:-576}")
  fi
}

run_api() {
  configure_node_memory_args
  exec node "${node_memory_args[@]}" "$repo_dir/service/lib/entry.js" start \
    --runner-type api --config-path "$config_path"
}

case "${1:-}" in
  backend-up)
    compose up -d --wait postgres mongo
    compose run --rm mongo-rs-init
    compose ps
    ;;
  backend-down)
    compose down
    ;;
  reset)
    compose down --volumes
    ;;
  build)
    build
    ;;
  replication)
    run_service sync
    ;;
  api)
    run_api
    ;;
  api-profile)
    timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    profile_dir="$repro_dir/profiles/$timestamp"
    mkdir -p "$profile_dir"

    node_args=(
      --heap-prof
      --heap-prof-dir="$profile_dir"
      --diagnostic-dir="$profile_dir"
      --report-on-fatalerror
      --report-on-signal
      --report-signal=SIGUSR2
      --heapsnapshot-near-heap-limit=3
    )
    configure_node_memory_args
    node_args+=("${node_memory_args[@]}")

    node "${node_args[@]}" "$repo_dir/service/lib/entry.js" start \
      --runner-type api --config-path "$config_path" &
    service_pid=$!
    "$repro_dir/monitor-memory.sh" "$service_pid" "$profile_dir/memory.csv" "${SAMPLE_SECONDS:-1}" &
    monitor_pid=$!

    stop_children() {
      kill -TERM "$service_pid" 2>/dev/null || true
      kill -TERM "$monitor_pid" 2>/dev/null || true
    }
    trap stop_children INT TERM EXIT

    echo "API PID: $service_pid"
    echo "Profile output: $profile_dir"
    echo "Send SIGUSR2 for a diagnostic report: kill -USR2 $service_pid"
    set +e
    wait "$service_pid"
    status=$?
    set -e
    kill -TERM "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
    trap - INT TERM EXIT
    exit "$status"
    ;;
  api-clinic)
    if ! command -v clinic >/dev/null 2>&1; then
      echo "Clinic.js is not installed or is not on PATH." >&2
      echo "Install it with: npm install -g clinic" >&2
      exit 1
    fi

    timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    profile_dir="$repro_dir/profiles/clinic-$timestamp"
    mkdir -p "$profile_dir"

    clinic_args=(doctor --dest "$profile_dir")
    if [[ "${CLINIC_COLLECT_ONLY:-0}" == "1" ]]; then
      clinic_args+=(--collect-only)
    fi

    configure_node_memory_args

    echo "Clinic.js output: $profile_dir"
    exec clinic "${clinic_args[@]}" -- \
      node "${node_memory_args[@]}" "$repo_dir/service/lib/entry.js" start \
      --runner-type api --config-path "$config_path"
    ;;
  clients)
    clients="${CLIENTS:-70}"
    mode="${MODE:-http}"
    once_args=()
    if [[ "${ONCE:-1}" != "0" ]]; then
      once_args+=(--once)
    fi
    exec node "$repo_dir/test-client/dist/bin.js" concurrent-connections \
      --config "$config_path" \
      --endpoint http://127.0.0.1:8080 \
      --sub shared-test-user \
      --num-clients "$clients" \
      --mode "$mode" \
      "${once_args[@]}"
    ;;
  *)
    usage
    exit 1
    ;;
esac
