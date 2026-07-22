#!/usr/bin/env bash
set -euo pipefail

pid="${1:?usage: monitor-memory.sh PID OUTPUT.csv [INTERVAL_SECONDS]}"
output="${2:?usage: monitor-memory.sh PID OUTPUT.csv [INTERVAL_SECONDS]}"
interval="${3:-1}"

mkdir -p "$(dirname "$output")"
echo "timestamp,pid,rss_kib,vsz_kib" > "$output"

while kill -0 "$pid" 2>/dev/null; do
  values="$(ps -o rss= -o vsz= -p "$pid" | awk '{$1=$1; print}')"
  if [[ -n "$values" ]]; then
    read -r rss_kib vsz_kib <<< "$values"
    printf '%s,%s,%s,%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$pid" "$rss_kib" "$vsz_kib" >> "$output"
  fi
  sleep "$interval"
done
