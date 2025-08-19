#!/usr/bin/env bash
set -euo pipefail

API="http://localhost:8080"
SCHED="http://localhost:8082"
WORKER="http://localhost:8083"  # if you mapped it in compose; otherwise skip

check() {
  local url="$1" path="$2"
  printf "GET %s%s -> " "$url" "$path"
  code="$(curl -s -o /dev/null -w '%{http_code}' "$url$path" || true)"
  echo "$code"
  if [[ "$code" == "200" ]]; then
    curl -s "$url$path" | sed 's/.*/  &/'
  fi
  echo
}

echo "==> API"
check "$API" "/healthz"
check "$API" "/readyz" || true

echo "==> Scheduler"
check "$SCHED" "/role" || true
check "$SCHED" "/healthz" || true
check "$SCHED" "/readyz" || true

echo "==> Worker"
check "$WORKER" "/healthz" || true
check "$WORKER" "/readyz" || true
