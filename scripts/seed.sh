#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"
COLOR=${COLOR:-1}

log() { if [[ "${COLOR}" == "1" ]]; then printf "\033[1;36m==>\033[0m %s\n" "$*"; else printf "==> %s\n" "$*"; fi; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }; }

json_get() {
  local json="$1" path="$2"
  if command -v jq >/dev/null 2>&1; then echo "$json" | jq -r "$path"; else
    case "$path" in
      ".job.id")      echo "$json" | grep -oE '"id"\s*:\s*"[^"]+"' | head -n1 | sed -E 's/.*"id"\s*:\s*"([^"]+)".*/\1/';;
      ".schedule.id") echo "$json" | grep -oE '"id"\s*:\s*"[^"]+"' | tail -n1 | sed -E 's/.*"id"\s*:\s*"([^"]+)".*/\1/';;
      ".run_id")      echo "$json" | grep -oE '"run_id"\s*:\s*"[^"]+"' | head -n1 | sed -E 's/.*"run_id"\s*:\s*"([^"]+)".*/\1/';;
      ".runId")       echo "$json" | grep -oE '"runId"\s*:\s*"[^"]+"' | head -n1 | sed -E 's/.*"runId"\s*:\s*"([^"]+)".*/\1/';;
      *) echo "";;
    esac
  fi
}

# cross-platform RFC3339 UTC in +N seconds (mac BSD date / GNU date)
rfc3339_utc_plus() {
  local sec="${1:-15}"
  if date -u -v+"${sec}"S "+%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1; then
    date -u -v+"${sec}"S "+%Y-%m-%dT%H:%M:%SZ"
  else
    date -u -d "+${sec} seconds" "+%Y-%m-%dT%H:%M:%SZ"
  fi
}

wait_for_http_200() {
  local url="$1" attempts="${2:-60}"
  log "Waiting for $url to be ready..."
  for _ in $(seq 1 "$attempts"); do
    if curl -s -o /dev/null -w '%{http_code}' "$url" | grep -q '^200$'; then log "OK: $url is ready"; return 0; fi
    sleep 1
  done
  echo "Timeout waiting for $url" >&2; exit 1
}

need curl

# 1) API ready
wait_for_http_200 "${API_URL}/healthz"

# 2) Create a shell job with args in BOTH fields (some servers expect argsJson)
log 'Creating shell job: echo "hello"'
CREATE_JOB_PAYLOAD='{
  "name": "hello-shell",
  "type": "batch",
  "handler": "shell",
  "args": { "command": "echo \"hello\"" },
  "argsJson": "{\"command\":\"echo hello\"}",
  "enabled": true
}'
JOB_RESP="$(curl -sS -X POST "${API_URL}/v1/jobs" -H "Content-Type: application/json" -d "${CREATE_JOB_PAYLOAD}")"
echo "$JOB_RESP" | sed 's/.*/[jobs.create] &/'
JOB_ID="$(json_get "$JOB_RESP" ".job.id")"
[[ -z "$JOB_ID" || "$JOB_ID" == "null" ]] && { echo "Could not parse job id from response above." >&2; exit 1; }
log "Created job id: ${JOB_ID}"

# 3) Create a fixed-interval schedule every 15s, next_run_at in ~5s
NEXT_RUN_AT="$(rfc3339_utc_plus 5)"
log "Creating schedule (every 15s, UTC). next_run_at=${NEXT_RUN_AT}"
CREATE_SCHED_PAYLOAD="$(cat <<JSON
{
  "job_id": "${JOB_ID}",
  "fixed_interval_seconds": 15,
  "timezone": "UTC",
  "next_run_at": "${NEXT_RUN_AT}",
  "enabled": true
}
JSON
)"
SCHED_RESP="$(curl -sS -X POST "${API_URL}/v1/schedules" -H "Content-Type: application/json" -d "${CREATE_SCHED_PAYLOAD}")"
echo "$SCHED_RESP" | sed 's/.*/[schedules.create] &/'
SCHED_ID="$(json_get "$SCHED_RESP" ".schedule.id")"
[[ -z "$SCHED_ID" || "$SCHED_ID" == "null" ]] && { echo "Could not parse schedule id from response above." >&2; exit 1; }
log "Created schedule id: ${SCHED_ID}"

# 4) Trigger an ad-hoc run WITH args to guarantee execution
log "Triggering ad-hoc run now (with args)"
RUN_PAYLOAD='{"args":{"command":"echo \"hello from adhoc\""}}'
RUN_RESP="$(curl -sS -X POST "${API_URL}/v1/jobs/${JOB_ID}:run" -H "Content-Type: application/json" -d "${RUN_PAYLOAD}")"
echo "$RUN_RESP" | sed 's/.*/[jobs.run] &/'
RUN_ID="$(json_get "$RUN_RESP" ".run_id")"
[[ -z "$RUN_ID" || "$RUN_ID" == "null" ]] && RUN_ID="$(json_get "$RUN_RESP" ".runId")"
[[ -z "$RUN_ID" || "$RUN_ID" == "null" ]] && RUN_ID="(see response above)"

log "Seed complete."
printf "\nSummary\n"
printf "  Job ID:        %s\n" "$JOB_ID"
printf "  Schedule ID:   %s\n" "$SCHED_ID"
printf "  Ad-hoc Run ID: %s\n" "$RUN_ID"
printf "\nNext:\n"
printf "  List jobs:     curl -s %s/v1/jobs | jq .\n" "$API_URL"
printf "  List runs:     curl -s %s/v1/jobs/%s/runs | jq .\n" "$API_URL" "$JOB_ID"
