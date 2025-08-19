#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:8080}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing $1" >&2; exit 1; }; }
need curl

echo "==> Ensure API ready"
for i in {1..60}; do
  code="$(curl -s -o /dev/null -w '%{http_code}' "${API_URL}/healthz" || true)"
  [[ "$code" == "200" ]] && break
  sleep 1
done

echo "==> Create intentionally failing shell job"
CREATE_JOB_PAYLOAD='{
  "name": "fail-shell",
  "type": "batch",
  "handler": "shell",
  "args": { "command": "bash -c \"exit 42\"" },
  "enabled": true
}'
JOB_RESP="$(curl -sS -X POST "${API_URL}/v1/jobs" -H "Content-Type: application/json" -d "${CREATE_JOB_PAYLOAD}")"
echo "$JOB_RESP"
JOB_ID="$(echo "$JOB_RESP" | jq -r '.job.id' 2>/dev/null || echo "")"
[[ -z "$JOB_ID" || "$JOB_ID" == "null" ]] && { echo "Could not parse failing job id"; exit 1; }
echo "JOB_ID=${JOB_ID}"

echo "==> Trigger ad-hoc run (will fail -> retry -> dlq after backoffs)"
RUN_RESP="$(curl -sS -X POST "${API_URL}/v1/jobs/${JOB_ID}:run" -H "Content-Type: application/json" -d '{}')"
echo "$RUN_RESP"

echo "==> Tail worker logs for ~20s to observe retries"
docker compose logs -f --since=1m worker & pid=$!
sleep 20
kill $pid || true

echo "==> Admin CLI (lag, pending, requeue-dlq)"
# Notes:
# - your admin Dockerfile ENTRYPOINT is "/admin"
# - use the containerized CLI so it reaches Redis/Postgres on the compose network
docker compose run --rm admin /admin lag || true
docker compose run --rm admin /admin pending || true
docker compose run --rm admin /admin requeue-dlq --max 50 || true

echo "==> List runs for failing job"
curl -sS "${API_URL}/v1/jobs/${JOB_ID}/runs" | (jq . 2>/dev/null || cat)
