#!/usr/bin/env bash
set -euo pipefail

echo "=== fixing replication slot / cdc issues ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

FLINK_BASE="${FLINK_BASE:-http://localhost:8081}"

ok()   { echo "[ok] $*"; }
warn() { echo "[warn] $*" >&2; }

cancel_flink_jobs() {
  local overview
  overview="$(curl -fsS "$FLINK_BASE/jobs/overview" 2>/dev/null || true)"
  [[ -n "$overview" ]] || { warn "flink rest api not reachable at $FLINK_BASE (skipping job cancellation)"; return 0; }

  local jids
  jids="$(python3 - <<'PY' <<<"$overview" 2>/dev/null || true
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
for j in jobs:
    jid=j.get("jid")
    state=j.get("state")
    if jid and state in {"RUNNING","CREATED","RESTARTING","FAILING"}:
        print(jid)
PY
)"

  if [[ -z "$jids" ]]; then
    ok "no running flink jobs found"
    return 0
  fi

  echo "[step] cancelling flink jobs"
  while read -r jid; do
    [[ -n "$jid" ]] || continue
    echo "  - cancelling $jid"
    curl -fsS -X PATCH "$FLINK_BASE/jobs/${jid}?mode=cancel" >/dev/null 2>&1 || true
  done <<<"$jids"
}

echo "1) cancelling flink jobs (best-effort)"
cancel_flink_jobs

echo "2) stopping flink containers"
$COMPOSE_CMD -f "$COMPOSE_FILE" stop jobmanager taskmanager >/dev/null 2>&1 || true

echo "3) resetting cdc (drop slot + publication)"
"$ROOT_DIR/scripts/fixes/reset-cdc.sh" >/dev/null
ok "cdc reset complete"

echo "4) restarting services"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d >/dev/null

echo "5) starting flink pipeline"
"$ROOT_DIR/scripts/setup/start-flink-aggregations.sh"

echo ""
echo "6) status"
if curl -fsS "$FLINK_BASE/jobs/overview" >/dev/null 2>&1; then
  running="$(curl -fsS "$FLINK_BASE/jobs/overview" | python3 - <<'PY'
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
print(sum(1 for j in jobs if j.get("state")=="RUNNING"))
PY
)"
  echo "running jobs: ${running} (expected: >= 1)"
else
  warn "flink rest api not reachable at $FLINK_BASE"
fi

echo ""
echo "open flink ui: http://localhost:8081"