#!/usr/bin/env bash
set -euo pipefail

echo "=== system health check ==="
echo ""

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
POSTGRES_DB="${POSTGRES_DB:-gdelt}"
POSTGRES_USER="${POSTGRES_USER:-flink_user}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-flink_pass}"

FLINK_BASE="${FLINK_BASE:-http://localhost:8081}"

if command -v colima >/dev/null 2>&1; then
  echo "1) colima status:"
  colima status | head -3 || true
  echo ""
fi

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

echo "2) containers:"
$COMPOSE_CMD ps || true
echo ""

echo "3) postgres row count (gdelt_events):"
docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" "$POSTGRES_CONTAINER" \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "select count(*) from public.gdelt_events;" || true
echo ""

echo "4) flink api:"
curl -fsS "$FLINK_BASE/overview" | grep -o '"taskmanagers":[0-9]*' || echo "flink not accessible"
echo ""

echo "=== end check ==="
