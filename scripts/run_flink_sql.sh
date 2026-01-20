#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"

# default host sql dir (overrideable)
SQL_DIR_HOST="${SQL_DIR_HOST:-$ROOT_DIR/flink/sql}"
SQL_DIR_CONTAINER="${SQL_DIR_CONTAINER:-/opt/flink/sql}"

# safety: skip sql files that have streaming SELECT statements
SKIP_STREAMING_SELECTS="${SKIP_STREAMING_SELECTS:-1}"

if command -v docker-compose >/dev/null 2>&1; then
  COMPOSE_CMD="docker-compose"
else
  COMPOSE_CMD="docker compose"
fi

if [[ ! -d "$SQL_DIR_HOST" ]]; then
  echo "[error] SQL_DIR_HOST not found: $SQL_DIR_HOST"
  exit 1
fi

echo "[up] docker compose up -d"
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

echo "[check] sql directory is mounted in flink container: $SQL_DIR_CONTAINER"
docker exec -i "$FLINK_JM_CONTAINER" bash -lc "test -d '$SQL_DIR_CONTAINER' || exit 1"

echo "[run] applying .sql files from: $SQL_DIR_HOST"
shopt -s nullglob
files=( "$SQL_DIR_HOST"/*.sql )
if (( ${#files[@]} == 0 )); then
  echo "[warn] no .sql files found in $SQL_DIR_HOST"
  exit 0
fi

for f in "${files[@]}"; do
  base="$(basename "$f")"
  container_path="$SQL_DIR_CONTAINER/$base"

  if [[ "$SKIP_STREAMING_SELECTS" == "1" ]]; then
    if grep -Eiq '^\s*select\b' "$f"; then
      echo "[skip] $base (contains top-level SELECT; likely interactive/streaming)"
      continue
    fi
  fi

  echo "[apply] $base"
  docker exec -i "$FLINK_JM_CONTAINER" bash -lc \
    "/opt/flink/bin/sql-client.sh -f '$container_path' >/tmp/sql_apply_${base}.log 2>&1 || (tail -n 200 /tmp/sql_apply_${base}.log && exit 1)"
done

echo "[ok] all sql files applied"