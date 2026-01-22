#!/usr/bin/env bash
set -euo pipefail

# fast loader for tab-separated (header) GDELT MASTERREDUCED-style files
# - this uses psql \copy so the file path is inside the container
# - docker-compose mounts ./data -> /data (read-only)

FILE_PATH="${1:-data/GDELT.MASTERREDUCEDV2.TXT}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
DB="${POSTGRES_DB:-gdelt}"
USER="${POSTGRES_USER:-flink_user}"

if [[ ! -f "$FILE_PATH" ]]; then
  echo "error: file not found: $FILE_PATH" >&2
  exit 1
fi

# convert host path -> container path (assumes ./data mount)
# if you pass an absolute path outside ./data, copy it into ./data first.
base="$(basename "$FILE_PATH")"
container_file="/data/$base"

echo "[load] loading $FILE_PATH into gdelt_events via \copy"
echo "[load] container path: $container_file"

docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 <<SQL
\echo '[load] detected 11-column file, loading core + action geo only'
\copy public.gdelt_events(
  event_date,
  source_actor,
  target_actor,
  cameo_code,
  num_events,
  num_articles,
  quad_class,
  goldstein,
  action_geo_type,
  action_geo_lat,
  action_geo_long
)
FROM PROGRAM 'tail -n +2 "$container_file" | head -n 200000 | awk -F "\t" '\''NF==17'\'''
WITH (
  FORMAT text,
  DELIMITER E'\t',
  NULL '',
  ENCODING 'UTF8'
);
SQL

echo "[ok] load complete. row count:"
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -t -c "select count(*) from public.gdelt_events;"
