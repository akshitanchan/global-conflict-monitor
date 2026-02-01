#!/usr/bin/env bash
set -euo pipefail

# load gdelt masterreducedv2 into public.gdelt_events
# stream + sanitize into \copy

FILE_PATH="${1:-data/GDELT.MASTERREDUCEDV2.TXT}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
DB="${POSTGRES_DB:-gdelt}"
USER="${POSTGRES_USER:-flink_user}"

# set for quick test runs
SMALL_LOAD_LINES="${SMALL_LOAD_LINES:-}"

if [[ ! -f "$FILE_PATH" ]]; then
  echo "error: file not found: $FILE_PATH" >&2
  exit 1
fi

base="$(basename "$FILE_PATH")"
container_file="/data/$base"

echo "[load] loading $FILE_PATH into gdelt_events"
echo "[load] container path: $container_file"
echo "[load] postgres container: $POSTGRES_CONTAINER"

# start clean
echo "[load] truncating gdelt_events..."
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 \
  -c "truncate table public.gdelt_events;"

# sniff 11 vs 17 cols
SAMPLE_LINES="${SAMPLE_LINES:-20000}"

echo "[load] detecting column count from first ${SAMPLE_LINES} data lines..."
detect_out="$(docker exec -i "$POSTGRES_CONTAINER" bash -lc "
FILE='$container_file';
tail -n +2 \"\$FILE\" | head -n ${SAMPLE_LINES} | awk -F \"\t\" '
  NF==17 {c17++}
  NF==11 {c11++}
  END {printf(\"c17=%d c11=%d\\n\", c17+0, c11+0)}
'
")"

echo "[load] sample counts: $detect_out"

c17="$(echo "$detect_out" | sed -n 's/.*c17=\([0-9]\+\).*/\1/p')"
c11="$(echo "$detect_out" | sed -n 's/.*c11=\([0-9]\+\).*/\1/p')"

expected_nf=17
cols_clause="event_date,source_actor,target_actor,cameo_code,num_events,num_articles,quad_class,goldstein,source_geo_type,source_geo_lat,source_geo_long,target_geo_type,target_geo_lat,target_geo_long,action_geo_type,action_geo_lat,action_geo_long"

# pick dominant schema
if [[ "${c11:-0}" -gt "${c17:-0}" ]]; then
  expected_nf=11
  cols_clause="event_date,source_actor,target_actor,cameo_code,num_events,num_articles,quad_class,goldstein,action_geo_type,action_geo_lat,action_geo_long"
fi

echo "[load] using expected_nf=$expected_nf"

# build the stream
STREAM_CMD="tail -n +2 \"$container_file\""
if [[ -n "$SMALL_LOAD_LINES" ]]; then
  echo "[load] small-load mode enabled: first $SMALL_LOAD_LINES data lines"
  STREAM_CMD="$STREAM_CMD | head -n $SMALL_LOAD_LINES"
fi

# strip bad bytes
STREAM_CMD="$STREAM_CMD | tr -d '\000' | tr -d '\r'"

# normalize field count
if [[ "$expected_nf" -eq 17 ]]; then
  STREAM_CMD="$STREAM_CMD | awk -F \"\t\" 'BEGIN{OFS=\"\t\"}
    NF==17 { print; next }
    NF==11 {
      a_type=\$9; a_lat=\$10; a_long=\$11;
      \$9=\"\"; \$10=\"\"; \$11=\"\";
      \$12=\"\"; \$13=\"\"; \$14=\"\";
      \$15=a_type; \$16=a_lat; \$17=a_long;
      print; next
    }
    { next }
  '"
else
  STREAM_CMD="$STREAM_CMD | awk -F \"\t\" 'BEGIN{OFS=\"\t\"}
    NF==11 { print; next }
    NF==17 { print \$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8,\$15,\$16,\$17; next }
    { next }
  '"
fi

# bulk load via stdin
echo "[load] streaming sanitized rows into \\copy FROM STDIN..."
docker exec -i "$POSTGRES_CONTAINER" bash -lc "$STREAM_CMD" \
  | docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -c "\
\\copy public.gdelt_events($cols_clause) \
FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '', ENCODING 'UTF8');"

# quick sanity check
echo "[ok] load complete. row count:"
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -t -c "select count(*) from public.gdelt_events;"