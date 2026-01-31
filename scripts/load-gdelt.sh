#!/usr/bin/env bash
set -euo pipefail

# Append GDELT MASTERREDUCEDV2 data into public.gdelt_events.
# - NO TRUNCATE
# - uses current row count as the offset unless SMALL_LOAD_OFFSET is provided
# - IMPORTANT FIXES:
#   (1) offset is COUNT(*) (line offset), NOT MAX(globaleventid)
#   (2) sync globaleventid sequence to MAX(globaleventid) before COPY (prevents duplicates)

FILE_PATH="${1:-data/GDELT.MASTERREDUCEDV2.TXT}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
DB="${POSTGRES_DB:-gdelt}"
USER="${POSTGRES_USER:-flink_user}"

SMALL_LOAD_LINES="${SMALL_LOAD_LINES:-}"
SMALL_LOAD_OFFSET="${SMALL_LOAD_OFFSET:-}"

if [[ ! -f "$FILE_PATH" ]]; then
  echo "error: file not found: $FILE_PATH" >&2
  exit 1
fi

if [[ -z "$SMALL_LOAD_LINES" ]]; then
  echo "error: set SMALL_LOAD_LINES (batch size), e.g. SMALL_LOAD_LINES=20000" >&2
  exit 2
fi

base="$(basename "$FILE_PATH")"
container_file="/data/$base"

echo "[append] file: $FILE_PATH"
echo "[append] container path: $container_file"
echo "[append] postgres container: $POSTGRES_CONTAINER"

# ---- FIX #1: offset must be "how many rows already loaded", i.e., COUNT(*)
if [[ -z "$SMALL_LOAD_OFFSET" ]]; then
  echo "[append] computing offset from current row COUNT(*) in public.gdelt_events..."
  SMALL_LOAD_OFFSET="$(docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -t -A -c \
    "SELECT COUNT(*) FROM public.gdelt_events;")"
  SMALL_LOAD_OFFSET="${SMALL_LOAD_OFFSET//$'\r'/}"
  SMALL_LOAD_OFFSET="${SMALL_LOAD_OFFSET//$'\n'/}"
fi

if ! [[ "$SMALL_LOAD_OFFSET" =~ ^[0-9]+$ ]]; then
  echo "error: SMALL_LOAD_OFFSET must be an integer (got: $SMALL_LOAD_OFFSET)" >&2
  exit 2
fi

echo "[append] lines=$SMALL_LOAD_LINES  offset=$SMALL_LOAD_OFFSET"

# ---- FIX #2: sync the sequence so auto-assigned globaleventid never reuses old IDs
# If globaleventid is SERIAL/BIGSERIAL, this prevents duplicate key errors.
echo "[append] syncing globaleventid sequence to MAX(globaleventid)..."
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -t -A -c "
DO \$\$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = 'gdelt_events_globaleventid_seq'
      AND n.nspname = 'public'
  ) THEN
    PERFORM setval(
      'public.gdelt_events_globaleventid_seq',
      GREATEST((SELECT COALESCE(MAX(globaleventid), 1) FROM public.gdelt_events), 1),
      true
    );
  END IF;
END
\$\$;
" >/dev/null

SAMPLE_LINES="${SAMPLE_LINES:-20000}"

echo "[append] detecting column count from first ${SAMPLE_LINES} data lines..."
detect_out="$(docker exec -i "$POSTGRES_CONTAINER" bash -lc "
FILE='$container_file';
tail -n +2 \"\$FILE\" | head -n ${SAMPLE_LINES} | awk -F \"\t\" '
  NF==17 {c17++}
  NF==11 {c11++}
  END {printf(\"c17=%d c11=%d\\n\", c17+0, c11+0)}
'
")"

echo "[append] sample counts: $detect_out"

c17="$(echo "$detect_out" | sed -n 's/.*c17=\([0-9]\+\).*/\1/p')"
c11="$(echo "$detect_out" | sed -n 's/.*c11=\([0-9]\+\).*/\1/p')"

expected_nf=17
cols_clause="event_date,source_actor,target_actor,cameo_code,num_events,num_articles,quad_class,goldstein,source_geo_type,source_geo_lat,source_geo_long,target_geo_type,target_geo_lat,target_geo_long,action_geo_type,action_geo_lat,action_geo_long"

if [[ "${c11:-0}" -gt "${c17:-0}" ]]; then
  expected_nf=11
  cols_clause="event_date,source_actor,target_actor,cameo_code,num_events,num_articles,quad_class,goldstein,action_geo_type,action_geo_lat,action_geo_long"
fi

echo "[append] using expected_nf=$expected_nf"

# Header is line 1, so data starts at line 2.
# If we already loaded N rows, next row is at data line (2 + N).
start_line=$((2 + SMALL_LOAD_OFFSET))

STREAM_CMD="tail -n +${start_line} \"$container_file\" | head -n ${SMALL_LOAD_LINES}"
STREAM_CMD="$STREAM_CMD | tr -d '\\000' | tr -d '\\r'"

if [[ "$expected_nf" -eq 17 ]]; then
  STREAM_CMD="$STREAM_CMD | awk -F \"\\t\" 'BEGIN{OFS=\"\\t\"}
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
  STREAM_CMD="$STREAM_CMD | awk -F \"\\t\" 'BEGIN{OFS=\"\\t\"}
    NF==11 { print; next }
    NF==17 { print \$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8,\$15,\$16,\$17; next }
    { next }
  '"
fi

echo "[append] streaming sanitized rows into \\copy FROM STDIN..."
docker exec -i "$POSTGRES_CONTAINER" bash -lc "$STREAM_CMD" \
  | docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -c "\
\\copy public.gdelt_events($cols_clause) \
FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '', ENCODING 'UTF8');"

echo "[append] done. row count now:"
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -t -A -c "SELECT COUNT(*) FROM public.gdelt_events;"