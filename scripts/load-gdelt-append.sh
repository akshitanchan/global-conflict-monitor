#!/usr/bin/env bash

set -euo pipefail

# Append GDELT MASTERREDUCEDV2 data into the gdelt_events table (NO TRUNCATE, NO CONSTRAINTS).
# - this file is tab-separated with a header.
# - the file has a tiny number of malformed rows (wrong field count / bad bytes).
# - we stream sanitized rows into \copy FROM STDIN to avoid fragile "FROM PROGRAM" parsing.
# - FAST MODE: Direct insert without duplicate checking.

FILE_PATH="${1:-data/GDELT.MASTERREDUCEDV2.TXT}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-gdelt-postgres}"
DB="${POSTGRES_DB:-gdelt}"
USER="${POSTGRES_USER:-flink_user}"

# set SMALL_LOAD_LINES to test quickly (e.g. 200000). leave unset for full load.
SMALL_LOAD_LINES="${SMALL_LOAD_LINES:-}"

if [[ ! -f "$FILE_PATH" ]]; then
    echo "error: file not found: $FILE_PATH" >&2
    exit 1
fi

base="$(basename "$FILE_PATH")"
container_file="/data/$base"

echo "[append] appending $FILE_PATH into gdelt_events (FAST MODE - NO TRUNCATE)"
echo "[append] container path: $container_file"
echo "[append] postgres container: $POSTGRES_CONTAINER"

# detect dominant column count from the first N data lines (skip header)
SAMPLE_LINES="${SAMPLE_LINES:-20000}"
echo "[append] detecting column count from first ${SAMPLE_LINES} data lines..."

detect_out="$(docker exec -i "$POSTGRES_CONTAINER" bash -lc "
FILE='$container_file';
tail -n +2 \"\$FILE\" | head -n ${SAMPLE_LINES} | awk -F \"	\" '
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

STREAM_CMD="tail -n +2 \"$container_file\""

if [[ -n "$SMALL_LOAD_LINES" ]]; then
    echo "[append] small-load mode enabled: first $SMALL_LOAD_LINES data lines"
    STREAM_CMD="$STREAM_CMD | head -n $SMALL_LOAD_LINES"
fi

STREAM_CMD="$STREAM_CMD | tr -d '\000' | tr -d '\r'"

if [[ "$expected_nf" -eq 17 ]]; then
    # output exactly 17 cols; if we read an 11-col row, map its action geo (9..11) into 15..17
    STREAM_CMD="$STREAM_CMD | awk -F \"	\" 'BEGIN{OFS=\"	\"}
NF==17 { print; next }
NF==11 {
    a_type=\$9; a_lat=\$10; a_long=\$11;
    # clear source + target geo
    \$9=\"\"; \$10=\"\"; \$11=\"\";
    \$12=\"\"; \$13=\"\"; \$14=\"\";
    # put action geo in the right place
    \$15=a_type; \$16=a_lat; \$17=a_long;
    print; next
}
{ next }
'"
else
    # expected_nf == 11: output exactly 11 cols; if we read a 17-col row, take action geo (15..17)
    STREAM_CMD="$STREAM_CMD | awk -F \"	\" 'BEGIN{OFS=\"	\"}
NF==11 { print; next }
NF==17 { print \$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8,\$15,\$16,\$17; next }
{ next }
'"
fi

echo "[append] streaming sanitized rows directly into \\copy FROM STDIN..."
# stream -> psql \copy from stdin (psql runs inside container) - DIRECT INSERT
docker exec -i "$POSTGRES_CONTAINER" bash -lc "$STREAM_CMD" \
| docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -v ON_ERROR_STOP=1 -c "\
\\copy public.gdelt_events($cols_clause) \
FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '', ENCODING 'UTF8');"

echo "[ok] append complete. total row count:"
docker exec -i "$POSTGRES_CONTAINER" psql -U "$USER" -d "$DB" -t -c "select count(*) from public.gdelt_events;"
