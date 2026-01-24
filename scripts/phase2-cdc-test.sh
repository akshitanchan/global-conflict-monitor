#!/usr/bin/env bash
set -euo pipefail

ok()   { echo "[phase2] ✅ $*"; }
fail() { echo "[phase2] ❌ $*" >&2; exit 1; }

PSQL="docker exec -i gdelt-postgres psql -U flink_user -d gdelt -v ON_ERROR_STOP=1 -tA"
FLINK_BASE="${FLINK_BASE:-http://127.0.0.1:8081}"

wait_for_json() {
  local url="$1" tries="${2:-40}" sleep_s="${3:-2}" body=""
  for _ in $(seq 1 "$tries"); do
    body="$(curl -4 -fsS --max-time 2 "$url" 2>/dev/null || true)"
    if [[ -n "$body" ]] && python3 -c 'import json,sys; json.loads(sys.stdin.read())' <<<"$body" >/dev/null 2>&1; then
      printf '%s' "$body"
      return 0
    fi
    sleep "$sleep_s"
  done
  return 1
}

# 0) containers up?
docker ps --format '{{.Names}}' | grep -qx 'gdelt-postgres' || fail "gdelt-postgres container not running"
docker ps --format '{{.Names}}' | grep -qx 'flink-jobmanager' || fail "flink-jobmanager container not running"
docker ps --format '{{.Names}}' | grep -qx 'flink-taskmanager' || fail "flink-taskmanager container not running"
ok "containers running"

# 1) postgres physical sink tables exist?
missing=()
for t in daily_event_volume_by_quadclass dyad_interactions top_actors daily_cameo_metrics; do
  exists="$($PSQL -c "select to_regclass('public.${t}') is not null;")"
  [[ "$exists" == "t" ]] || missing+=("$t")
done
((${#missing[@]}==0)) || fail "missing postgres sink tables: ${missing[*]}"
ok "postgres sink tables present"

# 2) flink api + job running?
_="$(wait_for_json "$FLINK_BASE/overview" 40 2)" || fail "flink api not responding at $FLINK_BASE"

jobs_overview="$(wait_for_json "$FLINK_BASE/jobs/overview" 40 2)" || fail "flink /jobs/overview not returning json"

job_state="$(python3 -c '
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
print(jobs[0].get("state","NO_STATE") if jobs else "NO_JOBS")
' <<<"$jobs_overview")"

job_jid="$(python3 -c '
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
print(jobs[0].get("jid","") if jobs else "")
' <<<"$jobs_overview")"

if [[ "$job_state" == "NO_JOBS" ]]; then
  fail "no flink jobs running. start the pipeline first:
docker exec -it flink-jobmanager ./bin/sql-client.sh -f /opt/flink/sql/00-run-pipeline.sql"
fi

[[ "$job_state" == "RUNNING" ]] || fail "flink job not RUNNING (state=$job_state)"
ok "flink job is RUNNING (jid=$job_jid)"

# 3) replication active?
rep="$($PSQL -c "select count(*) from pg_stat_replication where state='streaming';")"
[[ "$rep" != "0" ]] || fail "no streaming replication sessions (pg_stat_replication)"
ok "postgres logical replication streaming"

# 4) cdc effect test: insert 3 rows and expect sink totals to increase
key="$($PSQL -c "select event_date||','||quad_class from gdelt_events where event_date is not null and quad_class is not null limit 1;")"
[[ -n "$key" ]] || fail "gdelt_events seems empty"
event_date="${key%,*}"
quad_class="${key#*,}"
ok "using key event_date=$event_date quad_class=$quad_class"

baseline="$($PSQL -c "select coalesce(total_events,0)||','||coalesce(total_articles,0)
from daily_event_volume_by_quadclass
where event_date=${event_date} and quad_class=${quad_class};")"
if [[ -z "$baseline" ]]; then
  base_events=0
  base_articles=0
else
  base_events="${baseline%,*}"
  base_articles="${baseline#*,}"
fi

ids="$($PSQL -c "
with template as (
  select
    event_date,source_actor,target_actor,cameo_code,quad_class,goldstein,
    source_geo_type,source_geo_lat,source_geo_long,
    target_geo_type,target_geo_lat,target_geo_long,
    action_geo_type,action_geo_lat,action_geo_long
  from gdelt_events
  where event_date=${event_date} and quad_class=${quad_class}
  limit 1
),
base as (
  select coalesce(max(globaleventid),0) as max_id from gdelt_events
),
ins as (
  insert into gdelt_events (
    globaleventid,event_date,source_actor,target_actor,cameo_code,
    num_events,num_articles,quad_class,goldstein,
    source_geo_type,source_geo_lat,source_geo_long,
    target_geo_type,target_geo_lat,target_geo_long,
    action_geo_type,action_geo_lat,action_geo_long
  )
  select
    base.max_id + gs.g,
    template.event_date,
    template.source_actor,
    template.target_actor,
    template.cameo_code,
    1,1,
    template.quad_class,
    template.goldstein,
    template.source_geo_type,
    template.source_geo_lat,
    template.source_geo_long,
    template.target_geo_type,
    template.target_geo_lat,
    template.target_geo_long,
    template.action_geo_type,
    template.action_geo_lat,
    template.action_geo_long
  from base
  cross join template
  cross join generate_series(1,3) as gs(g)
  returning globaleventid
)
select array_to_string(array_agg(globaleventid), ',') from ins;
")"
[[ -n "$ids" ]] || fail "failed to insert test rows"
ok "inserted test rows ids=$ids"

target_events=$((base_events + 3))
target_articles=$((base_articles + 3))

deadline=$((SECONDS + 60))
while true; do
  cur="$($PSQL -c "select total_events||','||total_articles
  from daily_event_volume_by_quadclass
  where event_date=${event_date} and quad_class=${quad_class};")"

  if [[ -n "$cur" ]]; then
    cur_events="${cur%,*}"
    cur_articles="${cur#*,}"
    if [[ "$cur_events" == "$target_events" && "$cur_articles" == "$target_articles" ]]; then
      ok "cdc verified: sink updated (events $base_events->$cur_events, articles $base_articles->$cur_articles)"
      break
    fi
  fi

  if (( SECONDS > deadline )); then
    fail "cdc verification timed out: expected events=$target_events articles=$target_articles, got '$cur'"
  fi
  sleep 2
done

# cleanup
$PSQL -c "delete from gdelt_events where globaleventid = any(string_to_array('${ids}',',')::bigint[]);" >/dev/null
ok "cleanup done"

ok "phase 2 looks healthy"