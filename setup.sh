#!/usr/bin/env bash
set -euo pipefail

echo "============================================="
echo "Global Conflict Monitor - Guided Setup"
echo "============================================="
echo ""

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
JAR_DIR="flink/lib"
CDC_JAR="${JAR_DIR}/flink-sql-connector-postgres-cdc-2.4.2.jar"
PG_JAR="${JAR_DIR}/postgresql-42.7.1.jar"

info() { echo "$*"; }
warn() { echo "Warning: $*" >&2; }
err()  { echo "Error: $*" >&2; }

ts() { date '+%H:%M:%S'; }
step() { echo ""; echo "[$(ts)] == $* =="; }

prompt_yn() {
  local msg="$1" default_yes="${2:-1}" ans
  if [[ "$default_yes" == "1" ]]; then
    read -r -p "$msg [Y/n]: " ans || true
    ans="${ans:-Y}"
  else
    read -r -p "$msg [y/N]: " ans || true
    ans="${ans:-N}"
  fi
  case "${ans}" in
    y|Y|yes|YES) return 0 ;;
    *) return 1 ;;
  esac
}

need_cmd() { command -v "$1" >/dev/null 2>&1; }

os_detect() {
  local s
  s="$(uname -s 2>/dev/null || echo unknown)"
  case "$s" in
    Darwin) echo "mac" ;;
    Linux)  echo "linux" ;;
    CYGWIN*|MINGW*|MSYS*|Windows_NT) echo "windows" ;;
    *) echo "unknown" ;;
  esac
}

compose_cmd() {
  if need_cmd docker && docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return 0
  fi
  if need_cmd docker-compose; then
    echo "docker-compose"
    return 0
  fi
  return 1
}

download_if_missing() {
  local path="$1"
  local url="$2"
  local label="$3"

  [[ -f "$path" ]] && return 0

  need_cmd curl || { err "curl is not installed. Please install curl and re-run."; exit 1; }
  mkdir -p "$(dirname "$path")"

  info "Downloading ${label}..."
  curl -fL -o "${path}.tmp" "$url"
  mv "${path}.tmp" "$path"
}

ensure_mac() {
  if ! need_cmd brew; then
    err "Homebrew is not installed. Install it from https://brew.sh and re-run."
    exit 1
  fi

  if ! need_cmd colima; then
    info "Installing Colima..."
    brew install colima
  fi

  if ! need_cmd docker; then
    info "Installing Docker CLI..."
    brew install docker
  fi

  if ! compose_cmd >/dev/null 2>&1; then
    info "Installing Docker Compose..."
    brew install docker-compose || true
  fi

  if ! docker info >/dev/null 2>&1; then
    if ! colima status >/dev/null 2>&1; then
      info "Starting Colima..."
      colima start --cpu 4 --memory 8 --disk 50
    else
      info "Starting Colima (Docker daemon is not reachable yet)..."
      colima start --cpu 4 --memory 8 --disk 50 || true
    fi
  fi

  docker info >/dev/null 2>&1 || { err "Docker is still not reachable. Check Colima or Docker Desktop."; exit 1; }
}

ensure_linux() {
  if ! need_cmd docker; then
    err "Docker is not installed. Install Docker and Docker Compose, then re-run."
    err "Ubuntu/Debian: sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin"
    err "Fedora: sudo dnf install -y docker docker-compose-plugin"
    err "Arch: sudo pacman -S docker docker-compose"
    exit 1
  fi

  if ! docker info >/dev/null 2>&1; then
    err "Docker is installed but not usable (daemon not running or permissions issue)."
    err "Try: sudo systemctl start docker"
    err "Or add your user to the docker group and log out/in."
    exit 1
  fi
}

ensure_windows() {
  cat <<'EOF'
This project should be run from WSL2 on Windows.

Do this:
1) Install Docker Desktop.
2) Enable WSL2 and install Ubuntu.
3) In Docker Desktop settings:
   - Enable the WSL2 engine
   - Enable integration for Ubuntu
4) Open Ubuntu (WSL2) and run:
   ./setup.sh
EOF
  exit 1
}

wait_for_ready() {
  local max_seconds="${1:-120}"
  local start now
  start="$(date +%s)"

  while true; do
    # Postgres: if it has a docker healthcheck, require healthy; otherwise just require running
    local pg_ok=1
    if docker inspect -f '{{.State.Health.Status}}' gdelt-postgres >/dev/null 2>&1; then
      if docker ps --filter "name=gdelt-postgres" --filter "health=healthy" --format '{{.Names}}' | grep -q gdelt-postgres; then
        pg_ok=0
      fi
    else
      if docker ps --filter "name=gdelt-postgres" --format '{{.Names}}' | grep -q gdelt-postgres; then
        pg_ok=0
      fi
    fi

    # Flink Jobmanager: ready when the web ui responds
    local jm_ok=1
    if curl -fsS "http://localhost:8081/overview" >/dev/null 2>&1; then
      jm_ok=0
    fi

    if [[ "$pg_ok" -eq 0 && "$jm_ok" -eq 0 ]]; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start >= max_seconds )); then
      warn "Services did not become ready in time."
      warn "Check logs:"
      warn "  docker logs gdelt-postgres"
      warn "  docker logs flink-jobmanager"
      return 1
    fi

    echo -n "."
    sleep 2
  done
}

ensure_permissions() {
  chmod +x setup.sh 2>/dev/null || true
  chmod +x scripts/*.sh 2>/dev/null || true
  chmod -R a+r flink/lib 2>/dev/null || true
  mkdir -p scripts flink/lib 2>/dev/null || true
}

pg_exec() {
  local sql="$1"
  docker exec -i -e PGPASSWORD="${POSTGRES_PASSWORD:-flink_pass}" gdelt-postgres \
    psql -U "${POSTGRES_USER:-flink_user}" -d "${POSTGRES_DB:-gdelt}" -v ON_ERROR_STOP=1 -tAc "$sql"
}

flink_job_running() {
  local body
  body="$(curl -fsS "http://localhost:8081/jobs/overview" 2>/dev/null || true)"
  [[ -n "$body" ]] || return 1
  python3 - <<'PY' <<<"$body" >/dev/null 2>&1
import json,sys
d=json.loads(sys.stdin.read() or "{}")
jobs=d.get("jobs") or []
sys.exit(0 if any(j.get("state")=="RUNNING" for j in jobs) else 1)
PY
}

sink_tables_present() {
  local t
  for t in daily_event_volume_by_quadclass dyad_interactions top_actors daily_cameo_metrics; do
    [[ "$(pg_exec "select to_regclass('public.${t}') is not null;")" == "t" ]] || return 1
  done
  return 0
}

light_sanity_checks() {
  step "quick sanity checks (lightweight)"
  echo "[check] docker containers"
  docker ps --format '{{.Names}}' | grep -qx gdelt-postgres || { err "gdelt-postgres not running"; return 1; }
  docker ps --format '{{.Names}}' | grep -qx flink-jobmanager || { err "flink-jobmanager not running"; return 1; }
  docker ps --format '{{.Names}}' | grep -qx flink-taskmanager || { err "flink-taskmanager not running"; return 1; }
  echo "[ok] containers running"

  echo "[check] flink api"
  curl -fsS "http://localhost:8081/overview" >/dev/null
  echo "[ok] flink api responding"

  echo "[check] postgres reachable"
  pg_exec "select 1;" >/dev/null
  echo "[ok] postgres reachable"

  echo "[check] sink tables"
  sink_tables_present && echo "[ok] sink tables present" || echo "[warn] sink tables missing (pipeline might not be started yet)"

  echo "[check] flink job"
  if flink_job_running; then
    echo "[ok] flink job running"
  else
    echo "[warn] no running flink job found"
  fi
}

progress_bar() {
  # usage: progress_bar current total
  local cur="$1" total="$2" width=40
  if (( total <= 0 )); then total=1; fi
  local pct=$(( cur * 100 / total ))
  if (( pct > 100 )); then pct=100; fi
  local filled=$(( pct * width / 100 ))
  local empty=$(( width - filled ))
  printf "\r["; printf "%0.s#" $(seq 1 "$filled"); printf "%0.s-" $(seq 1 "$empty");
  printf "] %3d%% (%d/%d)" "$pct" "$cur" "$total"
}

run_small_load_with_progress() {
  local file_path="$1"
  local target_rows="${2:-500000}"

  step "loading a small dataset (${target_rows} rows)"
  echo "[info] file: $file_path"
  echo "[info] this uses SMALL_LOAD_LINES=${target_rows} (fast demo load)"

  # run loader in background
  ( SMALL_LOAD_LINES="$target_rows" ./scripts/setup/load-gdelt-copy.sh "$file_path" ) &
  local loader_pid=$!

  # poll row count and render a simple progress bar
  local cur=0
  while kill -0 "$loader_pid" >/dev/null 2>&1; do
    cur="$(pg_exec "select count(*) from public.gdelt_events;" 2>/dev/null || echo 0)"
    cur="${cur//[^0-9]/}"
    cur="${cur:-0}"
    progress_bar "$cur" "$target_rows"
    sleep 2
  done

  wait "$loader_pid"
  cur="$(pg_exec "select count(*) from public.gdelt_events;" 2>/dev/null || echo 0)"
  cur="${cur//[^0-9]/}"
  cur="${cur:-0}"
  progress_bar "$cur" "$target_rows"
  echo ""
  echo "[ok] load done"
}

ensure_python_deps() {
  if command -v streamlit >/dev/null 2>&1; then
    return 0
  fi
  step "python deps"
  warn "streamlit not found in your shell. to run the dashboard we need python deps."
  if prompt_yn "install python deps with: python3 -m pip install -r requirements.txt ?" 1; then
    python3 -m pip install -r requirements.txt
  else
    warn "skipping python deps install. you can install later and run: streamlit run app.py"
  fi
}

start_streamlit() {
  step "starting streamlit"
  ensure_python_deps || true
  if ! command -v streamlit >/dev/null 2>&1; then
    warn "streamlit still not available. skipping auto-start."
    return 0
  fi
  mkdir -p logs
  if command -v lsof >/dev/null 2>&1 && lsof -i :8501 >/dev/null 2>&1; then
    warn "port 8501 is already in use. streamlit might already be running."
    return 0
  fi
  echo "[run] streamlit run app.py --server.port 8501"
  nohup streamlit run app.py --server.port 8501 --server.headless true > logs/streamlit.log 2>&1 &
  echo "[ok] streamlit started: http://localhost:8501"
  echo "[log] logs/streamlit.log"
}

main() {
  [[ -f "$COMPOSE_FILE" ]] || { err "Cannot find ${COMPOSE_FILE} in this directory."; exit 1; }

  local os
  os="$(os_detect)"

  case "$os" in
    mac) ensure_mac ;;
    linux) ensure_linux ;;
    windows) ensure_windows ;;
    *) err "Unsupported OS: $(uname -a)"; exit 1 ;;
  esac

  local compose
  compose="$(compose_cmd)" || { err "Docker Compose is missing. Install Docker Compose or the Docker Compose plugin."; exit 1; }

  download_if_missing "$CDC_JAR" \
    "https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.4.2/flink-sql-connector-postgres-cdc-2.4.2.jar" \
    "the Flink PostgreSQL CDC connector JAR"

  download_if_missing "$PG_JAR" \
    "https://jdbc.postgresql.org/download/postgresql-42.7.1.jar" \
    "the PostgreSQL JDBC driver JAR"

  ensure_permissions

  step "starting services (docker compose)"
  if [[ "$compose" == "docker compose" ]]; then
    docker compose -f "$COMPOSE_FILE" up -d
  else
    docker-compose -f "$COMPOSE_FILE" up -d
  fi

  echo "[wait] waiting for postgres + flink jobmanager to be ready"
  wait_for_ready 180 || true
  echo ""

  # decide if this is a fresh setup or a resume
  local is_ready=0
  if sink_tables_present && flink_job_running; then
    is_ready=1
  fi

  if (( is_ready == 1 )); then
    step "looks already set up"
    echo "[info] running lightweight checks (no heavy tests by default)"
    light_sanity_checks || true

    if prompt_yn "run full tests anyway (phase1 + phase2)?" 0; then
      step "phase1 test"
      ./scripts/tests/phase1-infra-test.sh
      step "phase2 test"
      ./scripts/tests/phase2-cdc-test.sh
    fi
  else
    step "fresh setup / resume setup"

    if prompt_yn "run phase1 infra test now (recommended)?" 1; then
      step "phase1 test"
      ./scripts/tests/phase1-infra-test.sh
    else
      warn "skipping phase1 test"
    fi

    # load a small dataset
    local default_file="data/GDELT.MASTERREDUCEDV2.TXT"
    local file_path="${GDELT_FILE:-$default_file}"

    if [[ ! -f "$file_path" ]]; then
      echo ""
      warn "could not find $file_path"
      read -r -p "path to the GDELT file to load (tab-separated): " file_path
    fi

    run_small_load_with_progress "$file_path" 500000

    step "starting flink aggregations"
    ./scripts/setup/start-flink-aggregations.sh

    if prompt_yn "run phase2 cdc test now (recommended)?" 1; then
      step "phase2 test"
      ./scripts/tests/phase2-cdc-test.sh
    else
      warn "skipping phase2 test"
    fi
  fi

  start_streamlit || true

  step "done"
  echo "flink ui:       http://localhost:8081"
  echo "streamlit app:  http://localhost:8501"
  echo "postgres shell: docker exec -it gdelt-postgres psql -U flink_user -d gdelt"
}

main "$@"