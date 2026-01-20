#!/usr/bin/env bash
set -euo pipefail

echo "======================================"
echo "GLOBAL CONFLICT MONITOR - Quick Setup"
echo "======================================"
echo ""

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
JAR_DIR="flink/lib"
CDC_JAR="${JAR_DIR}/flink-sql-connector-postgres-cdc-2.4.2.jar"
PG_JAR="${JAR_DIR}/postgresql-42.7.1.jar"

info() { echo "$*"; }
warn() { echo "Warning: $*" >&2; }
err()  { echo "Error: $*" >&2; }

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

    sleep 2
  done
}

ensure_permissions() {
  chmod +x setup.sh 2>/dev/null || true
  chmod +x scripts/*.sh 2>/dev/null || true
  chmod -R a+r flink/lib 2>/dev/null || true
  mkdir -p scripts flink/lib 2>/dev/null || true
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

  info "Starting services..."
  if [[ "$compose" == "docker compose" ]]; then
    docker compose -f "$COMPOSE_FILE" up -d >/dev/null 2>&1
  else
    docker-compose -f "$COMPOSE_FILE" up -d >/dev/null 2>&1
  fi

  wait_for_ready 120 || true

  info "Setup complete."
  info "Flink UI: http://localhost:8081"
  info "PostgreSQL shell: docker exec -it gdelt-postgres psql -U flink_user -d gdelt"
  info "Flink SQL client: docker exec -it flink-jobmanager ./bin/sql-client.sh"
  info "Recommended check: ./scripts/phase1-infra-test.sh"
}

main "$@"