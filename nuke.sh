#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

YES=0
PURGE_DATA=0

usage() {
  cat <<'USAGE'
usage: ./nuke.sh [--yes] [--purge-data]

--yes         skip confirmation prompts
--purge-data  also delete files under ./data (keeps the folder)

examples:
  ./nuke.sh
  ./nuke.sh --yes
  ./nuke.sh --yes --purge-data
USAGE
}

say() { printf "[%s] %s\n" "$(date +"%H:%M:%S")" "$*"; }
warn() { printf "[%s] %s\n" "$(date +"%H:%M:%S")" "warning: $*"; }

for arg in "$@"; do
  case "$arg" in
    --yes) YES=1 ;;
    --purge-data) PURGE_DATA=1 ;;
    -h|--help) usage; exit 0 ;;
    *) warn "unknown arg: $arg"; usage; exit 1 ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  warn "docker not found. nothing to nuke."; exit 0
fi

if ! docker compose version >/dev/null 2>&1; then
  warn "docker compose not available. nothing to nuke."; exit 0
fi

say "this will remove containers + volumes for this project (including postgres data)."
if [[ "$PURGE_DATA" -eq 1 ]]; then
  say "it will also delete files under ./data."
else
  say "it will NOT delete ./data files (dataset stays)."
fi

if [[ "$YES" -ne 1 ]]; then
  echo
  read -r -p "type 'nuke' to confirm: " CONFIRM
  if [[ "$CONFIRM" != "nuke" ]]; then
    say "aborted."
    exit 0
  fi
fi

echo
say "stopping containers + removing volumes..."
# down -v removes named volumes (postgres_data). remove-orphans cleans leftovers.
docker compose down -v --remove-orphans || true

say "cleaning transient local artifacts..."
rm -rf .pytest_cache __pycache__ */__pycache__ *.log || true
rm -rf scripts/**/__pycache__ || true

# if the user previously ran setup and it wrote temp files, clean them here
rm -f .setup_state .setup_done .env.local || true

if [[ "$PURGE_DATA" -eq 1 ]]; then
  say "purging ./data contents..."
  # keep the directory itself (mounted in docker-compose)
  find ./data -mindepth 1 -maxdepth 1 -exec rm -rf {} + || true
fi

echo
say "done. next step: ./setup.sh"
