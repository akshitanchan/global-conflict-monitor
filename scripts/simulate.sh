#!/usr/bin/env bash
DB_URL="${DB_URL:-postgresql://flink_user:flink_pass@localhost:5432/gdelt}"

case "$1" in
  insert)
    psql "$DB_URL" -c "SELECT insert_events($2, ${3:-false});"
    ;;
  update)
    psql "$DB_URL" -c "SELECT update_events($2);"
    ;;
  delete)
    psql "$DB_URL" -c "SELECT delete_events($2);"
    ;;
  *)
    echo "Usage: $0 {insert|update|delete} <count> [late_flag]"
    exit 1
    ;;
esac
