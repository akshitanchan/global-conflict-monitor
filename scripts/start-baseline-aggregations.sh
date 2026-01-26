#!/usr/bin/env bash
set -euo pipefail

echo "[run] starting postgres baseline aggregations"
docker exec -i gdelt-postgres-baseline \
  psql -U flink_user -d gdelt_baseline <<'SQL'
\set ON_ERROR_STOP on

-- 1) schema / tables (safe to re-run)
\i /docker-entrypoint-initdb.d/01-baseline-schema.sql

-- 2) baseline aggregation run
\i /docker-entrypoint-initdb.d/02-run-aggregations-baseline.sql

SQL

echo "[ok] baseline aggregations completed"
