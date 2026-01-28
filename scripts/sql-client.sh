#!/usr/bin/env bash
set -euo pipefail

# open flink sql client with the table registry preloaded.
# (useful for interactive debugging)

FLINK_JM_CONTAINER="${FLINK_JM_CONTAINER:-flink-jobmanager}"
INIT_FILE="${INIT_FILE:-/opt/flink/sql/01-ddl.sql}"

docker exec -it "$FLINK_JM_CONTAINER" bash -lc "/opt/flink/bin/sql-client.sh -i '$INIT_FILE'"