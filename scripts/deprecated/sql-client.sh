#!/usr/bin/env bash
set -euo pipefail
docker exec -it flink-jobmanager bash -lc "/opt/flink/bin/sql-client.sh -i /opt/flink/sql/init.sql"