-- ==============================================
-- JDBC sink tables (PostgreSQL) - Phase 2
-- Run after create-cdc-source.sql
-- ==============================================

CREATE TABLE IF NOT EXISTS daily_event_volume_by_quadclass_sink (
  event_date INT,
  quad_class INT,
  total_events BIGINT,
  total_articles BIGINT,
  avg_goldstein DOUBLE,
  last_updated TIMESTAMP(3),
  PRIMARY KEY (event_date, quad_class) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gdelt',
  'table-name' = 'daily_event_volume_by_quadclass',
  'username' = 'flink_user',
  'password' = 'flink_pass',
  'driver' = 'org.postgresql.Driver'
  'sink.buffer-flush.max-rows' = '5000',         -- Batch writes
  'sink.buffer-flush.interval' = '2s',           -- Flush every 2s
  'sink.max-retries' = '3',
  'sink.parallelism' = '2'                       -- Parallel writes
);

CREATE TABLE IF NOT EXISTS dyad_interactions_sink (
  event_date INT,
  source_actor STRING,
  target_actor STRING,
  total_events BIGINT,
  avg_goldstein DOUBLE,
  last_updated TIMESTAMP(3),
  PRIMARY KEY (event_date, source_actor, target_actor) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gdelt',
  'table-name' = 'dyad_interactions',
  'username' = 'flink_user',
  'password' = 'flink_pass',
  'driver' = 'org.postgresql.Driver'
  'sink.buffer-flush.max-rows' = '5000',         -- Batch writes
  'sink.buffer-flush.interval' = '2s',           -- Flush every 2s
  'sink.max-retries' = '3',
  'sink.parallelism' = '2'                       -- Parallel writes
);

CREATE TABLE IF NOT EXISTS top_actors_sink (
  event_date INT,
  source_actor STRING,
  total_events BIGINT,
  total_articles BIGINT,
  avg_goldstein DOUBLE,
  last_updated TIMESTAMP(3),
  PRIMARY KEY (event_date, source_actor) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gdelt',
  'table-name' = 'top_actors',
  'username' = 'flink_user',
  'password' = 'flink_pass',
  'driver' = 'org.postgresql.Driver'
  'sink.buffer-flush.max-rows' = '5000',         -- Batch writes
  'sink.buffer-flush.interval' = '2s',           -- Flush every 2s
  'sink.max-retries' = '3',
  'sink.parallelism' = '2'                       -- Parallel writes
);

CREATE TABLE IF NOT EXISTS daily_cameo_metrics_sink (
  event_date INT,
  cameo_code STRING,
  total_events BIGINT,
  total_articles BIGINT,
  avg_goldstein DOUBLE,
  last_updated TIMESTAMP(3),
  PRIMARY KEY (event_date, cameo_code) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/gdelt',
  'table-name' = 'daily_cameo_metrics',
  'username' = 'flink_user',
  'password' = 'flink_pass',
  'driver' = 'org.postgresql.Driver'
  'sink.buffer-flush.max-rows' = '5000',         -- Batch writes
  'sink.buffer-flush.interval' = '2s',           -- Flush every 2s
  'sink.max-retries' = '3',
  'sink.parallelism' = '2'                       -- Parallel writes
);
