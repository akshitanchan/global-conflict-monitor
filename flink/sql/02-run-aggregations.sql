-- ==============================================
-- Incremental aggregations (Flink maintains state)
-- These INSERT statements are streaming and will keep running.
-- Run them in sql-client and keep the session alive.
-- ==============================================

-- 1) daily_event_volume_by_quadclass
INSERT INTO daily_event_volume_by_quadclass_sink
SELECT
  event_date,
  quad_class,
  SUM(CAST(num_events AS BIGINT)) AS total_events,
  SUM(CAST(num_articles AS BIGINT)) AS total_articles,
  AVG(goldstein) AS avg_goldstein,
  CURRENT_TIMESTAMP AS last_updated
FROM gdelt_cdc_source
GROUP BY event_date, quad_class;

-- 2) dyad_interactions
INSERT INTO dyad_interactions_sink
SELECT
  event_date,
  source_actor,
  target_actor,
  SUM(CAST(num_events AS BIGINT)) AS total_events,
  AVG(goldstein) AS avg_goldstein,
  CURRENT_TIMESTAMP AS last_updated
FROM gdelt_cdc_source
GROUP BY event_date, source_actor, target_actor;

-- 3) top_actors (source actor)
INSERT INTO top_actors_sink
SELECT
  event_date,
  source_actor,
  SUM(CAST(num_events AS BIGINT)) AS total_events,
  SUM(CAST(num_articles AS BIGINT)) AS total_articles,
  AVG(goldstein) AS avg_goldstein,
  CURRENT_TIMESTAMP AS last_updated
FROM gdelt_cdc_source
GROUP BY event_date, source_actor;

-- 4) cameo metrics (for top-k queries later)
INSERT INTO daily_cameo_metrics_sink
SELECT
  event_date,
  cameo_code,
  SUM(CAST(num_events AS BIGINT)) AS total_events,
  SUM(CAST(num_articles AS BIGINT)) AS total_articles,
  AVG(goldstein) AS avg_goldstein,
  CURRENT_TIMESTAMP AS last_updated
FROM gdelt_cdc_source
GROUP BY event_date, cameo_code;
