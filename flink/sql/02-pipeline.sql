-- ==============================================
-- 02-pipeline.sql
--
-- purpose:
--   submit the streaming statement-set job.
--
-- prereq:
--   run 01-ddl.sql first (tables registered).
-- ==============================================

BEGIN STATEMENT SET;

-- daily_event_volume_by_quadclass
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

-- dyad_interactions
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

-- top actors
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

-- cameo metrics (for top-k per day queries)
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

END;
