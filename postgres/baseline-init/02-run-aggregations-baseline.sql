-- baseline/run-aggregations.sql
BEGIN;

-- 1) daily_event_volume_by_quadclass
INSERT INTO daily_event_volume_by_quadclass
SELECT
    event_date,
    quad_class,
    SUM(num_events)::BIGINT AS total_events,
    SUM(num_articles)::BIGINT AS total_articles,
    AVG(goldstein) AS avg_goldstein,
    NOW() AS last_updated
FROM gdelt_events
GROUP BY event_date, quad_class
ON CONFLICT (event_date, quad_class)
DO UPDATE SET
    total_events   = EXCLUDED.total_events,
    total_articles = EXCLUDED.total_articles,
    avg_goldstein  = EXCLUDED.avg_goldstein,
    last_updated   = EXCLUDED.last_updated;

-- 2) dyad_interactions
INSERT INTO dyad_interactions
SELECT
    event_date,
    source_actor,
    target_actor,
    SUM(num_events)::BIGINT AS total_events,
    AVG(goldstein) AS avg_goldstein,
    NOW() AS last_updated
FROM gdelt_events
GROUP BY event_date, source_actor, target_actor
ON CONFLICT (event_date, source_actor, target_actor)
DO UPDATE SET
    total_events  = EXCLUDED.total_events,
    avg_goldstein = EXCLUDED.avg_goldstein,
    last_updated  = EXCLUDED.last_updated;

-- 3) top_actors
INSERT INTO top_actors
SELECT
    event_date,
    source_actor,
    SUM(num_events)::BIGINT AS total_events,
    SUM(num_articles)::BIGINT AS total_articles,
    AVG(goldstein) AS avg_goldstein,
    NOW() AS last_updated
FROM gdelt_events
GROUP BY event_date, source_actor
ON CONFLICT (event_date, source_actor)
DO UPDATE SET
    total_events   = EXCLUDED.total_events,
    total_articles = EXCLUDED.total_articles,
    avg_goldstein  = EXCLUDED.avg_goldstein,
    last_updated   = EXCLUDED.last_updated;

-- 4) daily_cameo_metrics
INSERT INTO daily_cameo_metrics
SELECT
    event_date,
    cameo_code,
    SUM(num_events)::BIGINT,
    SUM(num_events)::BIGINT AS total_events,
    SUM(num_articles)::BIGINT AS total_articles,
    AVG(goldstein) AS avg_goldstein,
    NOW() AS last_updated
FROM gdelt_events
GROUP BY event_date, cameo_code
ON CONFLICT (event_date, cameo_code)
DO UPDATE SET
    total_events   = EXCLUDED.total_events,
    total_articles = EXCLUDED.total_articles,
    avg_goldstein  = EXCLUDED.avg_goldstein,
    last_updated   = EXCLUDED.last_updated;

COMMIT;
