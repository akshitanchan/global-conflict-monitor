-- Flink SQL Setup for GDELT CDC Source Table

CREATE TABLE IF NOT EXISTS gdelt_cdc_source (
    globaleventid BIGINT,
    event_date INT,
    source_actor STRING,
    target_actor STRING,
    cameo_code STRING,
    num_events INT,
    num_articles INT,
    quad_class INT,
    goldstein DOUBLE,
    source_geo_type INT,
    source_geo_lat DOUBLE,
    source_geo_long DOUBLE,
    target_geo_type INT,
    target_geo_lat DOUBLE,
    target_geo_long DOUBLE,
    action_geo_type INT,
    action_geo_lat DOUBLE,
    action_geo_long DOUBLE,
    PRIMARY KEY (globaleventid) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'flink_user',
    'password' = 'flink_pass',
    'database-name' = 'gdelt',
    'schema-name' = 'public',
    'table-name' = 'gdelt_events',
    'scan.startup.mode' = 'initial',
    'slot.name' = 'gdelt_flink_slot',
    'debezium.publication.name' = 'gdelt_flink_pub',
    'debezium.publication.autocreate.mode' = 'filtered',
    'debezium.slot.drop.on.stop' = 'false',
    'decoding.plugin.name' = 'pgoutput',
    'changelog-mode' = 'all'
);

-- 2) jdbc sinks

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
);


-- 3) streaming aggregations as one job

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
