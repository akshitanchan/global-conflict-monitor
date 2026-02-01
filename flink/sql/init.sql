-- CDC source table (Flink)

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

-- JDBC sink tables (Postgres)

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

