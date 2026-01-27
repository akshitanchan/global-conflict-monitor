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