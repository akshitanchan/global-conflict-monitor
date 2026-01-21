-- ==============================================
-- Flink CDC Source Table for GDELT
-- ==============================================

CREATE TABLE IF NOT EXISTS gdelt_cdc_source (
    globaleventid BIGINT,
    event_date INT,
    source_country STRING,
    target_country STRING,
    cameo_code STRING,
    num_events DOUBLE,
    num_articles DOUBLE,
    quad_class INT,
    goldstein_scale DOUBLE,
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
    'slot.name' = 'gdelt_flink_slot',
    'debezium.snapshot.mode' = 'initial',
    'decoding.plugin.name' = 'pgoutput',
    'changelog-mode' = 'all'
);
