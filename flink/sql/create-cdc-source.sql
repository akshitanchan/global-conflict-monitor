-- ==============================================
-- Flink CDC Source Table for GDELT
-- ==============================================
CREATE TABLE IF NOT EXISTS gdelt_cdc_source (
    globaleventid BIGSERIAL PRIMARY KEY,
    event_date INT NOT NULL,              -- YYYYMMDD format
    source_country TEXT,
    target_country TEXT,
    cameo_code TEXT,
    num_events DOUBLE PRECISION,
    num_articles DOUBLE PRECISION,
    quad_class INT,
    goldstein_scale DOUBLE PRECISION
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
