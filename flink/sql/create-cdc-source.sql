-- Create CDC source table
CREATE TABLE gdelt_cdc_source (
    globaleventid BIGINT,
    actor1_country_code STRING,
    actor2_country_code STRING,
    goldstein_scale DECIMAL(10,2),
    event_time TIMESTAMP(3),
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
    'decoding.plugin.name' = 'pgoutput'
);