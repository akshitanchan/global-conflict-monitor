-- Create CDC source table
CREATE TABLE IF NOT EXISTS gdelt_cdc_source (
    globaleventid BIGINT,
    event_date DATE,
    actor1_country_code STRING,
    actor2_country_code STRING,
    event_code STRING,
    goldstein_scale DECIMAL(10,2),
    num_articles INT,
    avg_tone DECIMAL(10,2),
    event_time TIMESTAMP(3),
    source_url STRING,
    last_updated TIMESTAMP(3),
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
    'debezium.snapshot.mode' = 'never',
    'debezium.publication.name' = 'gdelt_flink_pub',
    'debezium.publication.autocreate.mode' = 'filtered',
    'debezium.slot.drop.on.stop' = 'false',
    'decoding.plugin.name' = 'pgoutput',
    'changelog-mode' = 'all'
);
