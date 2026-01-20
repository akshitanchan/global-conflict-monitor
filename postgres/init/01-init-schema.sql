-- ==============================================
-- GDELT Incremental Processing - Database Schema
-- ==============================================

-- Source table for GDELT events (raw data)
CREATE TABLE gdelt_events (
    globaleventid BIGSERIAL PRIMARY KEY,
    event_date DATE NOT NULL,
    source_country TEXT,
    target_country TEXT,
    cameo_code TEXT,
    num_events INT,
    num_articles INT,
    quad_class INT,
    goldstein_scale DOUBLE PRECISION,
    source_geo_type INT,
    source_geo_lat DOUBLE PRECISION,
    source_geo_long DOUBLE PRECISION,
    target_geo_type INT,
    target_geo_lat DOUBLE PRECISION,
    target_geo_long DOUBLE PRECISION,
    action_geo_type INT,
    action_geo_lat DOUBLE PRECISION,
    action_geo_long DOUBLE PRECISION,
    event_time TIMESTAMP NOT NULL DEFAULT NOW(),
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Indexes for faster queries
CREATE INDEX idx_event_time ON gdelt_events(event_time);
CREATE INDEX idx_source_country ON gdelt_events(source_country);
CREATE INDEX idx_target_country ON gdelt_events(target_country);
CREATE INDEX idx_event_date ON gdelt_events(event_date);

-- ==============================================
-- Result tables (populated by Flink)
-- ==============================================

-- Country-level aggregations
CREATE TABLE country_event_counts (
    country_code TEXT PRIMARY KEY,
    event_count BIGINT DEFAULT 0,
    avg_goldstein DOUBLE PRECISION DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Hourly time-series trends
CREATE TABLE hourly_event_trends (
    event_hour TIMESTAMP PRIMARY KEY,
    total_events BIGINT DEFAULT 0,
    avg_goldstein DOUBLE PRECISION DEFAULT 0,
    unique_countries INT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Actor interaction matrix (who interacts with whom)
CREATE TABLE actor_interaction_matrix (
    source_actor TEXT,
    target_actor TEXT,
    interaction_count BIGINT DEFAULT 0,
    avg_goldstein DOUBLE PRECISION DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (source_actor, target_actor)
);

-- ==============================================
-- Insert sample data for testing
-- ==============================================

-- INSERT INTO gdelt_events (globaleventid, event_date, event_time, actor1_country_code, actor2_country_code, event_code, goldstein_scale, num_articles, avg_tone)
-- VALUES 
--     (1, '2024-01-15', '2024-01-15 10:00:00', 'USA', 'CHN', '043', 5.2, 10, 2.5),
--     (2, '2024-01-15', '2024-01-15 10:05:00', 'USA', 'RUS', '112', -3.1, 8, -4.2),
--     (3, '2024-01-15', '2024-01-15 10:10:00', 'CHN', 'IND', '010', 2.0, 5, 1.8),
--     (4, '2024-01-15', '2024-01-15 10:15:00', 'GBR', 'FRA', '020', 4.5, 12, 3.1),
--     (5, '2024-01-15', '2024-01-15 10:20:00', 'RUS', 'UKR', '190', -8.0, 20, -10.5),
--     (6, '2024-01-15', '2024-01-15 10:25:00', 'USA', 'MEX', '050', 3.5, 7, 1.2),
--     (7, '2024-01-15', '2024-01-15 10:30:00', 'CHN', 'JPN', '043', -2.5, 15, -3.5),
--     (8, '2024-01-15', '2024-01-15 10:35:00', 'IND', 'PAK', '190', -7.0, 25, -8.0),
--     (9, '2024-01-15', '2024-01-15 10:40:00', 'DEU', 'FRA', '010', 6.0, 9, 4.5),
--     (10, '2024-01-15', '2024-01-15 10:45:00', 'BRA', 'ARG', '020', 2.8, 6, 1.5);

-- ==============================================
-- Grant permissions to Flink user
-- ==============================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flink_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flink_user;

-- ==============================================
-- Enable CDC for the source table
-- ==============================================

ALTER TABLE gdelt_events REPLICA IDENTITY FULL;

-- Confirmation message
SELECT 'Database schema initialized successfully!' AS status,
       (SELECT COUNT(*) FROM gdelt_events) AS sample_events;
