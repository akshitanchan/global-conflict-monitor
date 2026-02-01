-- Results tables (populated by Flink)

CREATE TABLE IF NOT EXISTS daily_event_volume_by_quadclass (
  event_date INT NOT NULL,
  quad_class INT NOT NULL,
  total_events BIGINT NOT NULL,
  total_articles BIGINT NOT NULL,
  avg_goldstein DOUBLE PRECISION,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_date, quad_class)
);

CREATE TABLE IF NOT EXISTS dyad_interactions (
  event_date INT NOT NULL,
  source_actor TEXT NOT NULL,
  target_actor TEXT NOT NULL,
  total_events BIGINT NOT NULL,
  avg_goldstein DOUBLE PRECISION,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_date, source_actor, target_actor)
);

CREATE TABLE IF NOT EXISTS top_actors (
  event_date INT NOT NULL,
  source_actor TEXT NOT NULL,
  total_events BIGINT NOT NULL,
  total_articles BIGINT NOT NULL,
  avg_goldstein DOUBLE PRECISION,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_date, source_actor)
);

-- Top-k cameo codes per day
CREATE TABLE IF NOT EXISTS daily_cameo_metrics (
  event_date INT NOT NULL,
  cameo_code TEXT NOT NULL,
  total_events BIGINT NOT NULL,
  total_articles BIGINT NOT NULL,
  avg_goldstein DOUBLE PRECISION,
  last_updated TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (event_date, cameo_code)
);

-- Grant permissions to Flink user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flink_user;
