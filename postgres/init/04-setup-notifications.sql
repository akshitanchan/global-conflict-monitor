-- Run with: psql -h localhost -U flink_user -d gdelt -f 04-setup-notifications.sql

CREATE OR REPLACE FUNCTION notify_view_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('view_updated', TG_TABLE_NAME);
  -- statement-level triggers don't have NEW/OLD
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Add trigger for each materialized results table
DROP TRIGGER IF EXISTS notify_on_daily_events ON daily_event_volume_by_quadclass;
CREATE TRIGGER notify_on_daily_events
AFTER INSERT OR UPDATE OR DELETE ON daily_event_volume_by_quadclass
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();

DROP TRIGGER IF EXISTS notify_on_actors ON top_actors;
CREATE TRIGGER notify_on_actors
AFTER INSERT OR UPDATE OR DELETE ON top_actors
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();

DROP TRIGGER IF EXISTS notify_on_dyads ON dyad_interactions;
CREATE TRIGGER notify_on_dyads
AFTER INSERT OR UPDATE OR DELETE ON dyad_interactions
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();

DROP TRIGGER IF EXISTS notify_on_cameo ON daily_cameo_metrics;
CREATE TRIGGER notify_on_cameo
AFTER INSERT OR UPDATE OR DELETE ON daily_cameo_metrics
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();
