-- Run with: psql -h localhost -U flink_user -d gdelt -f setup_notifications.sql

CREATE OR REPLACE FUNCTION notify_view_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('view_updated', TG_TABLE_NAME);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add trigger for each materialized view table
CREATE TRIGGER notify_on_daily_events
AFTER INSERT OR UPDATE ON daily_event_volume_by_quadclass
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();

CREATE TRIGGER notify_on_actors
AFTER INSERT OR UPDATE ON top_actors
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();

CREATE TRIGGER notify_on_dyads
AFTER INSERT OR UPDATE ON dyad_interactions
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_change();
