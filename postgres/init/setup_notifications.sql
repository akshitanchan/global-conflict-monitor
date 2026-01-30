CREATE OR REPLACE FUNCTION notify_view_updated() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('view_updated', TG_TABLE_NAME);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_daily_event_volume ON public.daily_event_volume_by_quadclass;
CREATE TRIGGER trg_notify_daily_event_volume
AFTER INSERT OR UPDATE OR DELETE ON public.daily_event_volume_by_quadclass
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_updated();

DROP TRIGGER IF EXISTS trg_notify_dyads ON public.dyad_interactions;
CREATE TRIGGER trg_notify_dyads
AFTER INSERT OR UPDATE OR DELETE ON public.dyad_interactions
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_updated();

DROP TRIGGER IF EXISTS trg_notify_top_actors ON public.top_actors;
CREATE TRIGGER trg_notify_top_actors
AFTER INSERT OR UPDATE OR DELETE ON public.top_actors
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_updated();

DROP TRIGGER IF EXISTS trg_notify_cameo ON public.daily_cameo_metrics;
CREATE TRIGGER trg_notify_cameo
AFTER INSERT OR UPDATE OR DELETE ON public.daily_cameo_metrics
FOR EACH STATEMENT EXECUTE FUNCTION notify_view_updated();