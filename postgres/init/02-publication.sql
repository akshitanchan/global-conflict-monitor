-- ==============================================
-- Publication for GDELT events table
-- ==============================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'gdelt_flink_pub') THEN
    CREATE PUBLICATION gdelt_flink_pub FOR TABLE public.gdelt_events;
  END IF;
END$$;