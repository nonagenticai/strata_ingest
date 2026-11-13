-- Cooperative cancellation: `POST /api/ingest/jobs/{id}/cancel` on a job with a step still
-- running sets it `cancelling`. Nothing new is claimed or enqueued for it; the running step(s)
-- finish (or fail, are released, or their lease expires), and when the last one has ended the
-- job becomes `cancelled`. A job with nothing running goes straight to `cancelled`.
-- Drift-tolerant and idempotent: every CHECK constraint on ingest_jobs that involves `status` is
-- dropped, whatever it is named (an environment whose check is not `ingest_jobs_status_check`
-- would otherwise keep the old one and reject the new status), then the new one is added.
DO $$
DECLARE
  c record;
BEGIN
  FOR c IN
    SELECT DISTINCT con.conname
    FROM pg_constraint con
    JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY (con.conkey)
    WHERE con.conrelid = 'ingest_jobs'::regclass AND con.contype = 'c' AND a.attname = 'status'
  LOOP
    EXECUTE format('ALTER TABLE ingest_jobs DROP CONSTRAINT %I', c.conname);
  END LOOP;
END $$;
ALTER TABLE ingest_jobs ADD CONSTRAINT ingest_jobs_status_check
  CHECK (status IN ('queued','running','cancelling','completed','failed','rejected','cancelled'));
