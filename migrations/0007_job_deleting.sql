-- `DELETE /api/ingest/jobs/{id}` runs in three phases so no row lock is held while
-- storage_service deletes blobs: a short transaction marks the job (and a crawl / sitemap
-- parent's children) `deleting`, the blobs are deleted outside any transaction, then a second
-- transaction deletes the rows. Phases 2 and 3 run in a task of their own (not the request's),
-- which refreshes `deleting_since` while it works. A job left `deleting` (a blob failure, the pod
-- died) is never claimed, retried or cancelled; DELETE again resumes it, and the delete sweeper
-- resumes it once `deleting_since` is older than its threshold. `deleting_from` keeps the status
-- it had (the `status` of the DELETE answer).
-- Drift-tolerant and idempotent: columns only if missing; every CHECK constraint on ingest_jobs
-- that involves `status` is dropped whatever its name, then the new one is added.
ALTER TABLE ingest_jobs ADD COLUMN IF NOT EXISTS deleting_from text;
ALTER TABLE ingest_jobs ADD COLUMN IF NOT EXISTS deleting_since timestamptz;
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
  CHECK (status IN ('queued','running','cancelling','completed','failed','rejected','cancelled',
                    'deleting'));
-- The delete sweeper's scan.
CREATE INDEX IF NOT EXISTS ingest_jobs_deleting_since_idx
  ON ingest_jobs (deleting_since) WHERE status = 'deleting';
