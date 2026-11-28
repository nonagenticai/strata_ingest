-- `GET /api/ingest/jobs?q=` server-side search: every job gets its searchable text as a
-- generated column (lowercased filename, URL, title and job id, space separated), indexed with
-- pg_trgm so `search_text LIKE '%token%'` is index-backed. The API splits `q` into tokens and
-- ANDs one LIKE per token.
-- Idempotent: IF NOT EXISTS throughout.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE ingest_jobs
  ADD COLUMN IF NOT EXISTS search_text text GENERATED ALWAYS AS (
    lower(id::text || ' ' || coalesce(source->>'filename', '') || ' ' ||
          coalesce(source->>'url', '') || ' ' || coalesce(meta->>'title', ''))
  ) STORED;

CREATE INDEX IF NOT EXISTS ingest_jobs_search_trgm
  ON ingest_jobs USING gin (search_text gin_trgm_ops);
