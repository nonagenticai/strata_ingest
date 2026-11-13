-- Crawl / sitemap sources (source kinds `crawl`, `sitemap`): the parent job's Discover step
-- creates one `url` child job per page it found; a child points at its parent. NULL for every
-- other job. Children go with their parent (ON DELETE CASCADE).
ALTER TABLE ingest_jobs
  ADD COLUMN parent_job_id uuid REFERENCES ingest_jobs (id) ON DELETE CASCADE;
CREATE INDEX ingest_jobs_parent_idx ON ingest_jobs (parent_job_id, created_at, id)
  WHERE parent_job_id IS NOT NULL;
