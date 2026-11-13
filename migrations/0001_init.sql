-- strata-ingest: jobs, their persisted steps (the work queue), stored assets (blobs in
-- storage_service) and textual artefacts (convert/ocr/transcript/bundle, kept here).

CREATE TABLE ingest_jobs (
  id uuid PRIMARY KEY, tenant uuid NOT NULL, store_id uuid NOT NULL,
  source jsonb NOT NULL,              -- {"kind":"upload","filename":..,"mime":..,"bytes":..,"scan":"clean|unverified|infected"} | {"kind":"url","url":..}
  options jsonb NOT NULL,             -- {"ocr":"auto|force|off","captions":bool,"language":str|null}
  meta jsonb NOT NULL DEFAULT '{}',   -- {title, mime, pages, language, scan} filled by the pipeline
  status text NOT NULL CHECK (status IN ('queued','running','completed','failed','rejected','cancelled')),
  error jsonb, document_ids uuid[] NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX ON ingest_jobs (tenant, store_id, status, created_at DESC);

CREATE TABLE ingest_steps (
  id bigserial PRIMARY KEY, job_id uuid NOT NULL REFERENCES ingest_jobs ON DELETE CASCADE,
  kind text NOT NULL, key text NOT NULL DEFAULT '', stage int NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','running','completed','failed','skipped','cancelled')),
  attempt int NOT NULL DEFAULT 0, run_after timestamptz NOT NULL DEFAULT now(), lease_until timestamptz,
  input jsonb NOT NULL DEFAULT '{}', output jsonb,
  provider text, model text, usage jsonb, cost_micros bigint, error jsonb,
  started_at timestamptz, finished_at timestamptz, UNIQUE (job_id, kind, key));
CREATE INDEX ON ingest_steps (status, run_after) WHERE status = 'pending';
CREATE INDEX ON ingest_steps (lease_until) WHERE status = 'running';  -- expired-lease reaper

CREATE TABLE ingest_assets (
  id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES ingest_jobs ON DELETE CASCADE,
  kind text NOT NULL CHECK (kind IN ('original','figure','page_image','audio_segment')),
  object_key text NOT NULL, page int, caption text, bytes bigint NOT NULL, mime text NOT NULL,
  UNIQUE (job_id, object_key));  -- a retried step re-records its blob in place

-- Textual intermediates of a job, by name relative to the job: convert.json, ocr/<step>.json,
-- transcript/<step>.json, bundle.md. Written by steps (fenced on the claim), upserted on retry.
CREATE TABLE ingest_artifacts (
  job_id uuid NOT NULL REFERENCES ingest_jobs ON DELETE CASCADE,
  key text NOT NULL, body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, key));
