-- `POST /api/stores/{id}/reingest-copy {target_store}` (agent D93, B3 T0i): re-run extraction on
-- the retained originals of every document of a strata store's subtree, with the TARGET store's
-- ocr / vision / transcribe bindings, into an empty new root that mirrors the source tree.
--
-- `reingest_copies` is the copy (what `GET /api/stores/{id}/reingest-copy/{job}` reads);
-- `reingest_copy_stores` maps each source store to the target store mirroring it (the source
-- store itself maps to the target root); `reingest_copy_items` is one row per source document
-- seen: the ingest job that produced it (`producer_job`), how it is copied (`file`: a new ingest
-- job from a copy of its retained original; `record`: a new structured-source job delivering the
-- stored record text; `missing`: nothing retained, listed in `missing_originals`), and the new
-- ingest job doing it (`child_job`). The copy is driven by `reingest::run_driver` (one pod at a
-- time per copy, `lease_until`); only a request creates one.
-- Idempotent: IF NOT EXISTS throughout.
CREATE TABLE IF NOT EXISTS reingest_copies (
  id            uuid        PRIMARY KEY,
  tenant        uuid        NOT NULL,
  source_store  uuid        NOT NULL,
  target_store  uuid        NOT NULL,
  state         text        NOT NULL,
  -- passes that found source documents not yet copied (D92 delta passes)
  passes        int         NOT NULL DEFAULT 0,
  -- `{code, message}` of a failed copy
  error         jsonb,
  lease_until   timestamptz,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  CONSTRAINT reingest_copies_state CHECK (state IN ('running', 'done', 'failed')),
  CONSTRAINT reingest_copies_tenant_not_nil
    CHECK (tenant <> '00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT reingest_copies_distinct_stores CHECK (source_store <> target_store)
);
-- One running copy per target.
CREATE UNIQUE INDEX IF NOT EXISTS reingest_copies_running_target
  ON reingest_copies (tenant, target_store) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS reingest_copies_running
  ON reingest_copies (updated_at) WHERE state = 'running';

CREATE TABLE IF NOT EXISTS reingest_copy_stores (
  copy_id       uuid NOT NULL REFERENCES reingest_copies (id) ON DELETE CASCADE,
  source_store  uuid NOT NULL,
  target_store  uuid NOT NULL,
  PRIMARY KEY (copy_id, source_store),
  UNIQUE (copy_id, target_store)
);

CREATE TABLE IF NOT EXISTS reingest_copy_items (
  copy_id            uuid        NOT NULL REFERENCES reingest_copies (id) ON DELETE CASCADE,
  source_document_id uuid        NOT NULL,
  source_store       uuid        NOT NULL,
  target_store       uuid        NOT NULL,
  kind               text        NOT NULL,
  producer_job       uuid,
  child_job          uuid,
  -- `missing`: why (`originals_not_retained`, `original_infected`, `original_too_large`)
  reason             text,
  title              text,
  source_id          text,
  -- the source document's fields carried onto the copy: {metadata, topic_keys?, supersedes?,
  -- provenance?} (`reingest::knowledge_of`)
  knowledge          jsonb       NOT NULL DEFAULT '{}',
  -- a `record` item's Deliver step key in its child job (a records child holds several)
  child_key          text,
  -- the source document is gone from the source (mirrored deletion at the end of the copy)
  removed            bool        NOT NULL DEFAULT false,
  created_at         timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (copy_id, source_document_id),
  CONSTRAINT reingest_copy_items_kind CHECK (kind IN ('file', 'record', 'missing'))
);
CREATE INDEX IF NOT EXISTS reingest_copy_items_child
  ON reingest_copy_items (child_job) WHERE child_job IS NOT NULL;

-- Which ingest job produced a strata document (`document_ids @> ARRAY[doc]`).
CREATE INDEX IF NOT EXISTS ingest_jobs_document_ids_gin ON ingest_jobs USING gin (document_ids);
