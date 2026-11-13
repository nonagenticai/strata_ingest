-- Providers bound per target store (owner ruling D92, the strata_rag `store_providers` pattern):
-- a job's ocr / vision / transcribe step resolves, per capability, the row of the job's store,
-- else the nearest ancestor's, else the tenant's (`tenant_providers`), else (dev only) the
-- providers.toml default, else `provider_not_configured`. Written by
-- PUT /api/stores/{id}/providers; same columns and rules as `tenant_providers`.
--
-- `store` is a strata (strata_rag) store id; strata owns the tree, so the rows are scoped by
-- tenant as well (the PK) and a store is only ever looked up within the caller's tenant. The key
-- is AES-256-GCM under STRATA_INGEST_PROVIDER_KEY with tenant || store || capability as
-- associated data (a ciphertext moved to another store, capability, tenant or to
-- `tenant_providers` does not decrypt).
--
-- `store_lineage` caches each store's ancestors (nearest first) as strata reported them. strata
-- never re-parents a store (PATCH changes name / description only) and store ids are uuids that
-- are never reused, so a lineage once read stays true; it is written when a job is created for a
-- leaf and when a store's providers are read or written. A job whose store has no lineage row
-- (jobs created before this migration) resolves its own store's row, then the tenant's.
-- Idempotent: IF NOT EXISTS throughout.
CREATE TABLE IF NOT EXISTS store_providers (
  tenant        uuid        NOT NULL,
  store         uuid        NOT NULL,
  capability    text        NOT NULL,
  base_url      text        NOT NULL,
  dialect       text        NOT NULL,
  model         text        NOT NULL,
  model_id      text,
  regions       text[]      NOT NULL,
  zdr           bool        NOT NULL,
  timeout_ms    int         NOT NULL,
  concurrency   int         NOT NULL,
  api_key_ct    bytea       NOT NULL,
  api_key_nonce bytea       NOT NULL,
  key_version   int         NOT NULL,
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tenant, store, capability),
  CONSTRAINT store_providers_tenant_not_nil
    CHECK (tenant <> '00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT store_providers_store_not_nil
    CHECK (store <> '00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT store_providers_capability CHECK (capability IN ('ocr', 'vision', 'transcribe')),
  CONSTRAINT store_providers_positive CHECK (timeout_ms > 0 AND concurrency > 0)
);

CREATE TABLE IF NOT EXISTS store_lineage (
  tenant     uuid        NOT NULL,
  store      uuid        NOT NULL,
  -- nearest ancestor first; empty for a root
  ancestors  uuid[]      NOT NULL,
  fetched_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant, store),
  CONSTRAINT store_lineage_not_self CHECK (NOT (store = ANY (ancestors)))
);
