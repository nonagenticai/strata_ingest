-- Tenant-registered providers (spec §4.1 / §12 step 6a): each tenant registers its own ocr /
-- vision / transcribe provider through PUT /api/tenant/providers; a job's step resolves its
-- tenant's row (else, in a dev deployment only, the providers.toml default; else the step fails
-- `provider_not_configured`).
--
-- The key is never stored in clear: AES-256-GCM under STRATA_INGEST_PROVIDER_KEY, a random 96-bit
-- nonce per write, tenant || capability as associated data (a ciphertext moved to another row
-- does not decrypt). key_version names the sealing key, for a future rotation. No read returns
-- the key.
CREATE TABLE tenant_providers (
  tenant        uuid        NOT NULL,
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
  PRIMARY KEY (tenant, capability),
  CONSTRAINT tenant_providers_tenant_not_nil
    CHECK (tenant <> '00000000-0000-0000-0000-000000000000'::uuid),
  CONSTRAINT tenant_providers_capability CHECK (capability IN ('ocr', 'vision', 'transcribe')),
  CONSTRAINT tenant_providers_positive CHECK (timeout_ms > 0 AND concurrency > 0)
);
