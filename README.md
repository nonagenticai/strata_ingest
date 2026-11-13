# strata-ingest

Turns uploaded files and URLs into markdown bundles and delivers them to strata (strata_rag). Jobs live in its own
Postgres DB; a worker pool runs persisted steps acquire → scan → detect → convert → enrich → bundle → deliver. CPU-only: conversion uses the docling-serve CPU sidecar and ffmpeg. OCR, image
captions and transcription are optional HTTP providers — each tenant's own registration
(`PUT /api/tenant/providers`), else, in a dev deployment, the `providers.toml` default. With none for a job's
tenant, documents still ingest (scanned pages fall back to docling CPU OCR, captions are skipped, audio/video fails
`unsupported_no_provider`); a step that needs a provider its tenant no longer has fails `provider_not_configured`.

## API (port 9135)
Every route except `GET /health` needs `Authorization: Bearer <token>`; the token decides the tenant.
With `STRATA_INGEST_LICENSE_INTROSPECT_URL` set, a bearer matching no static token is checked as a licence token
(licences service `POST /license/introspect`): it must be active for an app in `STRATA_INGEST_LICENSE_APP_KEYS`, and the
licence's tenant is the caller's tenant. A disagreeing `X-Tenant-Id` is 403 `tenant_mismatch`; an invalid licence 401; an active licence of another app 403 `licence_app_not_allowed`;
an introspect failure 503 `licence_unavailable` (fail closed). Positive answers are cached ≤60 s by token hash.
```sh
H='Authorization: Bearer verify-token'
# upload (one job per file part; store_id, source_id, source_run, options, topic_keys, supersedes and provenance must come before the first file part)
curl -H "$H" -F store_id=$STORE -F source_id=src-1 -F source_run=run-7 -F 'options={"ocr":"auto","captions":true}' -F file=@report.pdf localhost:9135/api/ingest/jobs
# URLs (1..=50)                                                   → 202 {"jobs":[{"job_id","status"}]}
curl -H "$H" -H 'Content-Type: application/json' -d '{"store_id":"'$STORE'","source_id":"src-1","source_run":"run-7","urls":["https://example.org/a"]}' localhost:9135/api/ingest/jobs
# structured source (api | datatable): ONE job, one document per record  → 202, status queued | failed
curl -H "$H" -H 'Content-Type: application/json' localhost:9135/api/ingest/jobs -d '{"store_id":"'$STORE'","source_id":"src-2",
  "source":{"kind":"api","config":{"url":"https://api.example.com/customers","auth":{"type":"header","header_name":"X-Api-Key"}},
            "credentials":{"secret":"…"}},
  "mapping":[{"source_field":"customer_id","entity":"Customer","attribute":"customer_id"}]}'
# crawl a site / a sitemap: ONE parent job (202 {"job_id","jobs":[…]}), one url child job per page
curl -H "$H" -H 'Content-Type: application/json' localhost:9135/api/ingest/jobs -d '{"store_id":"'$STORE'","source_id":"src-3",
  "source":{"kind":"crawl","config":{"url":"https://example.org/docs/","max_depth":2,"max_pages":50}}}'
curl -H "$H" -H 'Content-Type: application/json' localhost:9135/api/ingest/jobs -d '{"store_id":"'$STORE'",
  "source":{"kind":"sitemap","config":{"url":"https://example.org/sitemap.xml","include":["/docs/"]}}}'
curl -H "$H" "localhost:9135/api/ingest/jobs?store_id=$STORE&status=failed&limit=50"  # list, newest first → {"jobs":[…],"next":<job id>|null}
curl -H "$H" "localhost:9135/api/ingest/jobs?limit=50&before=$NEXT"                    # the next page
curl -H "$H" localhost:9135/api/ingest/jobs/$JOB                                  # one job
curl -H "$H" -X POST localhost:9135/api/ingest/jobs/$JOB/cancel                   # cancel (below)
curl -H "$H" -X DELETE localhost:9135/api/ingest/jobs/$JOB                        # delete an ended job: 200, or 202 while still deleting (below)
curl -H "$H" -X POST localhost:9135/api/ingest/jobs/$JOB/retry                    # retry a failed job
curl -H "$H" localhost:9135/api/tenant/providers                                  # the tenant's providers (below)
curl -H "$H" -X PUT localhost:9135/api/tenant/providers -H 'Content-Type: application/json' -d @providers.json
curl -H "$H" -X POST localhost:9135/api/tenant/providers/test -H 'Content-Type: application/json' -d @draft.json  # probe, stores nothing
curl localhost:9135/health   # always 200: {"status":"ok"|"degraded","db","storage","docling","strata",...}
```
A multipart upload while 8 others are in progress gets `503 {"error":{"code":"busy"}}` with `Retry-After: 30`.

A URL job is scraped by webscraper_service with `download_images: true` (strata-ingest never fetches an image itself).
Page images of type png / jpeg / webp / gif, `FIGURE_MIN_BYTES`..=10 MiB decoded, at most 20 per page, become figures
like a document's: each replaces its `![alt](src)` reference (images no reference names are appended under a final
`## Images` heading), is stored as a figure asset and captioned when `captions` is on and a vision provider is
configured; the bundle carries `![<caption>](asset:<id>)`. Images not kept keep their original markdown link. The page
original (markdown with the kept images) is virus-scanned like every upload and stays under `MAX_UPLOAD_BYTES`; a
webscraper answer over 256 MiB is fetched again without images.

### Job states, cancel and delete
Job `status`: `queued` → `running` → `completed` | `failed` | `rejected`; cancel adds `cancelling` → `cancelled`;
an unfinished `DELETE` leaves `deleting` (below). Clients must know all eight values.
Every route below is tenant-scoped: another tenant's job is `404 not_found`.

- `POST /api/ingest/jobs/{id}/cancel` → `200` + the job view (as `GET`).
  - `queued`, or `running` with no step in flight → `cancelled` at once; its pending steps are `cancelled`.
  - `running` with a step in flight → `cancelling` (cooperative): nothing new is claimed or enqueued; the worker finishes
    the step it holds (its result is recorded — a Deliver that completes still lists its document in `document_ids`)
    and when the last running step has ended the job becomes `cancelled`. A step is never aborted mid-way; a long
    step (convert, up to its deadline) keeps the job `cancelling` until it ends. Poll `GET` for `cancelled`.
  - `cancelling` / `cancelled` → `200`, unchanged (idempotent).
  - `completed` / `failed` / `rejected` → `409 conflict`. Exception: an ended crawl / sitemap parent (it completes
    when discovery does) with queued / running children → those children are cancelled, `200`.
  - A crawl / sitemap parent's queued and running children are cancelled with it.
- `DELETE /api/ingest/jobs/{id}` → `200`
  `{"job_id", "status": <status it had>, "deleted_jobs", "deleted_objects", "document_ids": [...], "documents": "retained"}`.
  - Only an ended job: `completed`, `failed`, `rejected`, `cancelled`. `queued` / `running` / `cancelling`, a step still
    running (a failed job's parallel step), or a child job not ended → `409 conflict` (cancel first, then poll until
    `cancelled`).
  - A crawl / sitemap **child** (page) job cannot be deleted alone — it would drop out of the parent's `child_job_ids`
    and a source run would count fewer documents than it ingested: `409`
    `{"error": {"code": "child_of_parent_job", "message", "parent_job_id"}}`. Delete the parent instead (it cascades).
  - Removes the job's staged blobs in storage_service (the original `{tenant}/{job}/original` and every recorded asset:
    figures, page images, audio segments), its rows (steps, assets, textual artefacts) and, for a crawl / sitemap
    parent, its child jobs with theirs (`deleted_jobs` counts them).
  - **Documents already delivered to strata_rag are left alone**: `document_ids` lists them; delete them in strata_rag
    if wanted. Billing rows (`metering_outbox`) are kept.
  - It runs in three steps so no database lock is held while storage_service works: (1) in the request, a short
    transaction checks the rules above and marks the job — and a parent's children — `deleting` (never claimed,
    `retry` and `cancel` → `409`); (2) the blobs are deleted; (3) the rows are deleted. Steps 2 and 3 run in a task
    of their own: a client that times out or disconnects does not stop them.
  - The request waits up to **5 s** for steps 2 and 3. Done in time → the `200` above. Still working (a big crawl
    parent, a slow storage_service) → **`202` `{"job_id", "status": "deleting"}`**: the delete goes on without the
    request; poll `GET` until `404`. (The `202` `status` is the job's current status, `deleting`; the `200`
    `status` is the status it had.)
  - A blob that cannot be deleted (within the 5 s) → `502 upstream_unavailable`. The job **stays `deleting`** (and
    so do its children): `GET` and the list (`?status=deleting`) show it, its rows are kept, but some of its blobs
    may already be gone — the original included — so it can never be retried. `DELETE` again resumes and finishes
    it (a missing blob counts as deleted; the answer's `status` is still the status it had before the first
    `DELETE`). A second `DELETE` of a deleted job → `404`.
  - Nothing stays `deleting` for ever: a delete that is working refreshes the job's `deleting_since` every 30 s;
    a **sweeper** (at start, then every 60 s, one job at a time across pods via `FOR UPDATE SKIP LOCKED`) resumes
    every job `deleting` for more than 2 min with nobody working on it — the pod died mid-delete, or the blobs
    failed (it retries until storage_service is back). A `DELETE` racing the sweeper deletes the job once; the
    side that finds it gone answers `404`.

`source_id` (optional, every job kind, 1..=200 chars) is kept in the job's `meta.source_id` and sent to strata as each
document's `source_id`. `source_run` (optional, every job kind, 1..=200 chars, no control characters, only together
with `source_id`, else `400`) is the caller's sync run: kept in `meta.source_run` and sent to strata as each document's
`source_run`, so `DELETE /api/sources/{source_id}/documents?except_run=<run>` on strata_rag keeps this run's documents
and removes the source's older ones. A job without one sends none. A completed job's `document_ids` lists every document it produced, dedup hits (`existing`) included.

### Platform-knowledge document fields (`topic_keys`, `supersedes`, `provenance`)
Every job kind takes three optional per-document fields and forwards them to strata_rag's JSON ingest
(`POST /api/stores/{id}/documents`, strata_rag README "Document sources and re-sync"):

| field | rule (strata_rag's, checked here first: `400 bad_request` before any job exists) |
|---|---|
| `topic_keys` | array of at most 32 topic keys, each 1–128 chars of `[a-z0-9_./-]`; kept sorted, deduplicated |
| `supersedes` | topic keys this document overrides; same rules |
| `provenance` | `{source_url, license, retrieved_at}`, exactly these fields, each non-blank (trimmed) and ≤ 2048 chars, `retrieved_at` RFC 3339 |

They are kept in the job's `meta` under the same names (so a retry, a worker on another pod and every crawl /
sitemap child send the same values) and sent on every delivery of that job. A field left out is not sent, so strata
keeps a re-ingested document's stored value. strata stores `provenance` as `metadata.provenance`; a **platform-owned**
store requires it with a licence on `STRATA_PLATFORM_LICENSES` — that 422 fails the job (no retry) with
`error.code` `provenance_rejected` and strata's message ("licence … is not allowed for platform knowledge …" /
"… provenance {source_url, license, retrieved_at} is required"). A re-ingest copy sends its source document's
values instead.

```jsonc
// (a) JSON url job: request-level fields apply to every URL; a `urls` item may be an object whose own fields
//     replace the request's for that URL (field by field).
{"store_id": "<leaf>", "source_id": "kb-1", "source_run": "run-7",
 "topic_keys": ["hr/leave"], "supersedes": ["hr/leave-2025"],
 "provenance": {"source_url": "https://example.org/leave", "license": "CC-BY-4.0", "retrieved_at": "2026-10-21T08:00:00Z"},
 "urls": ["https://example.org/leave",
          {"url": "https://example.org/sick", "topic_keys": ["hr/sick"],
           "provenance": {"source_url": "https://example.org/sick", "license": "CC0-1.0", "retrieved_at": "2026-10-21T08:00:00Z"}}]}
```
```sh
# (b) multipart upload: JSON text fields, before the first file part; every file of the request gets them
#     (one request per file for different values)
curl -H "$H" -F store_id=$STORE -F 'topic_keys=["hr/leave"]' -F 'supersedes=["hr/leave-2025"]' \
  -F 'provenance={"source_url":"https://example.org/leave.pdf","license":"CC-BY-4.0","retrieved_at":"2026-10-21T08:00:00Z"}' \
  -F file=@leave.pdf localhost:9135/api/ingest/jobs
```
```jsonc
// (c) source run (api | datatable | crawl | sitemap): source-level, applied to every document the source produces
//     (each record / each crawled page). There is no per-record override.
{"store_id": "<leaf>", "source_id": "crm-1", "source_run": "run-8",
 "topic_keys": ["crm/customers"], "supersedes": [],
 "provenance": {"source_url": "https://api.example.com/customers", "license": "CC0-1.0", "retrieved_at": "2026-10-21T08:00:00Z"},
 "source": {"kind": "api", "config": {"url": "https://api.example.com/customers"}},
 "mapping": [{"source_field": "customer_id", "entity": "Customer", "attribute": "customer_id"}]}
```

### Structured sources (`source.kind` = `api` | `datatable`)
- The API fetches the source **while the request is open**: `credentials.secret` exists only for that GET
  (`bearer` → `Authorization: Bearer <secret>`, `header` → `<header_name>: <secret>`, never to a redirect off the
  source's origin) and is never stored, logged or echoed — not in `ingest_jobs`, steps, artefacts or any error.
- Fetch guard (`src/netguard.rs`, shared with `url` jobs): http(s), no userinfo, no internal hostname or non-public literal
  IP (400); then every resolved address must be public and the connection is pinned to them (no proxy); at most 3
  redirects, each re-checked; 60 s and 25 MiB.
- `api`: the response is a JSON array, or an object holding exactly one array (or one under `items`, `data`, `results`,
  `records`, `rows`, `value`, `entries`). Object items are flattened (`address.city`, `lines.0.sku`); mapping
  `source_field`s use those names. `datatable`: `format` `csv` (delimiter `,` `;` tab or `|`, sniffed; first row = header)
  or `xlsx` (sheet `sheet`, else the first; dates come out as Excel serial numbers); without `format` the URL's
  `.csv`/`.xlsx` decides. `header_row: false` (datatable only; default true) takes no header: every row is a
  record and the columns are `column_1`, `column_2`, … .
- Parsing runs off the async runtime with a 30 s deadline; at most 10,000 records, 1,000 columns, 64 MiB of
  materialised record text (a repeated XLSX shared string counts each time) and Excel's 32,767 characters per cell,
  each enforced while parsing (`too_many_records`, `too_many_columns`, `records_too_large`, `cell_too_large`,
  `parse_timeout`); JSON keys ≤ 256 bytes, nesting ≤ 32, ≤ 200,000 values per record (`key_too_long`, `too_deep`,
  `too_many_nodes`), every value charged to the budget. The job stores and shows the source URL without its query and fragment.
- Each record (≤ 10,000) becomes one document: `Entity.attribute: value` lines for the mapped fields, a blank line, then
  every field as `name: value`. strata also gets `metadata` `{"source_id","attributes":{"Entity.attribute":value}}`,
  `title` (first mapped value, e.g. `Customer C-10482`) and `source` `<url without query>#item-N` / `#row-N`.
- Request-shape errors are 400, an unknown kind 422 `unsupported_source_kind`. A fetch/parse failure (source 401/5xx,
  blocked host, bad CSV, no records, …) is `202` with the job `failed` and `error.code` saying why; it has no steps, so
  retry is 409 (the credentials are gone): re-submit instead.

### Crawl and sitemap sources (`source.kind` = `crawl` | `sitemap`)
```json
{"store_id": "<leaf>", "source_id": "…", "source_run": "…", "options": {…},
 "source": {"kind": "crawl", "config": {"url": "https://…", "max_depth": 2, "max_pages": 50,
                                        "allowed_domains": ["example.org"], "link_query": "pricing"}}}
{"store_id": "<leaf>", "source": {"kind": "sitemap",
 "config": {"url": "https://…/sitemap.xml", "max_urls": 200, "include": ["/docs/"], "exclude": ["/old/"]}}}
```
- `crawl`: `max_depth` 1..=5 (default 2; the start page is depth 0), `max_pages` 1..=200 (default 50),
  `allowed_domains` 1..=50 bare hostnames (default: the URL's host; subdomains match), `link_query` (≤ 500 chars,
  passed to webscraper, which then ranks the links). `sitemap`: `max_urls` 1..=1000 (default 200), `include` /
  `exclude` 1..=50 substrings (≤ 200 chars) a URL must contain / must not contain.
- `credentials` / `mapping` are refused; every config problem, a URL the `url`-job guard refuses included, is
  `422 invalid_input`. Nothing is fetched in the request: `202` with the PARENT job (`job_id`, also in `jobs[0]`).
- The parent's one step, `discover` (deadline 3600 s, retried like any step; when its attempts are used up the
  parent fails), creates one `url` child job per page — same tenant, store, options, `source_id`, `source_run`,
  `topic_keys`, `supersedes` and `provenance`, its
  `source.parent_job_id` set — then the parent completes. Children run the full url pipeline (page images
  included) and are listed and retried like any job.
- crawl: webscraper's `/api/scrape/url` returns one page (its `follow_links` exists only on `/api/scrape/bulk`, and
  that Scrapy crawl still answers with the start page alone, without markdown), so strata-ingest walks breadth
  first over each page's `links[]`: http(s), host in `allowed_domains`, not a binary resource (`.pdf`, images,
  media, archives, …), accepted by the url-job guard, fragment stripped, deduplicated. Before a page is scraped its
  host is resolved with the structured-source guard (every address public: no loopback, private, link-local, CGNAT
  or metadata address); a refused page is skipped (`skipped_unsafe` in the step output), one whose host does not
  resolve too (`skipped_unresolvable`; for the start page that is a retryable `fetch_unresolvable`). Every page is scraped ONCE
  (with its images): its original is stored for the child (`source.prefetched`), whose Acquire records it instead
  of scraping. A page webscraper refuses becomes a child that scrapes itself (and fails visibly). Progress is kept
  per page (`discover/<child id>.json`), so a retried discover does not scrape those pages again (it replays them:
  `replayed` in the step output) and creates only the children still missing.
- sitemap: fetched with the structured-source guard (public addresses only, ≤ 3 redirects, 60 s, 25 MiB),
  gzip (`.xml.gz`) inflated up to 50 MiB, `<urlset>` / `<sitemapindex>` (index nesting followed 2 levels, ≤ 50
  sitemap documents, ≤ 49 nested sitemaps queued in all — further index entries are counted in `sitemaps_skipped`,
  a broken nested sitemap skipped); filters, the url-job guard, dedup, `max_urls`. Parsing streams: a document stops
  being read at 50 000 `<loc>`s or as soon as `max_urls` URLs are accepted, and only accepted URLs are kept
  (`locs_read`, `sitemaps_truncated` in the step output). Children scrape their pages themselves.
- Discovery that finds no page (an empty sitemap, filters that keep nothing, a refused start page) fails the parent
  with `error.code` `no_pages_discovered` (not retried, no children) instead of completing empty: a completed run
  replaces the source's earlier documents downstream (care-bus), and an empty one would wipe them.
- `GET /api/ingest/jobs/{id}` of a parent adds `children: {total, by_status: {queued, running, cancelling, completed,
  failed, rejected, cancelled, deleting}}` and `child_job_ids` (oldest first, at most 1000). Cancelling a parent
  (`POST …/{id}/cancel`) stops discovery and cancels its queued and running children in the same transaction; children
  that already ended keep their status. Deleting a parent deletes its children (all must have ended).

### Tenant providers (`/api/tenant/providers`, spec §4.1 — same contract as strata_rag's)
Each tenant registers its own commercial providers (care-bus provisions them). `PUT` replaces the whole set:
```json
{
  "ocr":        {"base_url": "https://api.mistral.ai/v1", "dialect": "mistral", "model": "mistral-ocr-latest",
                 "regions": ["eu"], "zdr": true, "api_key": "sk-...", "model_id": null,
                 "timeout_ms": 60000, "concurrency": 4},
  "vision":     {"base_url": "https://api.example.eu/v1", "dialect": "openai", "model": "gpt-4o-mini",
                 "regions": ["eu"], "zdr": true, "api_key": "sk-..."},
  "transcribe": null
}
```
- Capabilities `ocr` (dialect `mistral`), `vision` and `transcribe` (dialect `openai`). A capability `null` or
  left out is removed. `timeout_ms` default 60000, at most 300000 (ocr, transcribe) / 110000 (vision);
  `concurrency` default 4, 1..=64. `model_id` is stored and echoed only.
- `api_key` is write-only and required: omitted, the stored key is kept only when `base_url` is unchanged; with a
  new `base_url`, or nothing stored, it is `422 api_key_required` (a stored key never goes to a new host).
- `422` codes: `region_not_allowed` (regions must contain the deployment region), `zdr_required` (eu),
  `dialect_not_allowed`, `invalid_provider` (URL, model, numbers, key), `provider_host_not_allowed`, `api_key_required`,
  `probe_failed` (one live minimal call — a 1×1 PNG for ocr/vision, 0.5 s of silent WAV for transcribe — ≤ 10 s;
  the message names the error class only: `auth`, `not_found`, `retryable`, `invalid_request`, `timeout`,
  `unreachable`). `503 provider_key_unconfigured` when `STRATA_INGEST_PROVIDER_KEY` is unset. A malformed body is
  `400 bad_request`. Errors use the standard `{"error":{"code","message"}}` envelope.
- `base_url` is parsed with `url::Url` (reqwest's parser) and accepted only in canonical form (no backslash,
  whitespace, control characters, userinfo, query or fragment; `url.as_str()` equal to the input), never normalised.
- SSRF guard (unless the dev-only `allow_internal_provider_hosts = true`): `https` only; no IP literal in a
  non-public range (loopback, private, link-local, CGNAT, multicast, reserved, documentation, IPv4 embedded
  in IPv6, Teredo, …), no dotless / `.internal` / `.local` / `.svc` / `.cluster.local` / numeric host; every resolved
  address public; tenant clients resolve through a filtering resolver (DNS rebinding), follow no redirects and ignore
  proxy env. `providers.toml` defaults are operator-set and exempt.
- `GET` (and the PUT answer): per capability `source` = `tenant` | `default` | `unconfigured`; a registration adds
  `base_url, dialect, model, model_id, regions, zdr, timeout_ms, concurrency, has_api_key, updated_at`, a dev default
  only `dialect, model`. No response or log carries a key.
- Resolution per step: the job's store binding (§ Store providers), else the job's tenant's registration, else the
  dev default, else none. Clients are cached per (tenant, store or none, capability) and checked against the row's
  `updated_at` on every step, so a PUT on any replica takes effect on the next step everywhere. A registration has no price (`cost_micros` null). `/health` shows the
  defaults only.
- Keys at rest: AES-256-GCM under `STRATA_INGEST_PROVIDER_KEY` (`openssl rand -base64 32`), a random 96-bit nonce
  per write, `tenant || capability` as associated data. `serve` refuses to start when stored keys exist and the key
  is unset.

### Test a provider without saving (`POST /api/tenant/providers/test`, agent 06 §3 A3)
`{"capability": "ocr"|"vision"|"transcribe", "dialect", "base_url", "model", "api_key"?, "model_id"?, "regions"?,
"zdr"?, "timeout_ms"?}` → `200 {"ok", "detail"}`. Runs exactly the probe the PUT runs (same validation, SSRF guard
and minimal call) and stores nothing: no row, no cached client, **no metering event** (the probe is not a tenant's
use). `regions` defaults to the deployment region, `zdr` to `true`. `api_key` omitted → the tenant's stored key of
that capability, only when `base_url` is unchanged; otherwise `422 secret_required_for_new_host`. A failed call
(401, 404, 5xx, unreachable, timeout) is `ok: false` with `detail` naming the error class — never a 5xx, never a key
or URL. `422` codes as the PUT's, plus `unknown_capability`; a malformed body is `400`.

### Store providers (`/api/stores/{id}/providers`, owner ruling D92 — same contract as strata_rag's)
A job's ocr / vision / transcribe models can be pinned per **target store** (the job's `store_id`, a strata store),
so each agent job binds its own knowledge stores to its own models. Migration `0008_store_providers.sql`.
- `PUT /api/stores/{id}/providers` — body, defaults, validation, SSRF guard, live probe and `422` codes exactly as
  `PUT /api/tenant/providers`. Replaces the store's own set: a capability `null` or left out is removed (the store
  inherits it again). The kept-key rule uses the store's own row (same `base_url`); a tenant key is never reused.
- `GET /api/stores/{id}/providers` → `{"store_id", "ocr", "vision", "transcribe"}`; per capability `source` =
  `store` | `ancestor` (+ `from_store`) | `tenant` | `default` | `unconfigured`, with the same fields as the tenant
  view. Never a key (`has_api_key` only).
- `DELETE /api/stores/{id}/providers` = `PUT {}`; answers the view.
- The store is looked up in strata within the caller's tenant: unknown there (also another tenant's) → `404
  unknown_store`. A leaf or a router may bind; a router's rows are inherited by every store under it.
- Resolution per step, per capability: the job's store's row → its nearest ancestor's → the tenant's → dev default →
  none (`provider_not_configured`). Ancestors come from `store_lineage`: recorded when a job is created for the leaf
  (and on any store-providers call), by walking strata's `parent_id` once (strata never re-parents a store); a
  loop, more than 32 ancestors or an ancestor strata no longer knows → `502 upstream_unavailable`. A job created
  before migration 0008 resolves its own store's row, then the tenant's.
- Keys: AES-256-GCM, `tenant || store || capability` as associated data (never opens as a tenant row or for another
  store). Metering: a store binding is the tenant's own key (`funding = tenant_byo`).

### Re-ingest copy (`POST /api/stores/{id}/reingest-copy`, agent D93 — B3 T0i)
Rebuilds a store tree with other ocr / vision / transcribe models: re-runs extraction on the retained originals of
every document of `{id}`'s subtree with the **target's** bindings (§ Store providers), into an empty new root that
mirrors the source tree (each store recreated with its kind, name and description; the target keeps its own name).
strata_rag's `reembed-copy` is the counterpart for embed / rerank / chat. Migration `0009_reingest_copy.sql`.
- `POST /api/stores/{id}/reingest-copy {"target_store"}` → `202` the copy view (`state: "running"`). `404
  unknown_store` (source or target unknown in the caller's tenant); `409 pack_published` (the target is in a
  published platform pack — checked first, before anything runs); `422 invalid_target` (the source itself, not a
  root, or another kind than the source); `409 target_not_empty`; `409 conflict` (a copy into that target is
  already running); `400` for any other body.
- `GET /api/stores/{id}/reingest-copy/{job}` → `{job, source_store, target_store, state: running|done|failed,
  documents_done, documents_total, documents_failed, documents_missing, missing_originals: [{document_id, reason,
  title, source_id}], error?: {code, message}, passes, started_at, finished_at?}`. Another tenant's copy (or one of
  another source) → `404 not_found`.
- **What is copyable.** An original is kept only while the ingest job that delivered it exists (`DELETE
  /api/ingest/jobs/{id}` removes it). A document delivered by a live upload / URL job (crawl pages included) is
  copied from a copy of its original into a new job on the mirrored leaf (a URL page is not scraped again); a
  record of a live `api` / `datatable` job is delivered again from its stored text (no extraction). Anything else
  (its job was deleted, it was written to strata directly, or by another tenant's job) is listed in
  `missing_originals` with `reason` `originals_not_retained` (`original_too_large`, `original_infected` when the
  stored original cannot be copied) and not copied; the copy still finishes.
- Each copy carries its source document's `source_id`, `metadata` (with `metadata.provenance`), `provenance`,
  `topic_keys`, `supersedes` and `source_run` as strata lists them (no listed run: the producing job's
  `meta.source_run`).
- Driven by every pod (`reingest::run_driver`, 5 s tick, one pod per copy by a 2 min lease); only a request starts
  one. A listing that finds source documents not seen before is a pass; when a listing finds nothing new and every
  new ingest job has ended, target copies of documents deleted from the source meanwhile are deleted (mirrored
  deletion) and the copy is `done` — or `failed` when one of its jobs did not complete (`error.code` = their shared
  error code, e.g. `pack_published`, else `documents_failed`). More than 8 passes → `failed` `conflict` ("did not
  converge"). strata refusing a write because the target's pack was published meanwhile fails the copy
  `pack_published` (a Deliver step answered 409 `pack_published` fails its job with that code, never retried).

CLI: `strata-ingest serve` (migrates, then API + workers), `check-config` (validate env + providers,
print redacted config, non-zero on error), `migrate` (needs only `STRATA_INGEST_DATABASE_URL`).

## Configuration (`STRATA_INGEST_*`)
| Variable | Default | |
|---|---|---|
| `API_TOKENS` | — | **required**, `<tenant-uuid>:<token>,…` (nil uuid refused) |
| `STRATA_TOKEN` | — | **required**, bearer for strata |
| `DATABASE_URL` | `postgres://strata_ingest:strata_ingest@127.0.0.1:5441/strata_ingest` | required for `migrate` |
| `BIND` | `127.0.0.1:9135` (image: `0.0.0.0:9135`) | |
| `PROVIDERS` | `/etc/strata-ingest/providers.toml` | file must exist |
| `PROVIDER_KEY` | none | secret: base64 of 32 bytes sealing tenant provider keys; needed for `PUT /api/tenant/providers` |
| `STORAGE_URL` / `STORAGE_BUCKET` / `STORAGE_TOKEN` | `http://127.0.0.1:8391` / `strata-ingest` / none | |
| `DOCLING_URL` | `http://127.0.0.1:5002` | |
| `WEBSCRAPER_URL` / `WEBSCRAPER_TOKEN` | `http://127.0.0.1:9119` / none | |
| `STRATA_URL` | `http://127.0.0.1:8090` | |
| `LICENSE_INTROSPECT_URL` | none = static tokens only | licences service `POST /license/introspect` URL; a bearer matching no `API_TOKENS` entry is introspected as a licence token |
| `LICENSE_APP_KEYS` | `strata-ingest` | comma-separated app keys whose active licences grant access |
| `WORKERS` | `4` | |
| `MAX_UPLOAD_BYTES`, `MAX_PDF_PAGES`, `MAX_AUDIO_SECONDS`, `OCR_MIN_CHARS_PER_PAGE`, `FIGURE_MIN_BYTES`, `MAX_FIGURES_PER_JOB`, `AUDIO_SEGMENT_S`, `MAX_ATTEMPTS`, `MAX_OCR_INPUT_BYTES`, `DOCLING_OCR_ENGINE` | 100 MiB, 500, 14400, 100, 8192, 200, 600, 10, 50 MiB, `easyocr` | `src/limits.rs` |

Unprefixed (shared with the other billing emitters):

| Variable | Default | |
|---|---|---|
| `METERING_REDIS_URL` | none = drainer off (events still recorded) | secret: billing-service's Redis (`redis://…`); validated at load, never logged |
| `METERING_STREAM` | `billing:costs` | set it explicitly in the Deployment (billing EMITTER_GUIDE) |

### Metering (`src/metering.rs`, `src/db/outbox.rs`, migration `0005_metering_outbox.sql`)
One cost event per paid provider call (OcrPages with `engine: provider`, Caption, TranscribeSegment), through an
outbox. Contract: billing-service `docs/EMITTER_GUIDE.md`.
- **Recorded when the call returns.** Right after a successful provider call the step records its event
  (`metering::record`: one INSERT into `metering_outbox`, its own transaction), independent of the step's
  completion and its fence. The provider was already paid, so the row exists even when the step's result is then
  fenced out, rolled back or fails (e.g. `ocr_missing_page`), and for a refused caption (`caption_refused`). A
  step that made no successful provider call (a provider error, `caption_failed`, a skip, docling-cpu) has no row.
  Idempotent on `event_key` (`ON CONFLICT DO NOTHING`). A failing record never fails the step (a retry would
  call, and pay, the provider again): it is tried 4 times over ~1.3 s; if it still fails, the worker writes the
  event inside the step's completion transaction, or — when the step does not complete — tries once more and
  otherwise logs `metering_record_failed_after_paid_call` at error level with the full field map.
- **One paid call per step attempt** is what makes `event_key` unique per call. Recording a second, different call
  under the same key is an error log (and a panic in debug builds), never a silent dedup.
- **Delivered by the drainer** (`metering::run_drainer`, spawned by `serve` once migrations ran, only when
  `METERING_REDIS_URL` is set): a pass at start, then every 5 s (at once again after a full batch). A pass claims up
  to 100 due rows `FOR UPDATE SKIP LOCKED`, leases them (`next_attempt_at` = now + 60 s) and COMMITS; then, with no
  transaction open, XADDs them in order to `METERING_STREAM` (`MAXLEN ~ 100000`) and settles each row on its own:
  `sent`, or `attempts + 1`, `last_error`, backoff `min(300 s, 5 s · 2^(attempts-1))`. A row Redis refuses does not
  stop the pass; a transport failure (refused / dropped / timed out), or a pass past 30 s, gives the untried rest
  back (due in 5 s, no attempt counted). Each replica runs one; the lease keeps them apart. Failures are logged at
  most once a minute with the backlog; recovery is logged once. On shutdown it makes a last pass after the workers
  stopped.
- **At-least-once.** A drainer that dies between the XADD and marking the row leaves it leased; after the lease it
  is re-sent under the same `event_key`, which billing dedups on.
- **Redis off or down loses nothing**: rows are written whether or not `METERING_REDIS_URL` is set and stay
  `pending` until a drainer delivers them (their original `timestamp`). Pending rows are never pruned.
- **Backlog**: `/health` → `metering: {transport: "on"|"off", pending, oldest_pending_age_s}` (null when the DB
  could not be read). `/health` is cluster-internal only: strata-ingest has no VirtualService and no cloudflared
  host (in the deployment's mesh policy). Alert when `oldest_pending_age_s` stays above a few minutes with `transport: "on"`.
- **Retention**: `sent` rows older than 7 days are pruned hourly (at most 5000 per pass).
- **Ledger**: `ingest_steps` (`provider`, `model`, `usage`, `cost_micros` of the committed attempt) plus the outbox
  for calls whose step did not commit. Outbox rows have no foreign key, so a billing record outlives the job's rows.
- `cost_source` `strata-ingest`; `service_key` `strata-ingest-<ocr|vision|transcribe>`; `model` = the provider's model.
- `event_key` `<job_id>:<kind>/<key>:<attempt>`, e.g. `0199…:ocr_pages/p0-1:3` or `0199…:caption/<asset id>:1`
  (billing dedups on it; a key over 64 characters is condensed by billing to `p1:<sha256 prefix>`, deterministically).
  Unchanged by the move to the outbox. It is NOT the outbox row id: a step attempt makes one provider call, so the
  attempt identifies the call; a retried attempt bills its own call, once.
- Quantity: the natural unit goes in `input_tokens` — tokens (vision, `quantity_unit` `tokens`), seconds (transcribe,
  `quantity_unit` `seconds`) or OCR pages (billing has no pages unit, so `quantity_unit` is omitted; `usage_extra`
  carries `unit: "pages"` and `pages`). Usage the provider did not report: `input_tokens` 0, `usage_extra.units`
  null and `usage_unknown: true`, plus a warn log, so the $0 row explains itself. `cost_usd` = `cost_micros / 1e6`
  (a hint; billing reprices), omitted when unknown. `latency_ms` = the provider call; `timestamp` = when it returned (RFC 3339 UTC).
- `funding`: a tenant-registered provider is `tenant_byo` (`usage_extra.byo: true`, `provider` = its dialect). A
  `providers.toml` default on a private host (internal name, `localhost`, single-label name, non-global IP) is
  `local_gpu`, `provider` named after the host's first label (an IP → `local`), so it never joins a commercial
  vendor's price row; any other host is `platform_external`, `provider` = its dialect.
- Attribution: jobs carry only the tenant (from the API token), so `tenant_id` is the job's tenant, `project_id` is
  billing's `tenant_level_project_id(tenant)`, and `license_id` / `subject_id` / `app_id` are sent empty (`actor_id`
  omitted). `usage_extra` = `{capability, unit, units, job_id, step_key}` (+ `pages`, `byo`, `usage_unknown`).

## Queue behaviour and limits
- **Retries**: a Retryable step failure is retried up to `MAX_ATTEMPTS` (10) times with backoff 2, 4, … 256, 300 s
  (≈ 13.5 min in all), so a dependency restart or a 429 storm is survived. A provider 429/503 `Retry-After`
  lengthens the wait (capped at 300 s). storage_service / webscraper 401/403 (licence-service blips) are retried.
- **Fairness**: workers claim the step of the job with the fewest running steps first, then the oldest.
- **Deadlines** count work only: waiting for a provider permit/rpm slot, the pod's Convert slot or a storage
  upload slot is excluded and bounded on its own (30 min → `slot_wait_timeout`, retried).
- **Large documents**: `MAX_PDF_PAGES` 500 (docling is asked for at most one page more); docling timeout 1800 s,
  its answer is capped at 512 MiB (`conversion_too_large`); one Convert (or docling-cpu OCR batch) runs per pod at
  a time; docling-cpu OCR runs in batches of ≤ 8 pages like the provider path.
- **storage_service**: at most 2 uploads in flight per pod (storage-api buffers bodies in a 256 Mi pod).
- **Discover** (crawl / sitemap parents): deadline 3600 s; crawl ≤ 200 pages (≤ 1000 followed links kept per
  page), sitemap ≤ 1000 URLs from ≤ 50 sitemap documents (≤ 49 nested sitemaps queued, ≤ 50 000 `<loc>`s read per
  document; parsing stops once `max_urls` are accepted) (`src/limits.rs`). No page found → `no_pages_discovered`.
- **URL page images**: at most 20 per page (`MAX_PAGE_IMAGES`), each ≤ 10 MiB (`MAX_PAGE_IMAGE_BYTES`) and
  ≥ `FIGURE_MIN_BYTES`; they count towards `MAX_FIGURES_PER_JOB` like any figure (`src/limits.rs`).
- **Captions** are optional: a vision call that will not be retried ends the Caption step `skipped`
  (`caption_failed`, error kept) and the figure keeps its bare link; OCR and transcription failures stay fatal.
  A vision answer that is a refusal ("I'm sorry, but I cannot assist with that.") is never stored as the caption:
  the step ends `skipped` (`caption_refused`) and the figure keeps its bare link.
- **Shutdown** (SIGTERM): requests drain (≤ 30 s); each worker gives its step 10 s, then aborts and releases it
  (pending, attempt not used) for another pod. Needs `terminationGracePeriodSeconds` ≥ 120.

## providers.toml
The deployment layer: `region`, `mode = "dev" | "production"` (default `dev`; `production` allows no
`[ocr]`/`[vision]`/`[transcribe]` section — every tenant registers its own) and the dev-only
`allow_internal_provider_hosts` (default `false`; `true` in production is a startup error). The sections are the dev
defaults every tenant without a registration uses.
Zero providers (valid): `region = "eu"`. With providers (keys are `env:NAME` or `file:/path`, never inline;
`regions` must include the deployment region; `eu` requires `zdr = true`):
```toml
region = "eu"
[ocr]
base_url = "http://127.0.0.1:9131"
dialect = "mistral"          # ocr: mistral · vision/transcribe: openai
model = "mistral-ocr-latest"
api_key = "env:MLX_API_KEY"
regions = ["eu"]
zdr = true
[transcribe]
base_url = "http://127.0.0.1:9131"
dialect = "openai"
model = "whisper-1"
api_key = "env:MLX_API_KEY"
regions = ["eu"]
zdr = true
```
Optional per provider: `timeout_ms` (60000), `concurrency` (4), `rpm`, `price_micros_per_unit`.

## Storage
Originals, figures and audio segments go to the in-cluster storage_service, which virus-scans every upload
(ClamAV in the request) — no MinIO or clamd here. Upload limit 100 MiB (its clamd stream limit). Optional
licence token `STRATA_INGEST_STORAGE_TOKEN` is sent as `X-Tenant-Id: Bearer <token>`. Keys are `{tenant}/{job}/…`.

## Local dev
`docker compose up -d` (postgres :5441, docling-serve CPU :5002, images from Docker Hub), then
`cargo fmt --check && cargo clippy --all-targets -- -D warnings && ./scripts/ci-test.sh`
(testcontainers; the script puts ffmpeg/ffprobe on PATH — a static build from
`mwader/static-ffmpeg:7.1.1` on Linux, Homebrew's on macOS — then the guard runs
`cargo test --locked`; CI runs the same script and falls back to cargo).
Image: `Dockerfile` (debian 12-slim, uid 65532, ffmpeg 5.1, read-only root; only `/tmp` writable).

## Known limitations
- CPU OCR (docling easyocr) is best-effort: ~25–45% recall on JPEG scans. Configure an `ocr` provider for scans.
- HTML/EPUB uploads: figures are not exported by docling, so they are not captioned (URL jobs get their page images
  from webscraper instead).
- strata reads the bundle's `<!-- page:N -->` anchors into each chunk's `page_start` / `page_end` and returns them
  on query results (strata_rag #15); other bundle metadata beyond `{text, source, title, source_id}` (and a
  structured record's `metadata`) stays on the job.
- `serve` exits if Postgres is unreachable at startup (migrations run first); it keeps running if the DB drops later.

## Docs
Plan `docs/superpowers/plans/2026-10-16-strata-ingest.md` · spec strata_rag `docs/ingestion-providers-spec` @ `fa541eb`
(`docs/superpowers/specs/2026-10-16-ingestion-and-providers-design.md`) · contracts provider_contracts `CONTRACTS.md`.

## License

Apache 2.0 — see [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
