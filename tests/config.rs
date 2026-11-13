//! Config + providers.toml validation (Task 1). Pure: no database, no network.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use strata_ingest::config::{Config, Mode, ProvidersFile, Region, Secret};
use strata_ingest::error::IngestError;
use strata_ingest::limits::Limits;
use uuid::Uuid;

const TENANT: &str = "0199f0a0-1111-7000-8000-000000000001";

/// A file in the OS temp dir, removed on drop. Unique per call so parallel tests never collide.
struct TempFile(PathBuf);

impl TempFile {
    fn new(contents: &str) -> Self {
        let path = std::env::temp_dir().join(format!("strata-ingest-test-{}.toml", Uuid::now_v7()));
        std::fs::write(&path, contents).expect("write temp file");
        TempFile(path)
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn load(toml: &str, env: &HashMap<String, String>) -> strata_ingest::Result<ProvidersFile> {
    let f = TempFile::new(toml);
    ProvidersFile::load(&f.0, env)
}

fn key_env() -> HashMap<String, String> {
    env(&[
        ("OCR_KEY", "ocr-secret-value"),
        ("VISION_KEY", "vision-secret-value"),
    ])
}

fn config_err(r: strata_ingest::Result<impl std::fmt::Debug>) -> String {
    match r {
        Err(IngestError::Config(m)) => m,
        other => panic!("expected IngestError::Config, got {other:?}"),
    }
}

const OCR_EU: &str = r#"
region = "eu"

[ocr]
base_url = "https://api.mistral.example/v1"
dialect = "mistral"
model = "mistral-ocr-latest"
api_key = "env:OCR_KEY"
regions = ["eu"]
zdr = true
"#;

#[test]
fn region_mismatch_is_rejected() {
    let toml = OCR_EU.replace(r#"regions = ["eu"]"#, r#"regions = ["us"]"#);
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("ocr"), "names the capability: {m}");
    assert!(m.contains("region"), "names the region rule: {m}");
}

#[test]
fn eu_requires_zdr() {
    let toml = r#"
region = "eu"

[vision]
base_url = "https://api.openai.example/v1"
dialect = "openai"
model = "gpt-vision"
api_key = "env:VISION_KEY"
regions = ["eu"]
zdr = false
"#;
    let m = config_err(load(toml, &key_env()));
    assert!(m.contains("zdr"), "{m}");
    assert!(m.contains("vision"), "{m}");
}

#[test]
fn us_deployment_allows_zdr_false() {
    let toml = OCR_EU
        .replace(r#"region = "eu""#, r#"region = "us""#)
        .replace(r#"regions = ["eu"]"#, r#"regions = ["eu", "us"]"#)
        .replace("zdr = true", "zdr = false");
    let p = load(&toml, &key_env()).expect("us without zdr is valid");
    assert_eq!(p.region, Region::Us);
    assert!(!p.ocr.expect("ocr").zdr);
}

#[test]
fn unknown_dialect_is_rejected() {
    let toml = OCR_EU.replace(r#"dialect = "mistral""#, r#"dialect = "llamaparse""#);
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("dialect"), "{m}");
}

#[test]
fn dialect_must_fit_the_capability() {
    // `openai` is a v1 dialect, but not for ocr.
    let toml = OCR_EU.replace(r#"dialect = "mistral""#, r#"dialect = "openai""#);
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("dialect") && m.contains("ocr"), "{m}");
}

#[test]
fn unknown_section_is_rejected() {
    let toml = format!("{OCR_EU}\n[embed]\nmodel = \"x\"\n");
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("embed"), "{m}");
}

#[test]
fn unknown_field_is_rejected() {
    let toml = format!("{OCR_EU}fallback = \"ocr.secondary\"\n");
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("fallback"), "{m}");
}

#[test]
fn inline_key_is_rejected() {
    let toml = OCR_EU.replace(r#"api_key = "env:OCR_KEY""#, r#"api_key = "sk-123""#);
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("api_key"), "{m}");
    assert!(
        !m.contains("sk-123"),
        "the inline key must not be echoed: {m}"
    );
}

#[test]
fn missing_env_key_is_rejected() {
    let m = config_err(load(OCR_EU, &HashMap::new()));
    assert!(m.contains("OCR_KEY"), "{m}");
}

#[test]
fn env_and_file_keys_resolve_and_defaults_apply() {
    let key_file = TempFile::new("file-secret-value\n");
    let toml = format!(
        "{OCR_EU}\n[transcribe]\nbase_url = \"https://api.openai.example/v1/\"\ndialect = \"openai\"\n\
         model = \"whisper-1\"\napi_key = \"file:{}\"\nregions = [\"eu\"]\nzdr = true\n\
         timeout_ms = 120000\nconcurrency = 2\nrpm = 60\nprice_micros_per_unit = 100\n",
        key_file.0.display()
    );
    let p = load(&toml, &key_env()).expect("valid");
    let ocr = p.ocr.expect("ocr");
    assert_eq!(ocr.api_key.expose(), "ocr-secret-value");
    assert_eq!(ocr.timeout_ms, 60_000, "default timeout");
    assert_eq!(ocr.concurrency, 4, "default concurrency");
    assert_eq!(ocr.rpm, None);
    assert_eq!(ocr.price_micros_per_unit, None);
    assert_eq!(ocr.regions, vec![Region::Eu]);
    let tr = p.transcribe.expect("transcribe");
    assert_eq!(
        tr.api_key.expose(),
        "file-secret-value",
        "file key is trimmed"
    );
    assert_eq!(
        tr.base_url, "https://api.openai.example/v1",
        "trailing slash trimmed"
    );
    assert_eq!(tr.timeout_ms, 120_000);
    assert_eq!(tr.concurrency, 2);
    assert_eq!(tr.rpm, Some(60));
    assert_eq!(tr.price_micros_per_unit, Some(100));
    assert!(p.vision.is_none());
}

#[test]
fn zero_concurrency_is_rejected() {
    let toml = format!("{OCR_EU}concurrency = 0\n");
    let m = config_err(load(&toml, &key_env()));
    assert!(m.contains("concurrency"), "{m}");
}

#[test]
fn zero_providers_is_valid() {
    let p = load("region = \"eu\"\n", &HashMap::new()).expect("valid");
    assert_eq!(p.region, Region::Eu);
    assert!(p.ocr.is_none());
    assert!(p.vision.is_none());
    assert!(p.transcribe.is_none());
}

#[test]
fn mode_defaults_to_dev_and_the_cluster_shape_stays_valid() {
    let p = load(OCR_EU, &key_env()).expect("a file without mode is dev");
    assert_eq!(p.mode, Mode::Dev);
    assert!(!p.allow_internal_provider_hosts);
    let p = load(&format!("mode = \"dev\"\n{OCR_EU}"), &key_env()).expect("explicit dev");
    assert_eq!(p.mode, Mode::Dev);
    let p = load(
        "region = \"eu\"\nmode = \"dev\"\nallow_internal_provider_hosts = true\n",
        &HashMap::new(),
    )
    .expect("dev may lift the guard");
    assert!(p.allow_internal_provider_hosts);
}

#[test]
fn production_has_no_defaults_and_keeps_the_guard() {
    let p = load("region = \"eu\"\nmode = \"production\"\n", &HashMap::new())
        .expect("production without sections");
    assert_eq!(p.mode, Mode::Production);
    let m = config_err(load(
        &format!("mode = \"production\"\n{OCR_EU}"),
        &key_env(),
    ));
    assert!(m.contains("production") && m.contains("[ocr]"), "{m}");
    let m = config_err(load(
        "region = \"eu\"\nmode = \"production\"\nallow_internal_provider_hosts = true\n",
        &HashMap::new(),
    ));
    assert!(m.contains("allow_internal_provider_hosts"), "{m}");
    let m = config_err(load(
        "region = \"eu\"\nmode = \"staging\"\n",
        &HashMap::new(),
    ));
    assert!(m.contains("mode") || m.contains("staging"), "{m}");
}

#[test]
fn provider_key_is_optional_validated_and_redacted() {
    use base64::Engine as _;
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    assert!(Config::from_map(&e).expect("valid").provider_key.is_none());
    let key = base64::engine::general_purpose::STANDARD.encode([5u8; 32]);
    e.insert("STRATA_INGEST_PROVIDER_KEY".into(), key.clone());
    let cfg = Config::from_map(&e).expect("valid key");
    assert_eq!(cfg.provider_key.as_ref().unwrap().expose(), key);
    assert!(!format!("{cfg:?}").contains(&key));
    let short = base64::engine::general_purpose::STANDARD.encode([5u8; 16]);
    e.insert("STRATA_INGEST_PROVIDER_KEY".into(), short.clone());
    let m = config_err(Config::from_map(&e));
    assert!(
        m.contains("STRATA_INGEST_PROVIDER_KEY") && !m.contains(&short),
        "{m}"
    );
}

#[test]
fn metering_is_off_by_default_and_its_url_is_redacted() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    assert!(Config::from_map(&e).expect("valid").metering.is_none());
    e.insert(
        "METERING_REDIS_URL".into(),
        "redis://:s3cret-pw@billing-redis.example.svc.cluster.local:6379/0".into(),
    );
    let cfg = Config::from_map(&e).expect("valid");
    let m = cfg.metering.as_ref().expect("on");
    assert_eq!(m.stream, "billing:costs");
    assert!(!format!("{cfg:?}").contains("s3cret-pw"), "{cfg:?}");
    e.insert("METERING_STREAM".into(), "billing:costs:test".into());
    assert_eq!(
        Config::from_map(&e).unwrap().metering.unwrap().stream,
        "billing:costs:test"
    );
    e.insert("METERING_REDIS_URL".into(), "http://not-redis:6379".into());
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("METERING_REDIS_URL"), "{m}");
}

#[test]
fn licence_auth_is_off_by_default_and_defaults_to_strata_ingest() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    assert!(Config::from_map(&e).expect("valid").licence.is_none());
    // App keys alone do not switch it on.
    e.insert("STRATA_INGEST_LICENSE_APP_KEYS".into(), "x".into());
    assert!(Config::from_map(&e).expect("valid").licence.is_none());
    e.remove("STRATA_INGEST_LICENSE_APP_KEYS");
    e.insert(
        "STRATA_INGEST_LICENSE_INTROSPECT_URL".into(),
        "http://licenses.example.svc.cluster.local:8000/license/introspect".into(),
    );
    let l = Config::from_map(&e).expect("valid").licence.expect("on");
    assert_eq!(l.app_keys, vec!["strata-ingest".to_string()]);
    assert!(l.introspect_url.ends_with("/license/introspect"));
    e.insert(
        "STRATA_INGEST_LICENSE_APP_KEYS".into(),
        " strata-ingest , knowledge ,".into(),
    );
    let l = Config::from_map(&e).expect("valid").licence.expect("on");
    assert_eq!(l.app_keys, vec!["strata-ingest", "knowledge"]);
    e.insert("STRATA_INGEST_LICENSE_APP_KEYS".into(), " , ".into());
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_LICENSE_APP_KEYS"), "{m}");
    e.remove("STRATA_INGEST_LICENSE_APP_KEYS");
    e.insert(
        "STRATA_INGEST_LICENSE_INTROSPECT_URL".into(),
        "licenses:8000".into(),
    );
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_LICENSE_INTROSPECT_URL"), "{m}");
}

#[test]
fn missing_providers_file_is_a_config_error() {
    let path = std::env::temp_dir().join(format!("strata-ingest-missing-{}.toml", Uuid::now_v7()));
    let m = config_err(ProvidersFile::load(&path, &HashMap::new()));
    assert!(m.contains("providers"), "{m}");
}

#[test]
fn provider_debug_redacts_key() {
    let p = load(OCR_EU, &key_env()).expect("valid");
    let dbg = format!("{p:?}");
    assert!(!dbg.contains("ocr-secret-value"), "{dbg}");
    assert!(dbg.contains("Secret(***)"), "{dbg}");
}

#[test]
fn secret_debug_is_redacted() {
    let s = Secret::new("hunter2");
    assert_eq!(format!("{s:?}"), "Secret(***)");
    assert_eq!(s.expose(), "hunter2");
}

/// A full, valid env map pointing at `providers`.
fn full_env(providers: &TempFile) -> HashMap<String, String> {
    env(&[
        (
            "STRATA_INGEST_DATABASE_URL",
            "postgres://strata_ingest:db-pass-SECRET@127.0.0.1:5441/strata_ingest",
        ),
        (
            "STRATA_INGEST_API_TOKENS",
            &format!("{TENANT}:api-token-SECRET"),
        ),
        ("STRATA_INGEST_STORAGE_TOKEN", "storage-token-SECRET"),
        ("STRATA_INGEST_STRATA_TOKEN", "strata-token-SECRET"),
        ("STRATA_INGEST_WEBSCRAPER_TOKEN", "scraper-token-SECRET"),
        ("STRATA_INGEST_PROVIDERS", providers.0.to_str().unwrap()),
        ("OCR_KEY", "ocr-key-SECRET"),
    ])
}

#[test]
fn config_defaults() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    e.remove("STRATA_INGEST_DATABASE_URL");
    e.remove("STRATA_INGEST_WEBSCRAPER_TOKEN");
    e.remove("STRATA_INGEST_STORAGE_TOKEN");
    let cfg = Config::from_map(&e).expect("valid");
    assert_eq!(cfg.bind.to_string(), "127.0.0.1:9135");
    // storage_service in-cluster; the token is optional (tokenless = platform tenant).
    assert_eq!(cfg.storage_url, "http://127.0.0.1:8391");
    assert_eq!(cfg.storage_bucket, "strata-ingest");
    assert!(cfg.storage_token.is_none());
    assert_eq!(cfg.docling_url, "http://127.0.0.1:5002");
    assert!(cfg.database_url.contains(":5441/strata_ingest"));
    assert!(cfg.webscraper_token.is_none());
    assert_eq!(cfg.workers, 4);
    assert_eq!(cfg.tokens.len(), 1);
    assert_eq!(cfg.tokens[0].0, Uuid::parse_str(TENANT).unwrap());
    assert_eq!(cfg.tokens[0].1.expose(), "api-token-SECRET");
    assert_eq!(cfg.providers.region, Region::Eu);
    assert_eq!(cfg.limits, Limits::default());
}

#[test]
fn debug_redacts_secrets() {
    let providers = TempFile::new(OCR_EU);
    let cfg = Config::from_map(&full_env(&providers)).expect("valid");
    let dbg = format!("{cfg:?}");
    for secret in [
        "db-pass-SECRET",
        "api-token-SECRET",
        "storage-token-SECRET",
        "strata-token-SECRET",
        "scraper-token-SECRET",
        "ocr-key-SECRET",
    ] {
        assert!(!dbg.contains(secret), "Debug leaks {secret}: {dbg}");
    }
    assert!(
        dbg.contains("127.0.0.1:5441"),
        "non-secret parts stay visible: {dbg}"
    );
}

#[test]
fn nil_tenant_token_is_rejected() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    e.insert(
        "STRATA_INGEST_API_TOKENS".into(),
        format!("{TENANT}:good,{}:bad-nil-SECRET", Uuid::nil()),
    );
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("nil"), "{m}");
    assert!(
        !m.contains("bad-nil-SECRET"),
        "token must not be echoed: {m}"
    );
}

#[test]
fn malformed_and_missing_tokens_are_rejected() {
    let providers = TempFile::new("region = \"eu\"\n");
    for bad in [
        "no-colon-SECRET",
        "not-a-uuid:tok-SECRET",
        &format!("{TENANT}:"),
    ] {
        let mut e = full_env(&providers);
        e.insert("STRATA_INGEST_API_TOKENS".into(), bad.to_string());
        let m = config_err(Config::from_map(&e));
        assert!(m.contains("STRATA_INGEST_API_TOKENS"), "{bad}: {m}");
        assert!(!m.contains("SECRET"), "token must not be echoed: {m}");
    }
    let mut e = full_env(&providers);
    e.remove("STRATA_INGEST_API_TOKENS");
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_API_TOKENS"), "{m}");
}

#[test]
fn duplicate_token_is_rejected() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    let other = "0199f0a0-2222-7000-8000-000000000002";
    e.insert(
        "STRATA_INGEST_API_TOKENS".into(),
        format!("{TENANT}:same-SECRET,{other}:same-SECRET"),
    );
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("duplicate"), "{m}");
}

#[test]
fn required_secrets_are_required() {
    let providers = TempFile::new("region = \"eu\"\n");
    // The storage token is optional (tokenless in-cluster caller); strata's is not.
    let key = "STRATA_INGEST_STRATA_TOKEN";
    let mut e = full_env(&providers);
    e.remove(key);
    let m = config_err(Config::from_map(&e));
    assert!(m.contains(key), "{key}: {m}");
}

#[test]
fn storage_settings_are_read_and_validated() {
    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    e.insert(
        "STRATA_INGEST_STORAGE_URL".into(),
        "http://storage.example:8391/".into(),
    );
    e.insert("STRATA_INGEST_STORAGE_BUCKET".into(), "other-bucket".into());
    let cfg = Config::from_map(&e).expect("valid");
    assert_eq!(cfg.storage_url, "http://storage.example:8391");
    assert_eq!(cfg.storage_bucket, "other-bucket");
    assert_eq!(
        cfg.storage_token.as_ref().map(|t| t.expose()),
        Some("storage-token-SECRET")
    );
    e.insert("STRATA_INGEST_STORAGE_URL".into(), "storage:8391".into());
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_STORAGE_URL"), "{m}");
    // A blank bucket is the default one, never an empty name.
    e.remove("STRATA_INGEST_STORAGE_URL");
    e.insert("STRATA_INGEST_STORAGE_BUCKET".into(), " ".into());
    assert_eq!(
        Config::from_map(&e).expect("valid").storage_bucket,
        "strata-ingest"
    );
}

#[test]
fn invalid_providers_fail_the_whole_config() {
    let providers = TempFile::new(&OCR_EU.replace("zdr = true", "zdr = false"));
    let m = config_err(Config::from_map(&full_env(&providers)));
    assert!(m.contains("zdr"), "{m}");
}

#[test]
fn limits_defaults_and_overrides() {
    let d = Limits::default();
    // Rulings R24: storage_service's virus-scan ceiling (clamd StreamMaxLength).
    assert_eq!(d.max_upload_bytes, 100 * 1024 * 1024);
    assert_eq!(d.max_pdf_pages, 500, "R37");
    assert_eq!(d.max_audio_seconds, 4 * 3600);
    assert_eq!(d.ocr_min_chars_per_page, 100);
    assert_eq!(d.figure_min_bytes, 8 * 1024);
    assert_eq!(d.max_figures_per_job, 200);
    assert_eq!(d.audio_segment_s, 600);
    assert_eq!(d.max_attempts, 10, "R34");
    assert_eq!(d.max_ocr_input_bytes, 50 * 1024 * 1024);
    assert_eq!(d.docling_ocr_engine, "easyocr");

    let providers = TempFile::new("region = \"eu\"\n");
    let mut e = full_env(&providers);
    e.insert("STRATA_INGEST_MAX_PDF_PAGES".into(), "10".into());
    e.insert("STRATA_INGEST_MAX_FIGURES_PER_JOB".into(), "7".into());
    e.insert("STRATA_INGEST_MAX_OCR_INPUT_BYTES".into(), "1024".into());
    e.insert(
        "STRATA_INGEST_DOCLING_OCR_ENGINE".into(),
        "tesseract".into(),
    );
    let cfg = Config::from_map(&e).expect("valid");
    assert_eq!(cfg.limits.max_pdf_pages, 10);
    assert_eq!(cfg.limits.max_figures_per_job, 7);
    assert_eq!(cfg.limits.max_ocr_input_bytes, 1024);
    assert_eq!(cfg.limits.docling_ocr_engine, "tesseract");

    // The upload limit may be lowered, never raised past storage_service's scan limit.
    e.insert(
        "STRATA_INGEST_MAX_UPLOAD_BYTES".into(),
        (100 * 1024 * 1024).to_string(),
    );
    assert_eq!(
        Config::from_map(&e).expect("valid").limits.max_upload_bytes,
        100 * 1024 * 1024
    );
    e.insert(
        "STRATA_INGEST_MAX_UPLOAD_BYTES".into(),
        (100 * 1024 * 1024 + 1).to_string(),
    );
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_MAX_UPLOAD_BYTES"), "{m}");
    e.insert("STRATA_INGEST_MAX_UPLOAD_BYTES".into(), "1024".into());

    e.insert("STRATA_INGEST_MAX_ATTEMPTS".into(), "0".into());
    let m = config_err(Config::from_map(&e));
    assert!(m.contains("STRATA_INGEST_MAX_ATTEMPTS"), "{m}");
}

#[test]
fn error_codes_and_statuses() {
    let cases: [(IngestError, &str, u16); 15] = [
        (IngestError::NotFound, "not_found", 404),
        (
            IngestError::InvalidCopyTarget("x".into()),
            "invalid_target",
            422,
        ),
        (
            IngestError::PackPublished("x".into()),
            "pack_published",
            409,
        ),
        (
            IngestError::TargetNotEmpty("x".into()),
            "target_not_empty",
            409,
        ),
        (IngestError::UnknownStore, "unknown_store", 404),
        (IngestError::InvalidTarget, "invalid_target", 422),
        (IngestError::InvalidInput("x".into()), "invalid_input", 422),
        (
            IngestError::UnsupportedType("x".into()),
            "unsupported_type",
            422,
        ),
        (IngestError::PayloadTooLarge, "payload_too_large", 413),
        (IngestError::Unauthorized, "unauthorized", 401),
        (IngestError::Conflict("x".into()), "conflict", 409),
        (
            IngestError::Upstream {
                service: "docling",
                message: "down".into(),
            },
            "upstream_unavailable",
            502,
        ),
        (IngestError::Config("x".into()), "internal", 500),
        (IngestError::Db(sqlx::Error::RowNotFound), "internal", 500),
        (
            IngestError::Io(std::io::Error::other("boom")),
            "internal",
            500,
        ),
    ];
    for (err, code, status) in cases {
        assert_eq!(err.code(), code, "code for {err:?}");
        assert_eq!(err.status(), status, "status for {err:?}");
    }
}

/// `check-config` is the deploy gate: a bad providers.toml must exit non-zero.
#[test]
fn check_config_exit_codes() {
    let run = |providers: &TempFile| {
        let mut e = full_env(providers);
        e.insert("STRATA_INGEST_API_TOKENS".into(), format!("{TENANT}:tok"));
        Command::new(env!("CARGO_BIN_EXE_strata-ingest"))
            .arg("check-config")
            .env_clear()
            .envs(e)
            .output()
            .expect("run binary")
    };
    let good = TempFile::new(OCR_EU);
    let out = run(&good);
    assert!(
        out.status.success(),
        "valid config exits 0: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.contains("ocr-key-SECRET"), "{stdout}");
    assert!(!stdout.contains("storage-token-SECRET"), "{stdout}");

    let bad = TempFile::new(&OCR_EU.replace(r#"regions = ["eu"]"#, r#"regions = ["us"]"#));
    let out = run(&bad);
    assert!(!out.status.success(), "invalid config exits non-zero");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("region"), "{stderr}");
}
