//! `strata-ingest` command line: `serve` (API + workers), `check-config`, `migrate`.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::config::{Config, redact_url};
use crate::error::{IngestError, Result};

#[derive(Debug, Parser)]
#[command(
    name = "strata-ingest",
    version,
    about = "Turns uploaded files and URLs into markdown bundles for strata"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the HTTP API on STRATA_INGEST_BIND at once (job routes 503 `not_ready` until the DB is
    /// migrated), apply migrations (retried for up to 2 min while Postgres is unreachable), then
    /// start the worker pool; until SIGTERM / ctrl-c (stop accepting, finish in-flight requests
    /// and current steps, exit 0).
    Serve,
    /// Validate STRATA_INGEST_* env and the providers file, print the redacted config.
    /// Exits non-zero on any config error (deploy gate).
    CheckConfig,
    /// Apply the embedded database migrations and exit. Needs only STRATA_INGEST_DATABASE_URL
    /// (required; no tokens, storage settings or providers file), so it can run as a deploy step on its own.
    Migrate,
}

pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("strata_ingest=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        // No ANSI colour codes in container logs (they break Loki grep / LogQL parsing).
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .try_init();
}

pub async fn run(cli: Cli) -> Result<ExitCode> {
    match cli.command {
        Command::Serve => {
            let cfg = Config::from_env()?;
            let bind = cfg.bind;
            let ctx = std::sync::Arc::new(crate::pipeline::Ctx::from_config(cfg)?);
            let listener = match tokio::net::TcpListener::bind(bind).await {
                Ok(l) => l,
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                    return Err(IngestError::Config(format!(
                        "port {} on {} is already in use; set STRATA_INGEST_BIND to a free port \
                         (never 8000)",
                        bind.port(),
                        bind.ip()
                    )));
                }
                Err(e) => return Err(e.into()),
            };
            let startup = {
                let ctx = ctx.clone();
                async move {
                    crate::db::migrate_with_retry(&ctx.pool, crate::db::STARTUP_DB_DEADLINE)
                        .await?;
                    ctx.providers.check_keys().await
                }
            };
            crate::api::serve_starting(ctx, listener, crate::api::shutdown_signal(), startup)
                .await?;
            tracing::info!("strata-ingest stopped");
            Ok(ExitCode::SUCCESS)
        }
        Command::CheckConfig => {
            let cfg = Config::from_env()?;
            println!("config ok\n{cfg:#?}");
            Ok(ExitCode::SUCCESS)
        }
        Command::Migrate => {
            // Required here (no dev default): a deploy step must never migrate a guessed DB.
            let url = std::env::var("STRATA_INGEST_DATABASE_URL")
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    IngestError::Config("STRATA_INGEST_DATABASE_URL is required".to_string())
                })?;
            let pool = crate::db::connect(&url).await?;
            pool.close().await;
            println!("migrations applied to {}", redact_url(&url));
            Ok(ExitCode::SUCCESS)
        }
    }
}
