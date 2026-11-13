#![forbid(unsafe_code)]
//! `strata-ingest` binary entry point.

use std::process::ExitCode;

use clap::Parser;
use strata_ingest::cli::{Cli, init_tracing, run};

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}
