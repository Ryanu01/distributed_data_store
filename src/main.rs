mod bundle;
mod chunker;
mod cli;
mod client;
mod config;
mod error;
mod http;
mod identity;
mod manifest;
mod metadata;
mod metrics;
mod node;
mod protocol;
mod retrieval;

use clap::Parser;

use crate::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let cli = cli::Cli::parse();
    cli::run(cli).await
}
