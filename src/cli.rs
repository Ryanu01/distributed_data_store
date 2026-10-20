use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::BlobsProtocol;

use crate::bundle;
use crate::config::{Config, DEFAULT_DATA_DIR, DEFAULT_RECEIVE_DIR};
use crate::error::{Context, Result};
use crate::identity;
use crate::metadata::{FileRecord, Metadata};

#[derive(Debug, Parser)]
#[command(
    name = "p2p",
    version,
    about = "Content-addressed peer-to-peer storage"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Send a single file using the original blob-ticket flow.
    Send {
        file: PathBuf,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Receive a single file using the original blob-ticket flow.
    Receive {
        ticket: String,
        output: PathBuf,
        #[arg(long, default_value = DEFAULT_RECEIVE_DIR)]
        data_dir: PathBuf,
    },
    /// Split a file into chunks and store it locally.
    Put {
        file: PathBuf,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Reassemble a locally stored file.
    Get {
        file_id: String,
        output: PathBuf,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// List locally stored files.
    List {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Send { file, data_dir } => send(&file, &data_dir).await,
        Command::Receive {
            ticket,
            output,
            data_dir,
        } => receive(&ticket, &output, &data_dir).await,
        Command::Put { file, data_dir } => put(&file, &data_dir).await,
        Command::Get {
            file_id,
            output,
            data_dir,
        } => get(&file_id, &output, &data_dir).await,
        Command::List { data_dir } => list(&data_dir).await,
    }
}

async fn put(file: &Path, data_dir: &Path) -> Result<()> {
    let config = Config::new(data_dir);
    let store = open_store(&config).await?;
    let identity = identity::load_or_create(&config.identity_path())?;
    let metadata = Metadata::open(&config.metadata_path())?;

    let outcome = bundle::put_file(&store, file).await?;

    metadata.record_file(&FileRecord {
        file_id: outcome.file_id,
        file_name: outcome.file_name.clone(),
        file_size: outcome.file_size,
        file_hash: outcome.file_hash,
        chunk_count: outcome.chunk_count,
        created_at_unix: outcome.created_at_unix,
    })?;
    metadata.add_replica(outcome.file_id, &identity.public().to_string())?;

    println!("Stored file:");
    println!("  file id : {}", outcome.file_id.to_hex());
    println!("  name    : {}", outcome.file_name);
    println!("  size    : {} bytes", outcome.file_size);
    println!("  chunks  : {}", outcome.chunk_count);
    println!("  dedup   : {} bytes already present", outcome.bytes_saved);

    store.shutdown().await?;
    Ok(())
}

async fn get(file_id: &str, output: &Path, data_dir: &Path) -> Result<()> {
    let config = Config::new(data_dir);
    let store = open_store(&config).await?;
    let metadata = Metadata::open(&config.metadata_path())?;

    let file_id = match metadata.resolve_name(file_id)? {
        Some(file_id) => file_id,
        None => bundle::resolve_file_id(&store, file_id).await?,
    };

    let outcome = bundle::get_file(&store, file_id, output).await?;

    println!("Retrieved file:");
    println!("  file id : {}", outcome.file_id.to_hex());
    println!("  name    : {}", outcome.file_name);
    println!("  size    : {} bytes", outcome.file_size);
    println!("  chunks  : {}", outcome.chunk_count);
    println!("  output  : {}", output.display());

    store.shutdown().await?;
    Ok(())
}

async fn list(data_dir: &Path) -> Result<()> {
    let config = Config::new(data_dir);
    let store = open_store(&config).await?;
    let metadata = Metadata::open(&config.metadata_path())?;
    let files = bundle::list_files(&store, &metadata).await?;

    if files.is_empty() {
        println!("No stored files.");
    } else {
        println!(
            "{:<64}  {:>10}  {:>7}  created",
            "FILE ID", "SIZE", "CHUNKS"
        );

        for file in files {
            println!(
                "{:<64}  {:>10}  {:>7}  {}",
                file.file_id.to_hex(),
                file.file_size,
                file.chunk_count,
                file.created_at_unix
            );
            println!("  {}", file.file_name);
        }
    }

    store.shutdown().await?;
    Ok(())
}

async fn open_store(config: &Config) -> Result<FsStore> {
    tokio::fs::create_dir_all(config.data_dir())
        .await
        .with_context(|| {
            format!(
                "failed to create data directory {}",
                config.data_dir().display()
            )
        })?;

    FsStore::load(config.store_path())
        .await
        .with_context(|| format!("failed to open store at {}", config.store_path().display()))
}

async fn open_endpoint(config: &Config) -> Result<Endpoint> {
    tokio::fs::create_dir_all(config.data_dir())
        .await
        .with_context(|| {
            format!(
                "failed to create data directory {}",
                config.data_dir().display()
            )
        })?;

    let secret_key = identity::load_or_create(&config.identity_path())?;

    Endpoint::builder(presets::N0)
        .secret_key(secret_key)
        .bind()
        .await
        .context("failed to bind iroh endpoint")
}

async fn send(filename: &Path, data_dir: &Path) -> Result<()> {
    let config = Config::new(data_dir);
    let endpoint = open_endpoint(&config).await?;
    endpoint.online().await;
    let store = FsStore::load(config.store_path()).await?;
    let blobs = BlobsProtocol::new(&store, None);

    let path = std::path::absolute(filename)?;
    println!("Hashing file...");
    let tag = store.blobs().add_path(path).await?;

    let ticket = BlobTicket::new(endpoint.addr(), tag.hash, tag.format);
    println!("Share this ticket:");
    println!("{ticket}");

    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs)
        .spawn();

    println!("Serving file. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await?;
    println!("Shutting down.");
    router.shutdown().await?;
    Ok(())
}

async fn receive(ticket_str: &str, output: &Path, data_dir: &Path) -> Result<()> {
    let config = Config::new(data_dir);
    let endpoint = open_endpoint(&config).await?;
    endpoint.online().await;
    let store = FsStore::load(config.store_path()).await?;

    let ticket: BlobTicket = ticket_str.parse()?;

    println!("Connecting to provider...");
    let connection = endpoint.connect(ticket.addr().id, iroh_blobs::ALPN).await?;

    println!("Downloading...");
    let progress = store.remote().fetch(connection, ticket.hash());
    let _stats = progress.await?;
    println!("Download finished.");

    let out_path = std::path::absolute(output)?;
    store.blobs().export(ticket.hash(), out_path).await?;
    println!("File saved to: {}", output.display());

    endpoint.close().await;
    Ok(())
}
