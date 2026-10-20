mod bundle;
mod chunker;
mod manifest;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::BlobsProtocol;

#[derive(Debug, Parser)]
#[command(
    name = "p2p",
    version,
    about = "Content-addressed peer-to-peer storage"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Send a single file using the original blob-ticket flow.
    Send { file: PathBuf },
    /// Receive a single file using the original blob-ticket flow.
    Receive { ticket: String, output: PathBuf },
    /// Split a file into chunks and store it locally.
    Put {
        file: PathBuf,
        #[arg(long, default_value = "./p2p_data")]
        data_dir: PathBuf,
    },
    /// Reassemble a locally stored file.
    Get {
        file_id: String,
        output: PathBuf,
        #[arg(long, default_value = "./p2p_data")]
        data_dir: PathBuf,
    },
    /// List locally stored files.
    List {
        #[arg(long, default_value = "./p2p_data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Send { file } => send(&file).await,
        Command::Receive { ticket, output } => receive(&ticket, &output).await,
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
    let store = open_store(data_dir).await?;
    let outcome = bundle::put_file(&store, file).await?;

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
    let store = open_store(data_dir).await?;
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
    let store = open_store(data_dir).await?;
    let files = bundle::list_files(&store).await?;

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

async fn open_store(data_dir: &Path) -> Result<FsStore> {
    tokio::fs::create_dir_all(data_dir)
        .await
        .with_context(|| format!("failed to create data directory {}", data_dir.display()))?;

    FsStore::load(data_dir)
        .await
        .with_context(|| format!("failed to open store at {}", data_dir.display()))
}

async fn send(filename: &Path) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;
    let store = FsStore::load("./p2p_data").await?;
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

async fn receive(ticket_str: &str, output: &Path) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;
    let store = FsStore::load("./p2p_data_receive").await?;

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
