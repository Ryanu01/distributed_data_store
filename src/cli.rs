use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::BlobsProtocol;

use crate::client::ApiClient;
use crate::config::{Config, DEFAULT_API_ADDR, DEFAULT_DATA_DIR, DEFAULT_RECEIVE_DIR};
use crate::error::{Context, Result};
use crate::identity;

#[derive(Debug, Parser)]
#[command(
    name = "p2p",
    version,
    about = "Content-addressed peer-to-peer storage"
)]
pub struct Cli {
    /// Override the local node HTTP API address.
    #[arg(long, global = true)]
    pub api: Option<SocketAddr>,
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
    /// Split a file into chunks and store it through the local node.
    Put {
        file: PathBuf,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Retrieve a file by ID, local name, or ticket through the local node.
    Get {
        target: String,
        output: PathBuf,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// List locally stored files.
    List {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Show local node status.
    Status {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Show live node health and resource metrics.
    Health {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Show details for a locally stored file.
    Info {
        file_id: String,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Print a HashSeq ticket for a stored file.
    Ticket {
        file_id: String,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Manage known peers.
    Peers {
        #[command(subcommand)]
        command: PeersCommand,
    },
    /// Run the long-lived storage node.
    Node {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
pub enum PeersCommand {
    /// List known peers.
    List {
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
    /// Add a peer by endpoint ID, optionally with a direct IP address.
    Add {
        endpoint_id: String,
        #[arg(long)]
        addr: Option<SocketAddr>,
        #[arg(long, default_value = DEFAULT_DATA_DIR)]
        data_dir: PathBuf,
    },
}

pub async fn run(cli: Cli) -> Result<()> {
    let Cli { api, command } = cli;
    match command {
        Command::Send { file, data_dir } => send(&file, &data_dir).await,
        Command::Receive {
            ticket,
            output,
            data_dir,
        } => receive(&ticket, &output, &data_dir).await,
        Command::Put { file, data_dir } => put(&file, &data_dir, api).await,
        Command::Get {
            target,
            output,
            data_dir,
        } => get(&target, &output, &data_dir, api).await,
        Command::List { data_dir } => list(&data_dir, api).await,
        Command::Status { data_dir } => status(&data_dir, api).await,
        Command::Health { data_dir } => health(&data_dir, api).await,
        Command::Info { file_id, data_dir } => info(&file_id, &data_dir, api).await,
        Command::Ticket { file_id, data_dir } => ticket(&file_id, &data_dir, api).await,
        Command::Peers { command } => peers(command, api).await,
        Command::Node { data_dir } => node(&data_dir, api).await,
    }
}

async fn client(data_dir: &Path, api: Option<SocketAddr>) -> Result<ApiClient> {
    let config = Config::new(data_dir);
    ApiClient::from_config(&config, api)
}

async fn put(file: &Path, data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let outcome = client.put_file(file).await?;

    println!("Stored file:");
    println!("  file id : {}", outcome.file_id.to_hex());
    println!("  name    : {}", outcome.file_name);
    println!("  size    : {} bytes", outcome.file_size);
    println!("  chunks  : {}", outcome.chunk_count);
    println!("  dedup   : {} bytes already present", outcome.bytes_saved);
    Ok(())
}

async fn get(target: &str, output: &Path, data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let outcome = client.fetch_file(target, output).await?;

    println!("Retrieved file:");
    println!("  file id : {}", outcome.file_id.to_hex());
    println!("  name    : {}", outcome.file_name);
    println!("  size    : {} bytes", outcome.file_size);
    println!("  chunks  : {}", outcome.chunk_count);
    println!("  output  : {}", output.display());
    Ok(())
}

async fn list(data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let files = client.list_files().await?;

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

    Ok(())
}

async fn status(data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let status = client.status().await?;

    println!("Node status:");
    println!("  node id : {}", status.node_id);
    println!("  api     : http://{}", status.api_addr);
    println!("  files   : {}", status.files);
    println!("  chunks  : {}", status.chunks);
    println!("  peers   : {}", status.peers);
    println!("  uptime  : {}s", status.uptime_secs);
    Ok(())
}

async fn health(data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let health = client.health().await?;

    println!("Node health:");
    println!("  node id       : {}", health.node_id);
    println!("  free storage  : {} bytes", health.free_storage);
    println!("  cpu usage     : {:.2}%", health.cpu_usage);
    println!("  memory usage  : {:.2}%", health.memory_usage);
    println!("  latency       : {:.2} ms", health.latency);
    println!("  bandwidth     : {:.2} bytes/s", health.bandwidth);
    println!("  reliability   : {:.3}", health.reliability);
    println!("  failure rate  : {:.3}", health.failure_rate);
    println!("  current load  : {:.3}", health.current_load);
    Ok(())
}

async fn info(file_id: &str, data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let details = client.file_details(file_id).await?;

    println!("File details:");
    println!("  file id : {}", details.file_id.to_hex());
    println!("  name    : {}", details.manifest.file_name);
    println!("  size    : {} bytes", details.manifest.file_size);
    println!("  chunks  : {}", details.manifest.chunks.len());
    if let Some(record) = details.record {
        println!("  created : {}", record.created_at_unix);
    }
    if details.replicas.is_empty() {
        println!("  replicas: unknown");
    } else {
        println!("  replicas: {}", details.replicas.join(", "));
    }
    Ok(())
}

async fn ticket(file_id: &str, data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let client = client(data_dir, api).await?;
    let response = client.ticket(file_id).await?;

    println!("Share this ticket:");
    println!("{}", response.ticket);
    Ok(())
}

async fn peers(command: PeersCommand, api: Option<SocketAddr>) -> Result<()> {
    match command {
        PeersCommand::List { data_dir } => {
            let client = client(&data_dir, api).await?;
            let peers = client.list_peers().await?;

            if peers.is_empty() {
                println!("No known peers.");
            } else {
                println!("{:<64}  {:>12}  online", "NODE ID", "LAST SEEN");
                for peer in peers {
                    println!(
                        "{:<64}  {:>12}  {}",
                        peer.endpoint_id, peer.last_seen_unix, peer.online
                    );
                    for address in &peer.addr.addrs {
                        println!("  address: {address}");
                    }
                }
            }
        }
        PeersCommand::Add {
            endpoint_id,
            addr,
            data_dir,
        } => {
            let client = client(&data_dir, api).await?;
            let endpoint_id: EndpointId =
                endpoint_id.parse().context("invalid peer endpoint ID")?;
            let mut endpoint = EndpointAddr::new(endpoint_id);
            if let Some(addr) = addr {
                endpoint.addrs.insert(TransportAddr::Ip(addr));
            }

            let peer = client.add_peer(endpoint).await?;
            println!("Added peer:");
            println!("  node id : {}", peer.endpoint_id);
            for address in &peer.addr.addrs {
                println!("  address : {address}");
            }
        }
    }

    Ok(())
}

async fn node(data_dir: &Path, api: Option<SocketAddr>) -> Result<()> {
    let config = Config::new(data_dir);
    let api_addr = api.unwrap_or_else(|| {
        DEFAULT_API_ADDR
            .parse()
            .expect("default API address must be valid")
    });
    let node = crate::node::Node::start(&config, api_addr).await?;
    let addr = node.endpoint.addr();
    let health = node.metrics.snapshot();

    println!("Node running:");
    println!("  node id  : {}", addr.id);
    println!("  data dir : {}", config.data_dir().display());
    println!("  api      : http://{}", node.api_addr);
    println!("  files    : {}", node.metadata.list_files()?.len());
    println!("  free     : {} bytes", health.free_storage);
    for transport in &addr.addrs {
        println!("  address  : {transport:?}");
    }
    println!("Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    println!("Shutting down.");
    node.shutdown().await?;
    Ok(())
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
