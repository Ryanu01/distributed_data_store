use anyhow::Result;
use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::BlobsProtocol;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let arg_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    match arg_refs.as_slice() {
        ["send", filename] => send(filename).await,
        ["receive", ticket, filename] => receive(ticket, filename).await,
        _ => {
            eprintln!("Usage:");
            eprintln!("  p2p send <file>");
            eprintln!("  p2p receive <ticket> <output_file>");
            std::process::exit(1);
        }
    }
}

async fn send(filename: &str) -> Result<()> {
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

async fn receive(ticket_str: &str, output: &str) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;
    let store = FsStore::load("./p2p_data_receive").await?;

    let ticket: BlobTicket = ticket_str.parse()?;

    println!("Connecting to provider...");
    let connection = endpoint
        .connect(ticket.addr().id, iroh_blobs::ALPN)
        .await?;

    println!("Downloading...");
    let progress = store.remote().fetch(connection, ticket.hash());
    let _stats = progress.await?;
    println!("Download finished.");

    let out_path = std::path::absolute(output)?;
    store.blobs().export(ticket.hash(), out_path).await?;
    println!("File saved to: {output}");

    endpoint.close().await;
    Ok(())
}