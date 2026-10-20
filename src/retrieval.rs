use std::path::Path;

use iroh::Endpoint;
use iroh_blobs::api::downloader::{Downloader, Shuffled};
use iroh_blobs::api::Store;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};

use crate::bundle::{self, GetOutcome};
use crate::error::{bail, Context, Result};

pub fn ticket(endpoint: &Endpoint, file_id: Hash) -> BlobTicket {
    BlobTicket::new(endpoint.addr(), file_id, BlobFormat::HashSeq)
}

#[allow(dead_code)]
pub async fn fetch_bundle(
    endpoint: &Endpoint,
    store: &Store,
    providers: Vec<iroh::EndpointId>,
    file_id: Hash,
) -> Result<()> {
    if providers.is_empty() {
        bail!("no providers available for {}", file_id.to_hex());
    }

    let downloader = Downloader::new(store, endpoint);
    downloader
        .download(HashAndFormat::hash_seq(file_id), Shuffled::new(providers))
        .await
        .with_context(|| format!("failed to download bundle {}", file_id.to_hex()))?;

    Ok(())
}

#[allow(dead_code)]
pub async fn fetch_by_ticket(
    endpoint: &Endpoint,
    store: &Store,
    ticket: &BlobTicket,
    output: &Path,
) -> Result<GetOutcome> {
    if ticket.format() != BlobFormat::HashSeq {
        bail!("ticket does not point to a chunked bundle");
    }

    fetch_bundle(endpoint, store, vec![ticket.addr().id], ticket.hash()).await?;

    let (manifest, _) = bundle::load_manifest(store, ticket.hash()).await?;
    let value = HashAndFormat::hash_seq(ticket.hash());

    store
        .tags()
        .set(format!("file/{}", ticket.hash().to_hex()).as_bytes(), value)
        .await
        .context("failed to tag downloaded bundle")?;
    store
        .tags()
        .set(format!("name/{}", manifest.file_name).as_bytes(), value)
        .await
        .context("failed to tag downloaded file name")?;

    bundle::get_file(store, ticket.hash(), output).await
}
