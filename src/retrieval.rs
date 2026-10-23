use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::api::Store;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};

use crate::bundle::{self, GetOutcome};
use crate::error::{bail, Context, Result};
use crate::metadata::{FileRecord, Metadata, PeerRecord};

const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub fn ticket(endpoint: &Endpoint, file_id: Hash) -> BlobTicket {
    BlobTicket::new(endpoint.addr(), file_id, BlobFormat::HashSeq)
}

pub async fn fetch_bundle_from(
    endpoint: &Endpoint,
    store: &Store,
    provider: &EndpointAddr,
    file_id: Hash,
) -> Result<()> {
    let connection = tokio::time::timeout(
        PROVIDER_CONNECT_TIMEOUT,
        endpoint.connect(provider.clone(), iroh_blobs::ALPN),
    )
    .await
    .with_context(|| {
        format!(
            "timed out connecting to provider {} for bundle {}",
            provider.id.fmt_short(),
            file_id.to_hex()
        )
    })?
    .with_context(|| {
        format!(
            "failed to connect to provider {} for bundle {}",
            provider.id.fmt_short(),
            file_id.to_hex()
        )
    })?;

    let result = store
        .remote()
        .fetch(connection.clone(), HashAndFormat::hash_seq(file_id))
        .await
        .with_context(|| {
            format!(
                "failed to download bundle {} from provider {}",
                file_id.to_hex(),
                provider.id.fmt_short()
            )
        });

    connection.close(0u32.into(), b"bundle transfer complete");
    result?;
    Ok(())
}

pub async fn fetch_from_providers(
    endpoint: &Endpoint,
    store: &Store,
    providers: &[EndpointAddr],
    file_id: Hash,
) -> Result<EndpointAddr> {
    if providers.is_empty() {
        bail!("no providers available for {}", file_id.to_hex());
    }

    let mut failures = Vec::with_capacity(providers.len());
    for provider in providers {
        match fetch_bundle_from(endpoint, store, provider, file_id).await {
            Ok(()) => return Ok(provider.clone()),
            Err(error) => {
                tracing::warn!(
                    provider = %provider.id.fmt_short(),
                    "provider failed for {}: {error:#}",
                    file_id.to_hex()
                );
                failures.push(format!("{}: {error:#}", provider.id.fmt_short()));
            }
        }
    }

    bail!(
        "failed to retrieve bundle {} from {} provider(s): {}",
        file_id.to_hex(),
        failures.len(),
        failures.join("; ")
    )
}

pub async fn finalize_download(
    endpoint: &Endpoint,
    store: &Store,
    metadata: &Metadata,
    provider: &EndpointAddr,
    file_id: Hash,
    output: &Path,
) -> Result<GetOutcome> {
    let (manifest, _) = bundle::load_manifest(store, file_id).await?;
    let value = HashAndFormat::hash_seq(file_id);

    store
        .tags()
        .set(format!("file/{}", file_id.to_hex()).as_bytes(), value)
        .await
        .context("failed to tag downloaded bundle")?;
    store
        .tags()
        .set(format!("name/{}", manifest.file_name).as_bytes(), value)
        .await
        .context("failed to tag downloaded file name")?;

    metadata.record_file(&FileRecord {
        file_id,
        file_name: manifest.file_name.clone(),
        file_size: manifest.file_size,
        file_hash: manifest
            .file_hash
            .parse()
            .context("downloaded manifest contains an invalid file hash")?,
        chunk_count: manifest.chunks.len(),
        created_at_unix: now_unix(),
    })?;
    metadata.add_replica(file_id, &endpoint.id().to_string())?;
    metadata.upsert_peer(&PeerRecord {
        endpoint_id: provider.id,
        addr: provider.clone(),
        last_seen_unix: now_unix(),
        online: true,
    })?;

    bundle::get_file(store, file_id, output).await
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
