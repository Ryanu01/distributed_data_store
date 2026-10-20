use std::fmt;
use std::sync::Arc;

use iroh::endpoint::presets;
use iroh::protocol::Router;
use iroh::Endpoint;
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::BlobsProtocol;

use crate::config::Config;
use crate::error::{Context, Result};
use crate::identity;
use crate::metadata::Metadata;
use crate::protocol::{ControlHandler, CONTROL_ALPN};

pub struct Node {
    pub endpoint: Endpoint,
    pub store: FsStore,
    pub metadata: Arc<Metadata>,
    router: Router,
}

impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("endpoint", &self.endpoint.id())
            .finish_non_exhaustive()
    }
}

impl Node {
    pub async fn start(config: &Config) -> Result<Self> {
        let secret_key = identity::load_or_create(&config.identity_path())?;

        tokio::fs::create_dir_all(config.data_dir())
            .await
            .with_context(|| {
                format!(
                    "failed to create data directory {}",
                    config.data_dir().display()
                )
            })?;

        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .context("failed to bind iroh endpoint")?;
        endpoint.online().await;

        Self::start_with_endpoint(config, endpoint).await
    }

    pub async fn start_with_endpoint(config: &Config, endpoint: Endpoint) -> Result<Self> {
        let store = open_store(config).await?;
        let metadata = Arc::new(Metadata::open(&config.metadata_path())?);

        let blobs = BlobsProtocol::new(&store, None);
        let control = ControlHandler::new(store.clone(), metadata.clone(), endpoint.clone());

        let router = Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs)
            .accept(CONTROL_ALPN, control)
            .spawn();

        Ok(Self {
            endpoint,
            store,
            metadata,
            router,
        })
    }

    pub async fn shutdown(self) -> Result<()> {
        self.store
            .dump()
            .await
            .context("failed to flush blob store")?;
        self.router
            .shutdown()
            .await
            .context("failed to shut down node router")?;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::address_lookup::memory::MemoryLookup;
    use iroh_blobs::{BlobFormat, Hash};
    use tempfile::TempDir;

    use crate::bundle;
    use crate::metadata::FileRecord;
    use crate::protocol::{ControlClient, ControlRequest, ControlResponse};
    use crate::retrieval;

    fn test_data() -> Vec<u8> {
        let mut data = Vec::with_capacity(1024 * 1024);
        let pattern = b"two node bundle transfer test\n";
        while data.len() < 1024 * 1024 {
            data.extend_from_slice(pattern);
        }
        data
    }

    async fn bind_test_endpoint(config: &Config, lookup: &MemoryLookup) -> Endpoint {
        let secret_key = identity::load_or_create(&config.identity_path()).unwrap();
        Endpoint::builder(presets::Minimal)
            .secret_key(secret_key)
            .address_lookup(lookup.clone())
            .bind()
            .await
            .unwrap()
    }

    async fn start_pair() -> (TempDir, TempDir, Node, Node) {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let config_a = Config::new(dir_a.path());
        let config_b = Config::new(dir_b.path());
        let lookup = MemoryLookup::new();

        let endpoint_a = bind_test_endpoint(&config_a, &lookup).await;
        let endpoint_b = bind_test_endpoint(&config_b, &lookup).await;
        lookup.add_endpoint_info(endpoint_a.addr());
        lookup.add_endpoint_info(endpoint_b.addr());

        let node_a = Node::start_with_endpoint(&config_a, endpoint_a)
            .await
            .unwrap();
        let node_b = Node::start_with_endpoint(&config_b, endpoint_b)
            .await
            .unwrap();

        (dir_a, dir_b, node_a, node_b)
    }

    async fn store_file(node: &Node, dir: &TempDir) -> (Hash, Vec<u8>) {
        let data = test_data();
        let input = dir.path().join("payload.bin");
        std::fs::write(&input, &data).unwrap();

        let put = bundle::put_file(node.store.as_ref(), &input).await.unwrap();
        node.metadata
            .record_file(&FileRecord {
                file_id: put.file_id,
                file_name: put.file_name.clone(),
                file_size: put.file_size,
                file_hash: put.file_hash,
                chunk_count: put.chunk_count,
                created_at_unix: put.created_at_unix,
            })
            .unwrap();
        node.metadata
            .add_replica(put.file_id, &node.endpoint.id().to_string())
            .unwrap();

        (put.file_id, data)
    }

    #[tokio::test]
    async fn transfers_bundle_over_control_protocol() {
        let (dir_a, dir_b, node_a, node_b) = start_pair().await;
        let (file_id, data) = store_file(&node_a, &dir_a).await;

        let client = ControlClient::new(node_b.endpoint.clone(), node_a.endpoint.addr());

        assert!(matches!(
            client.request(&ControlRequest::Ping).await.unwrap(),
            ControlResponse::Pong
        ));

        match client.request(&ControlRequest::Status).await.unwrap() {
            ControlResponse::Status {
                endpoint_id,
                files,
                chunks,
                ..
            } => {
                assert_eq!(endpoint_id, node_a.endpoint.id());
                assert_eq!(files, 1);
                assert!(chunks > 1);
            }
            other => panic!("unexpected status response: {other:?}"),
        }

        match client
            .request(&ControlRequest::ListManifests)
            .await
            .unwrap()
        {
            ControlResponse::Manifests { files } => {
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].file_id, file_id);
            }
            other => panic!("unexpected manifests response: {other:?}"),
        }

        match client
            .request(&ControlRequest::GetManifest { file_id })
            .await
            .unwrap()
        {
            ControlResponse::Manifest { manifest } => {
                assert_eq!(manifest.file_name, "payload.bin");
                assert_eq!(manifest.file_size, data.len() as u64);
            }
            other => panic!("unexpected manifest response: {other:?}"),
        }

        match client
            .request(&ControlRequest::GetManifest {
                file_id: Hash::new(b"missing bundle"),
            })
            .await
            .unwrap()
        {
            ControlResponse::Error { message } => {
                assert!(message.contains("not available locally"));
            }
            other => panic!("unexpected missing manifest response: {other:?}"),
        }

        let ticket = match client
            .request(&ControlRequest::GetTicket { file_id })
            .await
            .unwrap()
        {
            ControlResponse::Ticket { ticket } => ticket,
            other => panic!("unexpected ticket response: {other:?}"),
        };
        assert_eq!(ticket.format(), BlobFormat::HashSeq);
        assert_eq!(ticket.hash(), file_id);
        assert_eq!(ticket.addr().id, node_a.endpoint.id());

        let output = dir_b.path().join("payload.out");
        retrieval::fetch_by_ticket(&node_b.endpoint, node_b.store.as_ref(), &ticket, &output)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), data);

        assert!(bundle::load_manifest(node_b.store.as_ref(), file_id)
            .await
            .is_ok());
        assert_eq!(node_a.metadata.list_peers().unwrap().len(), 1);

        node_b.shutdown().await.unwrap();
        node_a.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_provider_leaves_no_output() {
        let (dir_a, dir_b, node_a, node_b) = start_pair().await;
        let (file_id, _) = store_file(&node_a, &dir_a).await;

        let unavailable = iroh::SecretKey::generate().public();
        let ticket = iroh_blobs::ticket::BlobTicket::new(
            iroh::EndpointAddr::new(unavailable),
            file_id,
            BlobFormat::HashSeq,
        );

        let output = dir_b.path().join("missing.out");
        let result =
            retrieval::fetch_by_ticket(&node_b.endpoint, node_b.store.as_ref(), &ticket, &output)
                .await;
        assert!(result.is_err());
        assert!(!output.exists());

        node_b.shutdown().await.unwrap();
        node_a.shutdown().await.unwrap();
    }
}
