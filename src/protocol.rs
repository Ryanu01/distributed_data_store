use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::protocol::{AcceptError, ProtocolHandler};
use iroh::{Endpoint, EndpointAddr};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::ticket::BlobTicket;
use iroh_blobs::Hash;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::bundle::{self, FileSummary};
use crate::error::{bail, Context, Result};
use crate::manifest::Manifest;
use crate::metadata::Metadata;

pub const CONTROL_ALPN: &[u8] = b"p2p-storage/control/1";
pub const CONTROL_VERSION: u32 = 1;
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlRequest {
    Hello {
        version: u32,
        endpoint: EndpointAddr,
    },
    Ping,
    Status,
    ListManifests,
    GetManifest {
        file_id: Hash,
    },
    GetTicket {
        file_id: Hash,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlResponse {
    HelloAck {
        version: u32,
        files: u64,
        chunks: u64,
    },
    Pong,
    Status {
        endpoint_id: iroh::EndpointId,
        files: u64,
        chunks: u64,
        peers: u64,
    },
    Manifests {
        files: Vec<FileSummary>,
    },
    Manifest {
        manifest: Manifest,
    },
    Ticket {
        ticket: BlobTicket,
    },
    Error {
        message: String,
    },
}

fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(value).context("failed to encode control frame")?;
    if payload.len() > MAX_FRAME_SIZE {
        bail!(
            "control frame is too large: {} bytes (max {})",
            payload.len(),
            MAX_FRAME_SIZE
        );
    }

    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn decode_frame<T: DeserializeOwned>(frame: &[u8]) -> Result<T> {
    if frame.len() < 4 {
        bail!("control frame is missing its length prefix");
    }

    let len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if len > MAX_FRAME_SIZE {
        bail!(
            "control frame is too large: {} bytes (max {})",
            len,
            MAX_FRAME_SIZE
        );
    }
    if frame.len() != 4 + len {
        bail!(
            "control frame length mismatch: expected {} bytes, got {}",
            len,
            frame.len() - 4
        );
    }

    serde_json::from_slice(&frame[4..]).context("failed to decode control frame")
}

async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<()> {
    let frame = encode_frame(value)?;
    send.write_all(&frame)
        .await
        .context("failed to write control frame")
}

async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T> {
    let mut len_bytes = [0u8; 4];
    recv.read_exact(&mut len_bytes)
        .await
        .context("failed to read control frame length")?;

    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > MAX_FRAME_SIZE {
        bail!(
            "control frame is too large: {} bytes (max {})",
            len,
            MAX_FRAME_SIZE
        );
    }

    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .context("failed to read control frame payload")?;

    let mut frame = Vec::with_capacity(4 + len);
    frame.extend_from_slice(&len_bytes);
    frame.extend_from_slice(&payload);
    decode_frame(&frame)
}

#[derive(Clone)]
pub struct ControlHandler {
    store: FsStore,
    metadata: std::sync::Arc<Metadata>,
    endpoint: Endpoint,
}

impl fmt::Debug for ControlHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ControlHandler")
            .field("endpoint", &self.endpoint.id())
            .finish_non_exhaustive()
    }
}

impl ControlHandler {
    pub fn new(store: FsStore, metadata: std::sync::Arc<Metadata>, endpoint: Endpoint) -> Self {
        Self {
            store,
            metadata,
            endpoint,
        }
    }

    async fn handle_stream(&self, mut send: SendStream, mut recv: RecvStream) -> Result<()> {
        let handshake: ControlRequest = read_frame(&mut recv).await?;
        let (version, endpoint) = match handshake {
            ControlRequest::Hello { version, endpoint } => (version, endpoint),
            other => bail!("expected a hello handshake, got {other:?}"),
        };

        if version != CONTROL_VERSION {
            write_frame(
                &mut send,
                &ControlResponse::Error {
                    message: format!(
                        "unsupported control version {version}; this node speaks {CONTROL_VERSION}"
                    ),
                },
            )
            .await?;
            send.finish().context("failed to finish control stream")?;
            return Ok(());
        }

        self.metadata
            .mark_peer_seen(endpoint.id, endpoint, now_unix())?;

        let files = bundle::list_files(self.store.as_ref(), &self.metadata).await?;
        write_frame(
            &mut send,
            &ControlResponse::HelloAck {
                version: CONTROL_VERSION,
                files: files.len() as u64,
                chunks: files.iter().map(|file| file.chunk_count as u64).sum(),
            },
        )
        .await?;

        let request: ControlRequest = read_frame(&mut recv).await?;
        let response = match self.handle_request(request).await {
            Ok(response) => response,
            Err(error) => ControlResponse::Error {
                message: format!("{error:#}"),
            },
        };
        write_frame(&mut send, &response).await?;
        send.finish().context("failed to finish control stream")?;

        Ok(())
    }

    async fn handle_request(&self, request: ControlRequest) -> Result<ControlResponse> {
        match request {
            ControlRequest::Hello { .. } => bail!("hello is only valid as a handshake"),
            ControlRequest::Ping => Ok(ControlResponse::Pong),
            ControlRequest::Status => {
                let files = bundle::list_files(self.store.as_ref(), &self.metadata).await?;
                Ok(ControlResponse::Status {
                    endpoint_id: self.endpoint.id(),
                    files: files.len() as u64,
                    chunks: files.iter().map(|file| file.chunk_count as u64).sum(),
                    peers: self.metadata.list_peers()?.len() as u64,
                })
            }
            ControlRequest::ListManifests => {
                let files = bundle::list_files(self.store.as_ref(), &self.metadata).await?;
                Ok(ControlResponse::Manifests { files })
            }
            ControlRequest::GetManifest { file_id } => {
                let (manifest, _) = bundle::load_manifest(self.store.as_ref(), file_id).await?;
                Ok(ControlResponse::Manifest { manifest })
            }
            ControlRequest::GetTicket { file_id } => {
                bundle::load_manifest(self.store.as_ref(), file_id).await?;
                let ticket = crate::retrieval::ticket(&self.endpoint, file_id);
                Ok(ControlResponse::Ticket { ticket })
            }
        }
    }
}

impl ProtocolHandler for ControlHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::debug!(
                        remote = %connection.remote_id().fmt_short(),
                        "control connection closed: {error}"
                    );
                    break;
                }
            };

            if let Err(error) = self.handle_stream(send, recv).await {
                tracing::warn!(
                    remote = %connection.remote_id().fmt_short(),
                    "control request failed: {error:#}"
                );
            }
        }

        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct ControlClient {
    endpoint: Endpoint,
    addr: EndpointAddr,
}

#[allow(dead_code)]
impl ControlClient {
    pub fn new(endpoint: Endpoint, addr: EndpointAddr) -> Self {
        Self { endpoint, addr }
    }

    pub fn addr(&self) -> &EndpointAddr {
        &self.addr
    }

    pub async fn request(&self, request: &ControlRequest) -> Result<ControlResponse> {
        let connection = self
            .endpoint
            .connect(self.addr.clone(), CONTROL_ALPN)
            .await
            .with_context(|| format!("failed to connect to {}", self.addr.id.fmt_short()))?;

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .context("failed to open control stream")?;

        write_frame(
            &mut send,
            &ControlRequest::Hello {
                version: CONTROL_VERSION,
                endpoint: self.endpoint.addr(),
            },
        )
        .await?;
        write_frame(&mut send, request).await?;
        send.finish().context("failed to finish control stream")?;

        let hello: ControlResponse = read_frame(&mut recv).await?;
        match hello {
            ControlResponse::HelloAck { version, .. } if version == CONTROL_VERSION => {}
            ControlResponse::Error { message } => bail!("{message}"),
            other => bail!("unexpected control handshake response: {other:?}"),
        }

        let response: ControlResponse = read_frame(&mut recv).await?;
        connection.close(0u32.into(), b"done");
        Ok(response)
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_frames() {
        let encoded = encode_frame(&ControlRequest::Ping).unwrap();
        let decoded: ControlRequest = decode_frame(&encoded).unwrap();
        assert!(matches!(decoded, ControlRequest::Ping));
    }

    #[test]
    fn rejects_truncated_frames() {
        assert!(decode_frame::<ControlRequest>(&[0, 0]).is_err());

        let mut encoded = encode_frame(&ControlRequest::Ping).unwrap();
        encoded.pop();
        assert!(decode_frame::<ControlRequest>(&encoded).is_err());
    }

    #[test]
    fn rejects_oversized_frames() {
        let bytes = ((MAX_FRAME_SIZE + 1) as u32).to_be_bytes();
        assert!(decode_frame::<ControlRequest>(&bytes).is_err());
    }
}
