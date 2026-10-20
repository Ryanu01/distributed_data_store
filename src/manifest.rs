use std::str::FromStr;

use iroh_blobs::Hash;
use serde::{Deserialize, Serialize};

use crate::chunker::{ChunkedFile, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use crate::error::{bail, Context, Result};

pub const MANIFEST_VERSION: u32 = 1;
pub const MANIFEST_ENTRY: &str = "manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingParams {
    pub min_size: u64,
    pub avg_size: u64,
    pub max_size: u64,
}

impl Default for ChunkingParams {
    fn default() -> Self {
        Self {
            min_size: MIN_CHUNK_SIZE as u64,
            avg_size: AVG_CHUNK_SIZE as u64,
            max_size: MAX_CHUNK_SIZE as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub index: u64,
    pub hash: String,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub file_name: String,
    pub file_size: u64,
    pub file_hash: String,
    pub chunking: ChunkingParams,
    pub chunks: Vec<ChunkRef>,
    /// Reserved for older manifests. New manifests set this to zero so the
    /// content-addressed bundle (and therefore the file ID) stays deterministic.
    /// The real creation time lives in the redb metadata.
    #[serde(default)]
    pub created_at_unix: u64,
}

impl Manifest {
    pub fn from_chunked(file_name: String, chunked: &ChunkedFile) -> Self {
        Self {
            version: MANIFEST_VERSION,
            file_name,
            file_size: chunked.file_size,
            file_hash: chunked.file_hash.to_hex(),
            chunking: ChunkingParams::default(),
            chunks: chunked
                .chunks
                .iter()
                .map(|chunk| ChunkRef {
                    index: chunk.index,
                    hash: chunk.hash.to_hex(),
                    length: chunk.data.len() as u64,
                })
                .collect(),
            created_at_unix: 0,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context("failed to serialize manifest")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let manifest: Manifest =
            serde_json::from_slice(bytes).context("failed to parse manifest")?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != MANIFEST_VERSION {
            bail!(
                "unsupported manifest version {} (expected {})",
                self.version,
                MANIFEST_VERSION
            );
        }

        if self.file_name.is_empty() {
            bail!("manifest file name is empty");
        }

        let mut total_size = 0u64;

        for (position, chunk) in self.chunks.iter().enumerate() {
            if chunk.index != position as u64 {
                bail!(
                    "chunk index {} is out of order at position {}",
                    chunk.index,
                    position
                );
            }

            if chunk.length == 0 {
                bail!("chunk {} has zero length", chunk.index);
            }

            total_size = total_size
                .checked_add(chunk.length)
                .context("chunk size overflow")?;

            Hash::from_str(&chunk.hash)
                .with_context(|| format!("chunk {} has an invalid hash", chunk.index))?;
        }

        if total_size != self.file_size {
            bail!(
                "manifest file size is {} but chunks total {}",
                self.file_size,
                total_size
            );
        }

        Hash::from_str(&self.file_hash).context("manifest has an invalid file hash")?;

        if self.file_size == 0 && !self.chunks.is_empty() {
            bail!("empty manifest must not contain chunks");
        }

        Ok(())
    }
}

pub fn chunk_name(index: u64) -> String {
    format!("chunk-{index:06}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunker::chunk_bytes;

    #[test]
    fn manifest_round_trip() {
        let data = b"manifest round trip".repeat(10_000);
        let chunked = chunk_bytes(&data);
        let manifest = Manifest::from_chunked("test.bin".into(), &chunked);

        let bytes = manifest.to_bytes().unwrap();
        let parsed = Manifest::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.file_name, "test.bin");
        assert_eq!(parsed.file_size, data.len() as u64);
        assert_eq!(parsed.chunks.len(), chunked.chunks.len());
    }

    #[test]
    fn manifest_bytes_are_deterministic() {
        let data = b"deterministic manifest".repeat(10_000);
        let chunked = chunk_bytes(&data);

        let first = Manifest::from_chunked("test.bin".into(), &chunked);
        let second = Manifest::from_chunked("test.bin".into(), &chunked);

        assert_eq!(first.created_at_unix, 0);
        assert_eq!(first.to_bytes().unwrap(), second.to_bytes().unwrap());
    }

    #[test]
    fn manifest_rejects_wrong_total_size() {
        let data = b"hello world".to_vec();
        let chunked = chunk_bytes(&data);
        let mut manifest = Manifest::from_chunked("test.bin".into(), &chunked);
        manifest.file_size += 1;

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn manifest_rejects_out_of_order_chunks() {
        let data = b"hello world".repeat(10_000);
        let chunked = chunk_bytes(&data);
        let mut manifest = Manifest::from_chunked("test.bin".into(), &chunked);

        if manifest.chunks.len() > 1 {
            manifest.chunks.swap(0, 1);
            assert!(manifest.validate().is_err());
        }
    }
}
