use std::path::Path;

use fastcdc::v2020::FastCDC;
use iroh_blobs::Hash;

use crate::error::{Context, Result};

pub const MIN_CHUNK_SIZE: usize = 16 * 1024;
pub const AVG_CHUNK_SIZE: usize = 64 * 1024;
pub const MAX_CHUNK_SIZE: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub index: u64,
    pub data: Vec<u8>,
    pub hash: Hash,
}

#[derive(Debug)]
pub struct ChunkedFile {
    pub chunks: Vec<Chunk>,
    pub file_hash: Hash,
    pub file_size: u64,
}

pub fn chunk_file(path: &Path) -> Result<ChunkedFile> {
    let data =
        std::fs::read(path).with_context(|| format!("failed to read file {}", path.display()))?;
    Ok(chunk_bytes(&data))
}

pub fn chunk_bytes(data: &[u8]) -> ChunkedFile {
    let chunks = FastCDC::new(data, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE)
        .enumerate()
        .map(|(index, chunk)| {
            let bytes = data[chunk.offset..chunk.offset + chunk.length].to_vec();
            Chunk {
                index: index as u64,
                hash: Hash::new(&bytes),
                data: bytes,
            }
        })
        .collect();

    ChunkedFile {
        chunks,
        file_hash: Hash::new(data),
        file_size: data.len() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_data() -> Vec<u8> {
        let mut data = Vec::with_capacity(2 * 1024 * 1024);
        let pattern = b"the quick brown fox jumps over the lazy dog\n";
        while data.len() < 2 * 1024 * 1024 {
            data.extend_from_slice(pattern);
        }
        data
    }

    #[test]
    fn chunks_cover_entire_input() {
        let data = sample_data();
        let chunked = chunk_bytes(&data);

        assert!(!chunked.chunks.is_empty());
        assert_eq!(chunked.file_size, data.len() as u64);
        assert_eq!(chunked.file_hash, Hash::new(&data));

        let mut rebuilt = Vec::with_capacity(data.len());
        for (index, chunk) in chunked.chunks.iter().enumerate() {
            assert_eq!(chunk.index, index as u64);
            if index + 1 < chunked.chunks.len() {
                assert!(chunk.data.len() >= MIN_CHUNK_SIZE);
            }
            assert!(chunk.data.len() <= MAX_CHUNK_SIZE);
            assert_eq!(chunk.hash, Hash::new(&chunk.data));
            rebuilt.extend_from_slice(&chunk.data);
        }

        assert_eq!(rebuilt, data);
    }

    #[test]
    fn chunking_is_deterministic() {
        let data = sample_data();
        let first = chunk_bytes(&data);
        let second = chunk_bytes(&data);

        assert_eq!(first.chunks.len(), second.chunks.len());
        for (a, b) in first.chunks.iter().zip(second.chunks.iter()) {
            assert_eq!(a.data, b.data);
            assert_eq!(a.hash, b.hash);
        }
    }

    #[test]
    fn empty_input_has_no_chunks() {
        let chunked = chunk_bytes(&[]);

        assert!(chunked.chunks.is_empty());
        assert_eq!(chunked.file_size, 0);
        assert_eq!(chunked.file_hash, Hash::EMPTY);
    }

    #[test]
    fn small_input_has_one_chunk() {
        let data = b"small file".to_vec();
        let chunked = chunk_bytes(&data);

        assert_eq!(chunked.chunks.len(), 1);
        assert_eq!(chunked.chunks[0].data, data);
    }
}
