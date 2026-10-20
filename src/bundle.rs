use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use iroh_blobs::api::Store;
use iroh_blobs::format::collection::Collection;
use iroh_blobs::{Hash, HashAndFormat};
use tokio::io::AsyncWriteExt;

use crate::chunker::chunk_file;
use crate::error::{bail, Context, Result};
use crate::manifest::{chunk_name, Manifest, MANIFEST_ENTRY};
use crate::metadata::Metadata;

#[derive(Debug)]
pub struct PutOutcome {
    pub file_id: Hash,
    pub file_name: String,
    pub file_size: u64,
    pub file_hash: Hash,
    pub chunk_count: usize,
    pub created_at_unix: u64,
    pub bytes_saved: u64,
}

#[derive(Debug)]
pub struct GetOutcome {
    pub file_id: Hash,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_count: usize,
}

#[derive(Debug)]
pub struct FileSummary {
    pub file_id: Hash,
    pub file_name: String,
    pub file_size: u64,
    pub chunk_count: usize,
    pub created_at_unix: u64,
}

pub async fn put_file(store: &Store, path: &Path) -> Result<PutOutcome> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("invalid file name: {}", path.display()))?
        .to_string();

    let read_path = path.to_path_buf();
    let chunked = tokio::task::spawn_blocking(move || chunk_file(&read_path))
        .await
        .context("chunking task failed")??;

    let created_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the unix epoch")?
        .as_secs();

    let manifest = Manifest::from_chunked(file_name.clone(), &chunked);
    let manifest_bytes = manifest.to_bytes()?;

    let manifest_tag = store
        .blobs()
        .add_bytes(manifest_bytes)
        .temp_tag()
        .await
        .context("failed to store manifest")?;

    let mut collection_items = Vec::with_capacity(chunked.chunks.len() + 1);
    collection_items.push((MANIFEST_ENTRY.to_string(), manifest_tag.hash()));

    let mut chunk_tags = Vec::with_capacity(chunked.chunks.len());
    let mut bytes_saved = 0u64;

    for chunk in chunked.chunks {
        if store
            .blobs()
            .has(chunk.hash)
            .await
            .context("failed to inspect chunk store")?
        {
            bytes_saved += chunk.data.len() as u64;
        }

        let tag = store
            .blobs()
            .add_bytes(chunk.data)
            .temp_tag()
            .await
            .with_context(|| format!("failed to store chunk {}", chunk.index))?;

        collection_items.push((chunk_name(chunk.index), tag.hash()));
        chunk_tags.push(tag);
    }

    let collection = Collection::from_iter(collection_items);
    let root_tag = collection
        .store(store)
        .await
        .context("failed to store bundle collection")?;
    let file_id = root_tag.hash();
    let value = HashAndFormat::hash_seq(file_id);

    store
        .tags()
        .set(format!("file/{}", file_id.to_hex()).as_bytes(), value)
        .await
        .context("failed to tag file bundle")?;

    let name_tag = format!("name/{file_name}");
    store
        .tags()
        .set(name_tag.as_bytes(), value)
        .await
        .context("failed to tag file name")?;

    drop(chunk_tags);
    drop(root_tag);
    drop(manifest_tag);

    Ok(PutOutcome {
        file_id,
        file_name,
        file_size: chunked.file_size,
        file_hash: chunked.file_hash,
        chunk_count: manifest.chunks.len(),
        created_at_unix,
        bytes_saved,
    })
}

pub async fn get_file(store: &Store, file_id: Hash, output: &Path) -> Result<GetOutcome> {
    let (manifest, _entries) = load_manifest(store, file_id).await?;

    let tmp_path = temporary_output_path(output);
    let write_result = write_bundle(store, &manifest, &tmp_path).await;

    if let Err(error) = write_result {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(error);
    }

    tokio::fs::rename(&tmp_path, output)
        .await
        .with_context(|| format!("failed to move output into {}", output.display()))?;

    Ok(GetOutcome {
        file_id,
        file_name: manifest.file_name,
        file_size: manifest.file_size,
        chunk_count: manifest.chunks.len(),
    })
}

pub async fn list_files(store: &Store, metadata: &Metadata) -> Result<Vec<FileSummary>> {
    let mut files: HashMap<Hash, FileSummary> = HashMap::new();

    for record in metadata.list_files()? {
        files.insert(
            record.file_id,
            FileSummary {
                file_id: record.file_id,
                file_name: record.file_name,
                file_size: record.file_size,
                chunk_count: record.chunk_count,
                created_at_unix: record.created_at_unix,
            },
        );
    }

    let mut stream = store
        .tags()
        .list_prefix("file/")
        .await
        .context("failed to list file tags")?;

    while let Some(item) = stream.next().await {
        let tag = item.context("failed to read file tag")?;

        if files.contains_key(&tag.hash) {
            continue;
        }

        match load_manifest(store, tag.hash).await {
            Ok((manifest, _)) => {
                files.insert(
                    tag.hash,
                    FileSummary {
                        file_id: tag.hash,
                        file_name: manifest.file_name,
                        file_size: manifest.file_size,
                        chunk_count: manifest.chunks.len(),
                        created_at_unix: manifest.created_at_unix,
                    },
                );
            }
            Err(error) => {
                eprintln!(
                    "warning: skipping unreadable bundle {}: {error:#}",
                    tag.hash.to_hex()
                );
            }
        }
    }

    let mut files: Vec<_> = files.into_values().collect();
    files.sort_by(|a, b| {
        a.file_name
            .cmp(&b.file_name)
            .then_with(|| a.file_id.to_hex().cmp(&b.file_id.to_hex()))
    });
    Ok(files)
}

pub async fn resolve_file_id(store: &Store, input: &str) -> Result<Hash> {
    if input.len() == 64 && input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Hash::from_str(input).context("invalid file id");
    }

    if input.len() == 65
        && (input.starts_with('s') || input.starts_with('S'))
        && input[1..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Hash::from_str(&input[1..]).context("invalid file id");
    }

    let tag = store
        .tags()
        .get(format!("name/{input}").as_bytes())
        .await
        .context("failed to look up file name")?;

    match tag {
        Some(tag) => Ok(tag.hash),
        None => bail!("no local file named '{input}'"),
    }
}

async fn load_manifest(store: &Store, file_id: Hash) -> Result<(Manifest, BTreeMap<String, Hash>)> {
    let collection = Collection::load(file_id, store)
        .await
        .with_context(|| format!("file is not available locally: {}", file_id.to_hex()))?;

    let mut manifest_hash = None;
    let mut entries = BTreeMap::new();

    for (name, hash) in collection {
        if name == MANIFEST_ENTRY {
            manifest_hash = Some(hash);
        } else {
            entries.insert(name, hash);
        }
    }

    let manifest_hash = manifest_hash.context("bundle is missing manifest.json")?;
    let manifest_bytes = store
        .blobs()
        .get_bytes(manifest_hash)
        .await
        .context("failed to read manifest")?;
    let manifest = Manifest::from_bytes(&manifest_bytes)?;

    if manifest.chunks.len() != entries.len() {
        bail!(
            "manifest lists {} chunks but bundle contains {}",
            manifest.chunks.len(),
            entries.len()
        );
    }

    for chunk in &manifest.chunks {
        let name = chunk_name(chunk.index);
        let expected_hash = Hash::from_str(&chunk.hash)
            .with_context(|| format!("invalid chunk hash in manifest for {}", chunk.index))?;

        let actual_hash = entries
            .get(&name)
            .with_context(|| format!("bundle is missing {name}"))?;

        if *actual_hash != expected_hash {
            bail!("chunk {name} does not match the manifest");
        }
    }

    Ok((manifest, entries))
}

async fn write_bundle(store: &Store, manifest: &Manifest, output: &Path) -> Result<()> {
    let mut file = tokio::fs::File::create(output)
        .await
        .with_context(|| format!("failed to create {}", output.display()))?;

    let mut hasher = blake3::Hasher::new();
    let mut written = 0u64;

    for chunk in &manifest.chunks {
        let hash = Hash::from_str(&chunk.hash)
            .with_context(|| format!("invalid chunk hash for chunk {}", chunk.index))?;

        let bytes = store
            .blobs()
            .get_bytes(hash)
            .await
            .with_context(|| format!("failed to read chunk {}", chunk.index))?;

        if bytes.len() as u64 != chunk.length {
            bail!(
                "chunk {} has length {} but the manifest says {}",
                chunk.index,
                bytes.len(),
                chunk.length
            );
        }

        if Hash::new(&bytes) != hash {
            bail!("chunk {} failed hash verification", chunk.index);
        }

        hasher.update(&bytes);
        file.write_all(&bytes)
            .await
            .with_context(|| format!("failed to write chunk {}", chunk.index))?;
        written += bytes.len() as u64;
    }

    file.flush().await.context("failed to flush output")?;
    file.sync_all().await.context("failed to sync output")?;
    drop(file);

    if written != manifest.file_size {
        bail!(
            "wrote {} bytes but the manifest says {}",
            written,
            manifest.file_size
        );
    }

    let actual_file_hash = hasher.finalize().to_hex().to_string();
    if actual_file_hash != manifest.file_hash {
        bail!(
            "file hash mismatch: expected {}, got {}",
            manifest.file_hash,
            actual_file_hash
        );
    }

    Ok(())
}

fn temporary_output_path(output: &Path) -> PathBuf {
    let file_name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("download");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();

    let temp_name = format!(".{file_name}.tmp-{}-{nonce}", std::process::id());
    output.with_file_name(temp_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FileRecord;
    use iroh_blobs::store::fs::FsStore;
    use tempfile::TempDir;

    async fn open_store(dir: &TempDir) -> FsStore {
        FsStore::load(dir.path().join("store"))
            .await
            .expect("failed to open store")
    }

    fn open_metadata(dir: &TempDir) -> Metadata {
        Metadata::open(&dir.path().join("metadata.db")).expect("failed to open metadata")
    }

    fn multi_chunk_data() -> Vec<u8> {
        let mut data = Vec::with_capacity(1024 * 1024);
        let pattern = b"p2p distributed storage chunk test\n";
        while data.len() < 1024 * 1024 {
            data.extend_from_slice(pattern);
        }
        data
    }

    #[tokio::test]
    async fn put_and_get_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = open_store(&dir).await;

        let input = dir.path().join("input.bin");
        let output = dir.path().join("output.bin");
        let data = multi_chunk_data();
        std::fs::write(&input, &data).unwrap();

        let put = put_file(store.as_ref(), &input).await.unwrap();
        assert_eq!(put.file_size, data.len() as u64);
        assert!(put.chunk_count > 1);

        let get = get_file(store.as_ref(), put.file_id, &output)
            .await
            .unwrap();
        assert_eq!(get.file_id, put.file_id);
        assert_eq!(std::fs::read(&output).unwrap(), data);

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn identical_file_is_deduplicated() {
        let dir = TempDir::new().unwrap();
        let store = open_store(&dir).await;

        let input = dir.path().join("input.bin");
        let data = multi_chunk_data();
        std::fs::write(&input, &data).unwrap();

        let first = put_file(store.as_ref(), &input).await.unwrap();
        assert_eq!(first.bytes_saved, 0);

        let second = put_file(store.as_ref(), &input).await.unwrap();
        assert_eq!(second.bytes_saved, data.len() as u64);
        assert_eq!(first.file_id, second.file_id);

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn empty_file_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = open_store(&dir).await;

        let input = dir.path().join("empty.bin");
        let output = dir.path().join("empty.out");
        std::fs::write(&input, []).unwrap();

        let put = put_file(store.as_ref(), &input).await.unwrap();
        assert_eq!(put.file_size, 0);
        assert_eq!(put.chunk_count, 0);

        get_file(store.as_ref(), put.file_id, &output)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), Vec::<u8>::new());

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn bundle_survives_store_restart() {
        let dir = TempDir::new().unwrap();
        let data = multi_chunk_data();
        let input = dir.path().join("input.bin");
        let output = dir.path().join("output.bin");
        std::fs::write(&input, &data).unwrap();

        let file_id = {
            let store = open_store(&dir).await;
            let put = put_file(store.as_ref(), &input).await.unwrap();
            store.shutdown().await.unwrap();
            put.file_id
        };

        let store = open_store(&dir).await;
        get_file(store.as_ref(), file_id, &output).await.unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), data);

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn get_by_name() {
        let dir = TempDir::new().unwrap();
        let store = open_store(&dir).await;

        let input = dir.path().join("named.txt");
        let output = dir.path().join("named.out");
        std::fs::write(&input, b"named lookup").unwrap();

        put_file(store.as_ref(), &input).await.unwrap();
        let file_id = resolve_file_id(store.as_ref(), "named.txt").await.unwrap();
        get_file(store.as_ref(), file_id, &output).await.unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), b"named lookup");

        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn list_reports_stored_files() {
        let dir = TempDir::new().unwrap();
        let store = open_store(&dir).await;
        let metadata = open_metadata(&dir);

        let recorded_input = dir.path().join("recorded.txt");
        std::fs::write(&recorded_input, b"recorded").unwrap();
        let recorded = put_file(store.as_ref(), &recorded_input).await.unwrap();
        metadata
            .record_file(&FileRecord {
                file_id: recorded.file_id,
                file_name: recorded.file_name.clone(),
                file_size: recorded.file_size,
                file_hash: recorded.file_hash,
                chunk_count: recorded.chunk_count,
                created_at_unix: recorded.created_at_unix,
            })
            .unwrap();

        let tag_only_input = dir.path().join("tag-only.txt");
        std::fs::write(&tag_only_input, b"tag only").unwrap();
        put_file(store.as_ref(), &tag_only_input).await.unwrap();

        let files = list_files(store.as_ref(), &metadata).await.unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].file_name, "recorded.txt");
        assert_eq!(files[1].file_name, "tag-only.txt");

        store.shutdown().await.unwrap();
    }
}
