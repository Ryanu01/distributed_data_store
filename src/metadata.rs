use std::path::Path;

use iroh::{EndpointAddr, EndpointId};
use iroh_blobs::Hash;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::error::{Context, Result};

const FILES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("files");
const NAMES: TableDefinition<&str, &[u8]> = TableDefinition::new("names");
const REPLICAS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("replicas");
const PEERS: TableDefinition<&str, &[u8]> = TableDefinition::new("peers");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRecord {
    pub file_id: Hash,
    pub file_name: String,
    pub file_size: u64,
    pub file_hash: Hash,
    pub chunk_count: usize,
    pub created_at_unix: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRecord {
    pub endpoint_id: EndpointId,
    pub addr: EndpointAddr,
    pub last_seen_unix: u64,
    pub online: bool,
}

pub struct Metadata {
    db: Database,
}

impl Metadata {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let db = Database::create(path)
            .with_context(|| format!("failed to open metadata database {}", path.display()))?;

        let write = db
            .begin_write()
            .context("failed to start metadata transaction")?;
        {
            write
                .open_table(FILES)
                .context("failed to open files table")?;
            write
                .open_table(NAMES)
                .context("failed to open names table")?;
            write
                .open_table(REPLICAS)
                .context("failed to open replicas table")?;
            write
                .open_table(PEERS)
                .context("failed to open peers table")?;
        }
        write.commit().context("failed to commit metadata setup")?;

        Ok(Self { db })
    }

    pub fn record_file(&self, record: &FileRecord) -> Result<()> {
        let mut record = record.clone();
        if let Some(existing) = self.get_file(record.file_id)? {
            record.created_at_unix = existing.created_at_unix;
        }

        let value = serde_json::to_vec(&record).context("failed to serialize file record")?;
        let key = record.file_id.as_bytes();

        let write = self
            .db
            .begin_write()
            .context("failed to start metadata transaction")?;
        {
            let mut files = write
                .open_table(FILES)
                .context("failed to open files table")?;
            files
                .insert(key.as_slice(), value.as_slice())
                .context("failed to store file record")?;

            let mut names = write
                .open_table(NAMES)
                .context("failed to open names table")?;
            names
                .insert(record.file_name.as_str(), key.as_slice())
                .context("failed to store file name")?;
        }
        write.commit().context("failed to commit file record")?;

        Ok(())
    }

    pub fn get_file(&self, file_id: Hash) -> Result<Option<FileRecord>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let files = read
            .open_table(FILES)
            .context("failed to open files table")?;

        let value = files
            .get(file_id.as_bytes().as_slice())
            .context("failed to read file record")?;

        value
            .map(|value| {
                serde_json::from_slice(value.value()).context("failed to parse file record")
            })
            .transpose()
    }

    pub fn list_files(&self) -> Result<Vec<FileRecord>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let files = read
            .open_table(FILES)
            .context("failed to open files table")?;

        let mut records = Vec::new();
        for entry in files.iter().context("failed to iterate files table")? {
            let (_, value) = entry.context("failed to read file record")?;
            records.push(
                serde_json::from_slice(value.value()).context("failed to parse file record")?,
            );
        }

        Ok(records)
    }

    pub fn resolve_name(&self, name: &str) -> Result<Option<Hash>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let names = read
            .open_table(NAMES)
            .context("failed to open names table")?;

        let value = names.get(name).context("failed to read file name")?;
        value
            .map(|value| {
                let bytes: [u8; 32] = value
                    .value()
                    .try_into()
                    .context("stored file id has an invalid length")?;
                Ok(Hash::from_bytes(bytes))
            })
            .transpose()
    }

    pub fn add_replica(&self, file_id: Hash, endpoint_id: &str) -> Result<()> {
        let mut replicas = self.replicas(file_id)?;
        if !replicas.iter().any(|existing| existing == endpoint_id) {
            replicas.push(endpoint_id.to_string());
        }

        let value = serde_json::to_vec(&replicas).context("failed to serialize replicas")?;
        let key = file_id.as_bytes();

        let write = self
            .db
            .begin_write()
            .context("failed to start metadata transaction")?;
        {
            let mut table = write
                .open_table(REPLICAS)
                .context("failed to open replicas table")?;
            table
                .insert(key.as_slice(), value.as_slice())
                .context("failed to store replicas")?;
        }
        write.commit().context("failed to commit replicas")?;

        Ok(())
    }

    pub fn replicas(&self, file_id: Hash) -> Result<Vec<String>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let replicas = read
            .open_table(REPLICAS)
            .context("failed to open replicas table")?;

        let value = replicas
            .get(file_id.as_bytes().as_slice())
            .context("failed to read replicas")?;

        value
            .map(|value| serde_json::from_slice(value.value()).context("failed to parse replicas"))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub fn upsert_peer(&self, peer: &PeerRecord) -> Result<()> {
        let value = serde_json::to_vec(peer).context("failed to serialize peer record")?;
        let key = peer.endpoint_id.to_string();

        let write = self
            .db
            .begin_write()
            .context("failed to start metadata transaction")?;
        {
            let mut peers = write
                .open_table(PEERS)
                .context("failed to open peers table")?;
            peers
                .insert(key.as_str(), value.as_slice())
                .context("failed to store peer record")?;
        }
        write.commit().context("failed to commit peer record")?;

        Ok(())
    }

    pub fn get_peer(&self, endpoint_id: &EndpointId) -> Result<Option<PeerRecord>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let peers = read
            .open_table(PEERS)
            .context("failed to open peers table")?;

        let value = peers
            .get(endpoint_id.to_string().as_str())
            .context("failed to read peer record")?;

        value
            .map(|value| {
                serde_json::from_slice(value.value()).context("failed to parse peer record")
            })
            .transpose()
    }

    pub fn list_peers(&self) -> Result<Vec<PeerRecord>> {
        let read = self
            .db
            .begin_read()
            .context("failed to start metadata read")?;
        let peers = read
            .open_table(PEERS)
            .context("failed to open peers table")?;

        let mut records = Vec::new();
        for entry in peers.iter().context("failed to iterate peers table")? {
            let (_, value) = entry.context("failed to read peer record")?;
            records.push(
                serde_json::from_slice(value.value()).context("failed to parse peer record")?,
            );
        }

        Ok(records)
    }

    pub fn mark_peer_seen(
        &self,
        endpoint_id: EndpointId,
        addr: EndpointAddr,
        last_seen_unix: u64,
    ) -> Result<()> {
        let online = self
            .get_peer(&endpoint_id)?
            .map(|record| record.online)
            .unwrap_or(true);

        self.upsert_peer(&PeerRecord {
            endpoint_id,
            addr,
            last_seen_unix,
            online,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn record(file_id: Hash, name: &str) -> FileRecord {
        FileRecord {
            file_id,
            file_name: name.to_string(),
            file_size: 42,
            file_hash: Hash::new(name),
            chunk_count: 1,
            created_at_unix: 123,
        }
    }

    #[test]
    fn stores_and_lists_files() {
        let dir = TempDir::new().unwrap();
        let metadata = Metadata::open(&dir.path().join("metadata.db")).unwrap();
        let file_id = Hash::new(b"file one");
        let expected = record(file_id, "one.bin");

        metadata.record_file(&expected).unwrap();

        assert_eq!(metadata.get_file(file_id).unwrap(), Some(expected.clone()));
        assert_eq!(metadata.list_files().unwrap(), vec![expected]);
    }

    #[test]
    fn resolves_names_to_file_ids() {
        let dir = TempDir::new().unwrap();
        let metadata = Metadata::open(&dir.path().join("metadata.db")).unwrap();
        let file_id = Hash::new(b"named file");

        metadata.record_file(&record(file_id, "named.bin")).unwrap();

        assert_eq!(metadata.resolve_name("named.bin").unwrap(), Some(file_id));
        assert_eq!(metadata.resolve_name("missing.bin").unwrap(), None);
    }

    #[test]
    fn tracks_unique_replicas() {
        let dir = TempDir::new().unwrap();
        let metadata = Metadata::open(&dir.path().join("metadata.db")).unwrap();
        let file_id = Hash::new(b"replicated file");

        metadata.add_replica(file_id, "peer-a").unwrap();
        metadata.add_replica(file_id, "peer-b").unwrap();
        metadata.add_replica(file_id, "peer-a").unwrap();

        assert_eq!(
            metadata.replicas(file_id).unwrap(),
            vec!["peer-a".to_string(), "peer-b".to_string()]
        );
        assert!(metadata.replicas(Hash::new(b"unknown")).unwrap().is_empty());
    }

    #[test]
    fn survives_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("metadata.db");
        let file_id = Hash::new(b"persistent file");

        {
            let metadata = Metadata::open(&path).unwrap();
            metadata
                .record_file(&record(file_id, "persistent.bin"))
                .unwrap();
            metadata.add_replica(file_id, "peer-a").unwrap();
        }

        let metadata = Metadata::open(&path).unwrap();
        assert_eq!(
            metadata.get_file(file_id).unwrap().unwrap().file_name,
            "persistent.bin"
        );
        assert_eq!(metadata.replicas(file_id).unwrap(), vec!["peer-a"]);
    }

    #[test]
    fn tracks_peers() {
        let dir = TempDir::new().unwrap();
        let metadata = Metadata::open(&dir.path().join("metadata.db")).unwrap();
        let secret = iroh::SecretKey::generate();
        let id = secret.public();
        let addr = EndpointAddr::new(id);

        metadata.mark_peer_seen(id, addr.clone(), 123).unwrap();

        let peer = metadata.get_peer(&id).unwrap().unwrap();
        assert_eq!(peer.endpoint_id, id);
        assert_eq!(peer.addr, addr);
        assert_eq!(peer.last_seen_unix, 123);
        assert!(peer.online);
        assert_eq!(metadata.list_peers().unwrap().len(), 1);
    }

    #[test]
    fn peers_survive_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("metadata.db");
        let secret = iroh::SecretKey::generate();
        let id = secret.public();

        {
            let metadata = Metadata::open(&path).unwrap();
            metadata
                .mark_peer_seen(id, EndpointAddr::new(id), 456)
                .unwrap();
        }

        let metadata = Metadata::open(&path).unwrap();
        let peer = metadata.get_peer(&id).unwrap().unwrap();
        assert_eq!(peer.last_seen_unix, 456);
    }
}
