use std::path::{Path, PathBuf};

pub const DEFAULT_DATA_DIR: &str = "./p2p_data";
pub const DEFAULT_RECEIVE_DIR: &str = "./p2p_data_receive";
pub const IDENTITY_FILE: &str = "identity.key";
pub const METADATA_FILE: &str = "metadata.db";

#[derive(Debug, Clone)]
pub struct Config {
    data_dir: PathBuf,
}

impl Config {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn store_path(&self) -> &Path {
        &self.data_dir
    }

    pub fn identity_path(&self) -> PathBuf {
        self.data_dir.join(IDENTITY_FILE)
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.data_dir.join(METADATA_FILE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_paths_from_data_dir() {
        let config = Config::new("/tmp/p2p-test");

        assert_eq!(config.data_dir(), Path::new("/tmp/p2p-test"));
        assert_eq!(config.store_path(), Path::new("/tmp/p2p-test"));
        assert_eq!(
            config.identity_path(),
            Path::new("/tmp/p2p-test/identity.key")
        );
        assert_eq!(
            config.metadata_path(),
            Path::new("/tmp/p2p-test/metadata.db")
        );
    }
}
