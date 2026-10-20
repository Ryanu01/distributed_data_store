use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use iroh::SecretKey;

use crate::error::{bail, Context, Result};

pub fn load_or_create(path: &Path) -> Result<SecretKey> {
    match read_key(path)? {
        Some(key) => {
            restrict_permissions(path)?;
            Ok(key)
        }
        None => create_key(path),
    }
}

fn read_key(path: &Path) -> Result<Option<SecretKey>> {
    match fs::read(path) {
        Ok(bytes) => {
            if bytes.len() != 32 {
                bail!(
                    "identity key at {} must be 32 bytes, found {}",
                    path.display(),
                    bytes.len()
                );
            }

            let mut raw = [0u8; 32];
            raw.copy_from_slice(&bytes);
            Ok(Some(SecretKey::from_bytes(&raw)))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read identity key {}", path.display()))
        }
    }
}

fn create_key(path: &Path) -> Result<SecretKey> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let key = SecretKey::generate();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }

    match options.open(path) {
        Ok(mut file) => {
            file.write_all(&key.to_bytes())
                .with_context(|| format!("failed to write identity key {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("failed to sync identity key {}", path.display()))?;
            restrict_permissions(path)?;
            Ok(key)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_key(path)?.context("identity key disappeared while creating it")
        }
        Err(error) => {
            Err(error).with_context(|| format!("failed to create identity key {}", path.display()))
        }
    }
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = fs::Permissions::from_mode(0o600);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("failed to set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn identity_is_stable_across_loads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.key");

        let first = load_or_create(&path).unwrap();
        let second = load_or_create(&path).unwrap();

        assert_eq!(first.public(), second.public());
        assert_eq!(first.to_bytes(), second.to_bytes());
    }

    #[test]
    fn rejects_wrong_length() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.key");
        std::fs::write(&path, b"short").unwrap();

        assert!(load_or_create(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn key_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("identity.key");
        load_or_create(&path).unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
