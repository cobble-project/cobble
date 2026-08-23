use bytes::Bytes;
use cobble::{
    Config, Error, File, FileSystem, FileSystemRegistry, Result, SequentialWriteFile,
    VolumeUsageKind,
};
use std::sync::Arc;
use uuid::Uuid;

/// File storage used exclusively by the table catalog.
///
/// Catalog paths are relative to `<storage-id>/catalog` on the first Meta volume. Files are
/// published with a temporary write followed by an atomic rename.
#[derive(Clone)]
pub(super) struct CatalogStore {
    file_system: Arc<dyn FileSystem>,
    prefix: String,
}

impl CatalogStore {
    pub(super) fn open(config: &Config, storage_id: &str) -> Result<Self> {
        validate_segment("catalog storage id", storage_id)?;
        let volume = config
            .volumes
            .iter()
            .find(|volume| volume.supports(VolumeUsageKind::Meta))
            .ok_or_else(|| Error::ConfigError("No volume configured for metadata".to_string()))?;
        let file_system = FileSystemRegistry::new().get_or_register_volume(volume)?;
        let store = Self {
            file_system,
            prefix: format!("{storage_id}/catalog"),
        };
        store.ensure_dir(&store.prefix)?;
        Ok(store)
    }

    pub(super) fn write(&self, relative_path: &str, payload: &[u8]) -> Result<()> {
        let path = self.path(relative_path)?;
        if let Some((parent, _)) = path.rsplit_once('/') {
            self.ensure_dir(parent)?;
        }

        let temp_path = format!("{path}.tmp-{}", Uuid::new_v4());
        let mut writer = self.file_system.open_write(&temp_path)?;
        let result = (|| {
            write_all(&mut writer, payload, relative_path)?;
            writer.close()?;
            self.file_system.rename(&temp_path, &path)
        })();
        if result.is_err() {
            let _ = self.file_system.delete(&temp_path);
        }
        result
    }

    pub(super) fn read(&self, relative_path: &str) -> Result<Bytes> {
        let file = self.file_system.open_read(&self.path(relative_path)?)?;
        file.read_at(0, file.size())
    }

    pub(super) fn exists(&self, relative_path: &str) -> Result<bool> {
        self.file_system.exists(&self.path(relative_path)?)
    }

    fn path(&self, relative_path: &str) -> Result<String> {
        validate_relative_path(relative_path)?;
        Ok(format!("{}/{}", self.prefix, relative_path))
    }

    fn ensure_dir(&self, path: &str) -> Result<()> {
        let mut current = String::new();
        for segment in path.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(segment);
            if !self.file_system.exists(&current)? {
                self.file_system.create_dir(&current)?;
            }
        }
        Ok(())
    }
}

fn write_all(
    writer: &mut impl SequentialWriteFile,
    payload: &[u8],
    relative_path: &str,
) -> Result<()> {
    let mut offset = 0;
    while offset < payload.len() {
        let written = writer.write(&payload[offset..])?;
        if written == 0 {
            return Err(Error::IoError(format!(
                "write returned zero bytes for catalog metadata {relative_path}"
            )));
        }
        offset += written;
    }
    Ok(())
}

fn validate_segment(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\']) {
        return Err(Error::ConfigError(format!("invalid {label}: {value}")));
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.starts_with('/')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(Error::ConfigError(format!(
            "invalid catalog metadata path: {value}"
        )));
    }
    Ok(())
}
