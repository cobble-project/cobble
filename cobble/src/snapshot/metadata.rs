use super::manifest::{ManifestPayload, decode_manifest};
use crate::config::{Config, VolumeUsageKind};
use crate::coordinator::GlobalSnapshotManifest;
use crate::error::{Error, Result};
use crate::file::{FileSystemRegistry, MetadataReader};
use crate::paths::schema_file_path_from_snapshot_manifest_path;
use crate::schema::{SnapshotColumnFamily, snapshot_column_families_from_payload};
use crate::util::normalize_storage_path_to_url;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::path::Path;
use url::Url;

/// Complete transient metadata captured by one shard snapshot.
///
/// Unlike [`crate::coordinator::ShardSnapshotRef`], this includes the captured column-family
/// schema metadata used by the shard and is never embedded in a persisted global snapshot manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShardSnapshotMetadata {
    pub ranges: Vec<RangeInclusive<u16>>,
    pub db_id: String,
    pub snapshot_id: u64,
    pub manifest_path: String,
    pub timestamp_seconds: u32,
    pub data_size_bytes: u64,
    pub incremental_data_size_bytes: u64,
    pub schema_id: u64,
    pub column_families: BTreeMap<String, SnapshotColumnFamily>,
}

impl ShardSnapshotMetadata {
    /// Return the column-family ids represented by the captured schema.
    #[must_use]
    pub fn column_family_ids(&self) -> BTreeMap<String, u8> {
        self.column_families
            .iter()
            .map(|(name, family)| (name.clone(), family.id))
            .collect()
    }
}

/// Load complete metadata referenced by one shard snapshot manifest.
///
/// This opens only the manifest and schema files through the configured metadata volume. It does
/// not initialize a database, open SST files, resolve merge operators, or follow incremental
/// manifest dependencies. The caller supplies `db_id` because shard manifests do not persist an
/// identity.
pub fn load_shard_snapshot_metadata(
    config: &Config,
    db_id: &str,
    manifest_path: &str,
) -> Result<ShardSnapshotMetadata> {
    if !is_absolute_storage_path(manifest_path) {
        return Err(Error::ConfigError(
            "snapshot metadata manifest path must be absolute".to_string(),
        ));
    }
    let (metadata_volume, relative_manifest_path) =
        metadata_volume_and_relative_path(config, manifest_path)?;
    let filesystem = FileSystemRegistry::new().get_or_register_volume(metadata_volume)?;
    let manifest_payload =
        MetadataReader::new(filesystem.open_read(&relative_manifest_path)?).read_all()?;
    let (
        snapshot_id,
        schema_id,
        ranges,
        timestamp_seconds,
        data_size_bytes,
        incremental_data_size_bytes,
    ) = match decode_manifest(manifest_payload.as_ref())? {
        ManifestPayload::Snapshot(manifest) => (
            manifest.id,
            manifest.latest_schema_id,
            manifest.bucket_ranges,
            manifest.timestamp_seconds,
            manifest.data_size_bytes,
            manifest.incremental_data_size_bytes,
        ),
        ManifestPayload::IncrementalSnapshot(manifest) => (
            manifest.id,
            manifest.latest_schema_id,
            manifest.bucket_ranges,
            manifest.timestamp_seconds,
            manifest.data_size_bytes,
            manifest.incremental_data_size_bytes,
        ),
    };
    let absolute_schema_path =
        schema_file_path_from_snapshot_manifest_path(manifest_path, schema_id)?;
    let schema_path = relative_path_for_volume(metadata_volume, &absolute_schema_path)?;
    let schema_payload = MetadataReader::new(filesystem.open_read(&schema_path)?).read_all()?;
    Ok(ShardSnapshotMetadata {
        ranges,
        db_id: db_id.to_string(),
        snapshot_id,
        manifest_path: manifest_path.to_string(),
        timestamp_seconds,
        data_size_bytes,
        incremental_data_size_bytes,
        schema_id,
        column_families: snapshot_column_families_from_payload(schema_payload.as_ref(), schema_id)?,
    })
}

/// Load one persisted global snapshot manifest without opening shards or a writable coordinator.
///
/// This is intentionally metadata-only so external checkpoint resolvers can validate and reuse an
/// already materialized global manifest without constructing a reader or mutating storage.
pub fn load_global_snapshot_metadata(
    config: &Config,
    manifest_path: &str,
) -> Result<GlobalSnapshotManifest> {
    if !is_absolute_storage_path(manifest_path) {
        return Err(Error::ConfigError(
            "global snapshot manifest path must be absolute".to_string(),
        ));
    }
    let (metadata_volume, relative_manifest_path) =
        metadata_volume_and_relative_path(config, manifest_path)?;
    let filesystem = FileSystemRegistry::new().get_or_register_volume(metadata_volume)?;
    let payload = MetadataReader::new(filesystem.open_read(&relative_manifest_path)?).read_all()?;
    let manifest: GlobalSnapshotManifest =
        serde_json::from_slice(payload.as_ref()).map_err(|err| {
            Error::IoError(format!("Failed to decode global snapshot manifest: {err}"))
        })?;
    manifest.validate_version()?;
    Ok(manifest)
}

fn metadata_volume_and_relative_path<'a>(
    config: &'a Config,
    path: &str,
) -> Result<(&'a crate::config::VolumeDescriptor, String)> {
    let metadata_volumes = config
        .volumes
        .iter()
        .filter(|volume| volume.supports(VolumeUsageKind::Meta))
        .collect::<Vec<_>>();
    if metadata_volumes.is_empty() {
        return Err(Error::ConfigError(
            "No metadata volume configured".to_string(),
        ));
    }
    let normalized_path = normalize_storage_path_to_url(path)?;
    for volume in metadata_volumes {
        if let Ok(relative) = relative_path_for_volume(volume, &normalized_path) {
            return Ok((volume, relative));
        }
    }
    Err(Error::ConfigError(format!(
        "snapshot metadata path is outside configured metadata volumes: {path}"
    )))
}

fn relative_path_for_volume(
    volume: &crate::config::VolumeDescriptor,
    path: &str,
) -> Result<String> {
    let normalized_path = normalize_storage_path_to_url(path)?;
    let normalized_base = normalize_storage_path_to_url(&volume.base_dir)?;
    let path_url = Url::parse(&normalized_path).map_err(|error| {
        Error::ConfigError(format!("Invalid snapshot metadata path {path}: {error}"))
    })?;
    let base_url = Url::parse(&normalized_base).map_err(|error| {
        Error::ConfigError(format!(
            "Invalid metadata volume path {}: {error}",
            volume.base_dir
        ))
    })?;
    if path_url.scheme() == "file" && base_url.scheme() == "file" {
        let path = path_url.to_file_path().map_err(|_| {
            Error::ConfigError(format!(
                "Invalid file snapshot metadata path {normalized_path}"
            ))
        })?;
        let base = base_url.to_file_path().map_err(|_| {
            Error::ConfigError(format!(
                "Invalid file metadata volume path {normalized_base}"
            ))
        })?;
        return path
            .strip_prefix(base)
            .map(|relative| {
                relative
                    .to_string_lossy()
                    .trim_start_matches('/')
                    .to_string()
            })
            .map_err(|_| {
                Error::ConfigError("snapshot metadata path is outside metadata volume".into())
            });
    }
    let base = normalized_base.trim_end_matches('/');
    let relative = normalized_path
        .strip_prefix(base)
        .filter(|relative| relative.is_empty() || relative.starts_with('/'))
        .ok_or_else(|| {
            Error::ConfigError("snapshot metadata path is outside metadata volume".into())
        })?;
    Ok(relative.trim_start_matches('/').to_string())
}

fn is_absolute_storage_path(path: &str) -> bool {
    path.contains("://") || Path::new(path).is_absolute()
}
