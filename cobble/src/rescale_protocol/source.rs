//! Resolve source metadata using configured routes, including credentials.
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::metrics_manager::MetricsManager;
use crate::storage_url::{match_storage_path, normalize_storage_path_to_url};
use std::sync::Arc;

pub(crate) fn snapshot_source_file_manager(
    config: &crate::Config,
    source_db_id: &str,
    manifest_path: &str,
) -> Result<(Arc<FileManager>, u64)> {
    let location = if manifest_path.trim().contains("://") {
        manifest_path.trim().to_string()
    } else {
        normalize_storage_path_to_url(manifest_path)?
    };
    let mut root = url::Url::parse(&location)?;
    if !root.username().is_empty() || root.password().is_some() || root.fragment().is_some() {
        return Err(Error::ConfigError(
            "Source manifest location must not contain credentials or fragments".into(),
        ));
    }
    let segments = root
        .path_segments()
        .ok_or_else(|| Error::ConfigError("Source manifest location has no path".into()))?
        .collect::<Vec<_>>();
    let snapshot_id = segments
        .last()
        .and_then(|name| name.strip_prefix("SNAPSHOT-"))
        .and_then(|id| id.parse::<u64>().ok())
        .ok_or_else(|| {
            Error::ConfigError("Source manifest location must name a snapshot".into())
        })?;
    if segments.last().copied() != Some(format!("SNAPSHOT-{snapshot_id}").as_str()) {
        return Err(Error::ConfigError(
            "Source manifest location must use a canonical snapshot name".into(),
        ));
    }
    if segments.len() < 3
        || segments[segments.len() - 2] != "snapshot"
        || segments[segments.len() - 3] != source_db_id
    {
        return Err(Error::ConfigError(
            "Source manifest location does not match the source DB".into(),
        ));
    }
    root.path_segments_mut()
        .map_err(|_| Error::ConfigError("Invalid source manifest location".into()))?
        .pop()
        .pop()
        .pop();
    let source_path = root.path().trim_end_matches('/').to_string();
    root.set_path(&source_path);
    let source_root = root.as_str();
    let bases = config
        .volumes
        .iter()
        .map(|volume| normalize_storage_path_to_url(&volume.base_dir))
        .collect::<Result<Vec<_>>>()?;
    let mut source_volume =
        match_storage_path(source_root, bases.iter().map(String::as_str).enumerate())?
            .filter(|(_, relative)| relative.is_empty())
            .map(|(index, _)| config.volumes[index].clone())
            .ok_or_else(|| {
                Error::ConfigError(format!(
                    "Source metadata volume is not configured: {source_root}"
                ))
            })?;
    source_volume.kinds = 0;
    source_volume.set_usage(crate::VolumeUsageKind::Meta);
    source_volume.set_usage(crate::VolumeUsageKind::Snapshot);
    // The first metadata route selects the source's snapshot/lease root. Preserve the other
    // configured routes for absolute file reads and FileManager's primary-storage invariant.
    let mut source_config = config.clone();
    source_config.volumes.insert(0, source_volume);
    let manager = FileManager::from_config_readonly(
        &source_config,
        source_db_id,
        Arc::new(MetricsManager::new(format!("{source_db_id}-expand-source"))),
    )?;
    Ok((Arc::new(manager), snapshot_id))
}
