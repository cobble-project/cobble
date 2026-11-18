//! Storage-volume capacity accounting and file placement.

use super::file_manager::FileManager;
use crate::Config;
use crate::config::VolumeUsageKind;
use crate::error::{Error, Result};
use crate::file::deletion::FileDeletionQueue;
use crate::file::file_system::{FileSystem, FileSystemRegistry};
use crate::storage_url::normalize_storage_path_to_url;
use metrics::{Gauge, gauge};
use rand::random;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum VolumePriority {
    High,
    Medium,
    Low,
}

impl VolumePriority {
    pub(crate) fn rank(self) -> u8 {
        match self {
            VolumePriority::High => 3,
            VolumePriority::Medium => 2,
            VolumePriority::Low => 1,
        }
    }
}

pub(crate) struct DataVolume {
    pub(crate) fs: Arc<dyn FileSystem>,
    /// Shared across this DB's volumes, independently of filesystem instance sharing.
    pub(crate) deletions: Arc<FileDeletionQueue>,
    pub(crate) base_dir: Option<String>,
    pub(crate) size_limit: Option<u64>,
    pub(crate) used_bytes: Arc<AtomicU64>,
    pub(super) storage_file_bytes: Option<Gauge>,
    pub(crate) projected_offload_bytes: AtomicU64,
    pub(crate) priority: VolumePriority,
    pub(crate) supports_primary_data: bool,
    pub(crate) supports_meta: bool,
    pub(crate) snapshot_persistable: bool,
    pub(crate) readonly_source: bool,
}

impl Clone for DataVolume {
    fn clone(&self) -> Self {
        Self {
            fs: Arc::clone(&self.fs),
            deletions: Arc::clone(&self.deletions),
            base_dir: self.base_dir.clone(),
            size_limit: self.size_limit,
            used_bytes: Arc::clone(&self.used_bytes),
            storage_file_bytes: self.storage_file_bytes.clone(),
            projected_offload_bytes: AtomicU64::new(
                self.projected_offload_bytes.load(Ordering::SeqCst),
            ),
            priority: self.priority,
            supports_primary_data: self.supports_primary_data,
            supports_meta: self.supports_meta,
            snapshot_persistable: self.snapshot_persistable,
            readonly_source: self.readonly_source,
        }
    }
}

impl DataVolume {
    pub(crate) fn base_dir(&self) -> Option<&str> {
        self.base_dir.as_deref()
    }

    pub(crate) fn add_usage(&self, bytes: u64) {
        let mut current = self.used_bytes.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_add(bytes);
            match self.used_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if let Some(gauge) = &self.storage_file_bytes {
                        gauge.set(next as f64);
                    }
                    break;
                }
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn add_projected_offload_bytes(&self, bytes: u64) {
        self.projected_offload_bytes
            .fetch_add(bytes, Ordering::SeqCst);
    }

    pub(crate) fn subtract_projected_offload_bytes(&self, bytes: u64) {
        let mut current = self.projected_offload_bytes.load(Ordering::SeqCst);
        loop {
            let next = current.saturating_sub(bytes);
            match self.projected_offload_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn projected_offload_bytes(&self) -> u64 {
        self.projected_offload_bytes.load(Ordering::SeqCst)
    }

    pub(crate) fn subtract_usage(&self, bytes: u64) {
        let mut current = self.used_bytes.load(Ordering::SeqCst);
        loop {
            // Use saturating_sub to avoid underflow, but ensure it doesn't go negative.
            let next = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if let Some(gauge) = &self.storage_file_bytes {
                        gauge.set(next as f64);
                    }
                    break;
                }
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn is_full(&self, base_file_size: u64) -> bool {
        let Some(limit) = self.size_limit else {
            return false;
        };
        let threshold = limit.saturating_sub(base_file_size);
        let used = self.used_bytes.load(Ordering::SeqCst);
        used >= threshold
    }

    pub(crate) fn is_write_stopped(&self, write_stop_watermark: f64) -> bool {
        self.usage_ratio()
            .map(|ratio| ratio >= write_stop_watermark)
            .unwrap_or(false)
    }

    pub(crate) fn is_write_stopped_with_expected(
        &self,
        write_stop_watermark: f64,
        expected_write_bytes: u64,
    ) -> bool {
        let Some(limit) = self.size_limit else {
            return false;
        };
        if limit == 0 {
            return true;
        }
        let used = self.used_bytes.load(Ordering::SeqCst);
        let projected = used.saturating_add(expected_write_bytes);
        (projected as f64 / limit as f64) >= write_stop_watermark
    }

    pub(crate) fn usage_ratio(&self) -> Option<f64> {
        let limit = self.size_limit?;
        if limit == 0 {
            return Some(1.0);
        }
        let used = self.used_bytes.load(Ordering::SeqCst);
        Some((used as f64 / limit as f64).min(1.0))
    }

    pub(crate) fn fs(&self) -> &Arc<dyn FileSystem> {
        &self.fs
    }
}

/// Placement policy for a file that is becoming locally primary.
///
/// Ordinary SSTs retain the normal highest-priority-first behavior. VLog files newly created or
/// copied into primary may opt into the lowest primary tier through [`FileManagerOptions`](super::file_manager::FileManagerOptions).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrimaryDataPlacement {
    Standard,
    Vlog,
}

impl FileManager {
    fn is_volume_write_stopped(&self, volume: &Arc<DataVolume>, expected_write_bytes: u64) -> bool {
        volume.is_write_stopped_with_expected(
            self.options.primary_volume_write_stop_watermark,
            expected_write_bytes,
        )
    }

    pub(super) fn sort_data_volumes(mut volumes: Vec<Arc<DataVolume>>) -> Vec<Arc<DataVolume>> {
        volumes.sort_by_key(|volume| std::cmp::Reverse(volume.priority.rank()));
        volumes
    }

    pub(super) fn select_data_volume(
        &self,
        expected_write_bytes: Option<u64>,
    ) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let expected_write_bytes = expected_write_bytes.unwrap_or(base_file_size);
        let mut selected: Option<&Arc<DataVolume>> = None;
        let mut candidates: Vec<&Arc<DataVolume>> = Vec::with_capacity(self.data_volumes.len());
        for volume in &self.data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            if !candidates.is_empty() && volume.priority.rank() < candidates[0].priority.rank() {
                break;
            }
            if self.is_volume_write_stopped(volume, expected_write_bytes) {
                continue;
            }
            if volume.is_full(base_file_size.max(expected_write_bytes)) {
                continue;
            }
            candidates.push(volume);
        }
        if candidates.len() == 1 {
            selected = Some(candidates[0]);
        } else if !candidates.is_empty() {
            // Randomly select among candidates of the same priority
            let idx = random::<usize>() % candidates.len();
            selected = Some(candidates[idx]);
        }
        selected.ok_or_else(|| Error::IoError("All primary data volumes are full".to_string()))
    }

    /// Selects only the configured lowest-priority primary tier for a new value-log file.
    ///
    /// The priority tier is determined before capacity filtering. This deliberately does not
    /// fall back to a higher-priority volume when every volume in the lowest tier is full or
    /// write-stopped: callers that opt into direct VLOG placement expect the file to stay there.
    fn select_lowest_priority_data_volume(
        &self,
        expected_write_bytes: Option<u64>,
    ) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let expected_write_bytes = expected_write_bytes.unwrap_or(base_file_size);
        let Some(lowest_rank) = self
            .data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source)
            .map(|volume| volume.priority.rank())
            .min()
        else {
            return Err(Error::IoError(
                "No primary data volume is configured for value-log files".to_string(),
            ));
        };

        let candidates: Vec<&Arc<DataVolume>> = self
            .data_volumes
            .iter()
            .filter(|volume| {
                volume.supports_primary_data
                    && !volume.readonly_source
                    && volume.priority.rank() == lowest_rank
                    && !self.is_volume_write_stopped(volume, expected_write_bytes)
                    && !volume.is_full(base_file_size.max(expected_write_bytes))
            })
            .collect();
        match candidates.len() {
            0 => Err(Error::IoError(
                "All lowest-priority primary data volumes are full or write-stopped for value-log files"
                    .to_string(),
            )),
            1 => Ok(candidates[0]),
            _ => Ok(candidates[random::<usize>() % candidates.len()]),
        }
    }

    pub(crate) fn uses_lowest_primary_tier(&self, placement: PrimaryDataPlacement) -> bool {
        placement == PrimaryDataPlacement::Vlog && self.options.vlog_low_priority_primary_enabled
    }

    pub(crate) fn lowest_writable_primary_rank(&self) -> Option<u8> {
        self.data_volumes
            .iter()
            .filter(|volume| volume.supports_primary_data && !volume.readonly_source)
            .map(|volume| volume.priority.rank())
            .min()
    }

    pub(super) fn select_primary_data_volume_for_placement(
        &self,
        placement: PrimaryDataPlacement,
        expected_write_bytes: Option<u64>,
    ) -> Result<&Arc<DataVolume>> {
        if self.uses_lowest_primary_tier(placement) {
            self.select_lowest_priority_data_volume(expected_write_bytes)
        } else {
            self.select_data_volume(expected_write_bytes)
        }
    }

    pub(super) fn choose_meta_volume(volumes: &[Arc<DataVolume>]) -> Result<Arc<DataVolume>> {
        if let Some(meta_volume) = volumes.iter().find(|volume| volume.supports_meta) {
            return Ok(Arc::clone(meta_volume));
        }

        if let Some(snapshot_volume) = volumes.iter().find(|volume| volume.snapshot_persistable) {
            return Ok(Arc::clone(snapshot_volume));
        }

        Err(Error::ConfigError(
            "No volume configured for snapshot persistence".to_string(),
        ))
    }

    pub(crate) fn data_volumes_from_config(config: &Config) -> Result<Vec<DataVolume>> {
        Self::data_volumes_from_config_with_metrics(config, None)
    }

    pub(super) fn data_volumes_from_config_with_metrics(
        config: &Config,
        db_id: Option<&str>,
    ) -> Result<Vec<DataVolume>> {
        let registry = FileSystemRegistry::new();
        let volumes = if config.volumes.is_empty() {
            return Err(Error::ConfigError("No volumes configured".to_string()));
        } else {
            config.volumes.clone()
        };
        let has_explicit_snapshot_volume = volumes
            .iter()
            .any(|volume| volume.supports(VolumeUsageKind::Snapshot));
        let mut data_volumes = Vec::new();
        for (volume_index, volume) in volumes.iter().enumerate() {
            let readonly_source = volume.supports(VolumeUsageKind::Readonly);
            if readonly_source {
                let has_other_kinds = volume.supports(VolumeUsageKind::Meta)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
                    || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
                    || volume.supports(VolumeUsageKind::Snapshot)
                    || volume.supports(VolumeUsageKind::Cache)
                    || volume.supports(VolumeUsageKind::Wal);
                if has_other_kinds {
                    return Err(Error::ConfigError(format!(
                        "Volume {} uses readonly and other kinds; readonly must be exclusive",
                        volume.base_dir
                    )));
                }
            }
            let mut priority = None;
            if volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh) {
                priority = Some(VolumePriority::High);
            }
            if priority.is_none() && volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium) {
                priority = Some(VolumePriority::Medium);
            }
            if priority.is_none() && volume.supports(VolumeUsageKind::PrimaryDataPriorityLow) {
                priority = Some(VolumePriority::Low);
            }
            let supports_primary_data = priority.is_some();
            let supports_meta = volume.supports(VolumeUsageKind::Meta);
            let supports_snapshot = if has_explicit_snapshot_volume {
                volume.supports(VolumeUsageKind::Snapshot)
            } else {
                supports_primary_data || volume.supports(VolumeUsageKind::Snapshot)
            };
            if supports_primary_data || supports_snapshot || supports_meta || readonly_source {
                let fs = registry.get_or_register_volume(volume)?;
                let normalized_base_dir = normalize_storage_path_to_url(&volume.base_dir)?;
                data_volumes.push(DataVolume {
                    fs,
                    deletions: Default::default(),
                    base_dir: Some(normalized_base_dir),
                    size_limit: volume.size_limit_bytes()?,
                    used_bytes: Arc::new(AtomicU64::new(0)),
                    storage_file_bytes: db_id.map(|db_id| {
                        gauge!(
                            "storage_file_bytes",
                            "db_id" => db_id.to_string(),
                            "volume" => volume_index.to_string()
                        )
                    }),
                    projected_offload_bytes: AtomicU64::new(0),
                    priority: priority.unwrap_or(VolumePriority::Low),
                    supports_primary_data,
                    supports_meta,
                    snapshot_persistable: supports_snapshot,
                    readonly_source,
                });
            }
        }
        if data_volumes.is_empty() {
            return Err(Error::ConfigError(
                "No volume configured for primary data storage".to_string(),
            ));
        }
        if !data_volumes
            .iter()
            .any(|volume| volume.snapshot_persistable)
        {
            return Err(Error::ConfigError(
                "No volume configured for snapshot persistence".to_string(),
            ));
        }
        Ok(data_volumes)
    }

    pub(super) fn select_snapshot_persistable_volume(&self) -> Result<&Arc<DataVolume>> {
        let base_file_size = self.options.base_file_size as u64;
        let snapshot_only_non_full = self
            .data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable && !volume.supports_primary_data)
            .find(|volume| !volume.is_full(base_file_size));
        if let Some(volume) = snapshot_only_non_full {
            return Ok(volume);
        }
        let snapshot_only_any = self
            .data_volumes
            .iter()
            .find(|volume| volume.snapshot_persistable && !volume.supports_primary_data);
        if let Some(volume) = snapshot_only_any {
            return Ok(volume);
        }
        let shared_non_full = self
            .data_volumes
            .iter()
            .filter(|volume| volume.snapshot_persistable && volume.supports_primary_data)
            .find(|volume| !volume.is_full(base_file_size));
        if let Some(volume) = shared_non_full {
            return Ok(volume);
        }
        self.data_volumes
            .iter()
            .find(|volume| volume.snapshot_persistable && volume.supports_primary_data)
            .ok_or_else(|| {
                Error::ConfigError("Snapshot persistence volume is not configured".to_string())
            })
    }
}

#[cfg(test)]
#[path = "../../tests/unit/file/volume.rs"]
mod tests;
