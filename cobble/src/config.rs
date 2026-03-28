use crate::SstCompressionAlgorithm;
use crate::data_file::DataFileType;
use crate::error::Result;
use crate::time::TimeProviderKind;
use crate::util::normalize_storage_path_to_url;
use log::warn;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use url::Url;

const DEFAULT_READ_PROXY_RELOAD_TOLERANCE_SECONDS: u64 = 10;

fn deserialize_volume_kinds<'de, D>(deserializer: D) -> Result<u8, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum KindsInput {
        Mask(u8),
        MaskString(String),
        List(Vec<String>),
    }

    let input = KindsInput::deserialize(deserializer)?;
    let mut mask = 0u8;
    let add_kind = |mask: &mut u8, kind: VolumeUsageKind| {
        *mask |= kind.mask();
    };
    let mut parse_values = |values: Vec<String>| -> Result<u8, D::Error> {
        for value in values {
            let normalized = value.trim().to_lowercase().replace('-', "_");
            match normalized.as_str() {
                "meta" => add_kind(&mut mask, VolumeUsageKind::Meta),
                "primary_data_priority_high" => {
                    add_kind(&mut mask, VolumeUsageKind::PrimaryDataPriorityHigh);
                }
                "primary_data_priority_medium" => {
                    add_kind(&mut mask, VolumeUsageKind::PrimaryDataPriorityMedium);
                }
                "primary_data_priority_low" => {
                    add_kind(&mut mask, VolumeUsageKind::PrimaryDataPriorityLow);
                }
                "snapshot" => add_kind(&mut mask, VolumeUsageKind::Snapshot),
                "cache" => add_kind(&mut mask, VolumeUsageKind::Cache),
                "readonly" => add_kind(&mut mask, VolumeUsageKind::Readonly),
                _ => {
                    return Err(serde::de::Error::custom(format!(
                        "Unknown volume usage kind: {}",
                        value
                    )));
                }
            }
        }
        Ok(mask)
    };
    match input {
        KindsInput::Mask(value) => Ok(value),
        KindsInput::MaskString(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(0);
            }
            if trimmed.contains(',') {
                let values = trimmed
                    .split(',')
                    .map(|entry| entry.trim().to_string())
                    .collect();
                parse_values(values)
            } else if let Ok(parsed) = trimmed.parse::<u8>() {
                Ok(parsed)
            } else {
                parse_values(vec![trimmed.to_string()])
            }
        }
        KindsInput::List(values) => parse_values(values),
    }
}

/// Compaction policy selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionPolicyKind {
    RoundRobin,
    MinOverlap,
}

/// Primary-volume offload policy selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryVolumeOffloadPolicyKind {
    LargestFile,
    #[default]
    Priority,
}

/// Memtable implementation selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemtableType {
    #[default]
    Hash,
    Skiplist,
    Vec,
}

/// Volume usage classification.
/// Used to indicate which volumes support which kinds of data.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeUsageKind {
    // Metadata storage (manifests, snapshots, etc).
    Meta = 0,
    // Primary data storage with the highest priority (SST files, write-ahead log).
    PrimaryDataPriorityHigh = 1,
    // Primary data storage with medium priority (SST files, write-ahead log).
    PrimaryDataPriorityMedium = 2,
    // Primary data storage with low priority (SST files, write-ahead log).
    PrimaryDataPriorityLow = 3,
    // Snapshot materialization storage (snapshot manifests, schema, and uploaded snapshot data).
    Snapshot = 4,
    // Block cache storage. e.g. foryer cache files.
    Cache = 5,
    // Read-only source volume used only for loading historical snapshot data.
    Readonly = 6,
}

impl VolumeUsageKind {
    fn mask(self) -> u8 {
        1 << (self as u8)
    }
}

/// Descriptor for a storage volume.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct VolumeDescriptor {
    /// Base directory URL for the volume.
    pub base_dir: String,
    /// Optional access id used to connect.
    pub access_id: Option<String>,
    /// Optional secret key used to connect.
    pub secret_key: Option<String>,
    /// Optional size limit for the volume in bytes.
    pub size_limit: Option<u64>,
    /// Usage kinds supported by the volume (bitmask of VolumeUsageKind).
    #[serde(deserialize_with = "deserialize_volume_kinds")]
    pub kinds: u8,
}

impl VolumeDescriptor {
    pub fn new(base_dir: String, kinds: Vec<VolumeUsageKind>) -> Self {
        let mut descriptor = Self {
            base_dir,
            access_id: None,
            secret_key: None,
            size_limit: None,
            kinds: 0,
        };
        for kind in kinds {
            descriptor.set_usage(kind);
        }
        descriptor
    }

    /// Helper to create a single volume config for both primary data and meta.
    pub fn single_volume(base_dir: String) -> Vec<Self> {
        vec![Self::new(
            base_dir,
            vec![
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Meta,
            ],
        )]
    }

    pub fn set_usage(&mut self, kind: VolumeUsageKind) {
        self.kinds |= kind.mask();
    }

    pub fn supports(&self, kind: VolumeUsageKind) -> bool {
        (self.kinds & kind.mask()) != 0
    }
}

fn supports_primary_data(volume: &VolumeDescriptor) -> bool {
    volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
        || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
        || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReaderConfigEntry {
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: usize,
    pub reload_tolerance_seconds: u64,
}

impl Default for ReaderConfigEntry {
    fn default() -> Self {
        Self {
            pin_partition_in_memory_count: 1,
            block_cache_size: 512 * 1024 * 1024,
            reload_tolerance_seconds: DEFAULT_READ_PROXY_RELOAD_TOLERANCE_SECONDS,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub column_indices: Option<Vec<usize>>,
    max_index: Option<usize>,
    cached_masks: Arc<Mutex<Option<ReadOptionsMasks>>>,
}

#[derive(Clone, Debug, Default)]
pub struct ScanOptions {
    pub read_ahead_bytes: usize,
    pub column_indices: Option<Vec<usize>>,
    max_index: Option<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub ttl_seconds: Option<u32>,
}

impl WriteOptions {
    pub fn with_ttl(ttl_seconds: u32) -> Self {
        Self {
            ttl_seconds: Some(ttl_seconds),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadOptionsMasks {
    pub(crate) num_columns: usize,
    pub(crate) selected_mask: Option<Arc<[u8]>>,
    pub(crate) base_mask: Arc<[u8]>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            column_indices: None,
            max_index: None,
            cached_masks: Arc::new(Mutex::new(None)),
        }
    }
}

impl ScanOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(Some(column_indices))
    }

    fn new_with_indices(column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            read_ahead_bytes: 0,
            column_indices,
            max_index,
        }
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }
}

impl ReadOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(Some(column_indices))
    }

    fn new_with_indices(column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            column_indices,
            max_index,
            cached_masks: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub(crate) fn masks(&self, num_columns: usize) -> ReadOptionsMasks {
        let mut guard = self.cached_masks.lock().unwrap();
        if guard
            .as_ref()
            .map(|mask| mask.num_columns != num_columns)
            .unwrap_or(true)
        {
            *guard = Some(self.build_masks(num_columns));
        }
        guard.as_ref().expect("cached mask initialized").clone()
    }

    fn build_masks(&self, num_columns: usize) -> ReadOptionsMasks {
        let mask_size = num_columns.div_ceil(8).max(1);
        let last_bits = (num_columns - 1) % 8 + 1;
        let last_mask = (1u8 << last_bits) - 1;
        let selected_mask = self.column_indices.as_ref().map(|columns| {
            let mut mask = vec![0u8; mask_size];
            for &column_idx in columns {
                if column_idx < num_columns {
                    mask[column_idx / 8] |= 1 << (column_idx % 8);
                }
            }
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        });
        let base_mask = if let Some(mask) = selected_mask.as_ref() {
            Arc::clone(mask)
        } else {
            let mut mask = vec![0xFF; mask_size];
            mask[mask_size - 1] &= last_mask;
            Arc::from(mask.into_boxed_slice())
        };
        ReadOptionsMasks {
            num_columns,
            selected_mask,
            base_mask,
        }
    }
}

/// Config for opening the database.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    /// Storage volume descriptors for this database.
    pub volumes: Vec<VolumeDescriptor>,
    /// Memtable capacity in bytes.
    pub memtable_capacity: usize,
    /// Number of memtable buffers to keep in memory.
    pub memtable_buffer_count: usize,
    /// Memtable implementation type.
    pub memtable_type: MemtableType,
    /// Number of columns in the value schema.
    pub num_columns: usize,
    /// Total number of buckets in the cluster. Should be 1~65536.
    pub total_buckets: u32,
    /// Maximum number of L0 files before triggering compaction.
    pub l0_file_limit: usize,
    /// Maximum number of immutables + L0 files before write stall.
    /// If None, uses min(l0_file_limit + 4, l0_file_limit * 2).
    pub write_stall_limit: Option<usize>,
    /// Base size for level 1.
    pub l1_base_bytes: usize,
    /// Size multiplier for deeper levels.
    pub level_size_multiplier: usize,
    /// Maximum level number (inclusive).
    pub max_level: u8,
    /// Compaction policy to use.
    pub compaction_policy: CompactionPolicyKind,
    /// Enable read-ahead buffering for compaction reads.
    pub compaction_read_ahead_enabled: bool,
    /// Optional remote compaction worker address (host:port). If set, use remote compaction.
    pub compaction_remote_addr: Option<String>,
    /// Remote compaction worker thread pool size.
    pub compaction_threads: usize,
    /// Remote compaction network timeout in milliseconds.
    pub compaction_remote_timeout_ms: u64,
    /// Size of the block cache in bytes. If zero, cache is disabled.
    pub block_cache_size: usize,
    /// Enable foyer hybrid block cache (memory + local disk).
    pub block_cache_hybrid_enabled: bool,
    /// Optional disk capacity for hybrid block cache in bytes.
    /// If unset, defaults to the in-memory block cache size.
    pub block_cache_hybrid_disk_size: Option<usize>,
    /// Read proxy configuration overrides.
    pub reader: ReaderConfigEntry,
    /// Target base SST file size in bytes.
    pub base_file_size: usize,
    /// Enable bloom filter in SST files.
    pub sst_bloom_filter_enabled: bool,
    /// Bits per key for SST bloom filter when enabled.
    pub sst_bloom_bits_per_key: u32,
    /// Whether to enable two-level index and filter blocks in SST files.
    pub sst_partitioned_index: bool,
    /// Output data-file format used by flush/compaction writers.
    pub data_file_type: DataFileType,
    /// Target parquet row-group size in bytes when parquet output format is selected.
    pub parquet_row_group_size_bytes: usize,
    /// Compression algorithm per level (index by level number).
    pub sst_compression_by_level: Vec<SstCompressionAlgorithm>,
    /// Whether TTL is enabled. If false, TTL metadata is ignored.
    pub ttl_enabled: bool,
    /// Default TTL duration (in seconds). None means no expiration by default.
    pub default_ttl_seconds: Option<u32>,
    /// Values larger than this threshold are marked for value-log separation.
    pub value_separation_threshold: usize,
    /// Time provider to use for TTL.
    pub time_provider: TimeProviderKind,
    /// Optional log file path. If None, logs go to console only. Must be a local path.
    pub log_path: Option<String>,
    /// Whether to enable console logging.
    pub log_console: bool,
    /// Log level filter (trace, debug, info, warn, error, off).
    pub log_level: log::LevelFilter,
    /// Automatically take a snapshot on every successful flush.
    pub snapshot_on_flush: bool,
    /// If active memtable usage ratio is below this value during snapshot, write an
    /// incremental active-memtable snapshot data file instead of flushing to SST.
    pub active_memtable_incremental_snapshot_ratio: f64,
    /// Optional level ordinal whose overflow triggers automatic LSM tree splitting.
    /// When set, the tree is split by bucket boundaries instead of compacting into deeper levels.
    pub lsm_split_trigger_level: Option<u8>,
    /// Usage ratio watermark for stopping new writes on a primary volume.
    /// Range: [0.0, 1.0].
    pub primary_volume_write_stop_watermark: f64,
    /// Usage ratio watermark for triggering background offload from a primary volume.
    /// Range: [0.0, 1.0], and should be <= write-stop watermark.
    pub primary_volume_offload_trigger_watermark: f64,
    /// Offload policy for selecting candidate files on pressured primary volumes.
    pub primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind,
    /// Auto-expire snapshots after this many newer snapshots are completed.
    /// None disables auto-expiration.
    pub snapshot_retention: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble".to_string()),
            memtable_capacity: 64 * 1024 * 1024,
            memtable_buffer_count: 2,
            memtable_type: MemtableType::Hash,
            num_columns: 1,
            total_buckets: 1,
            l0_file_limit: 4,
            write_stall_limit: None,
            l1_base_bytes: 64 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 6,
            compaction_policy: CompactionPolicyKind::RoundRobin,
            compaction_read_ahead_enabled: true,
            compaction_remote_addr: None,
            compaction_threads: 4,
            compaction_remote_timeout_ms: 300_000,
            block_cache_size: 64 * 1024 * 1024,
            block_cache_hybrid_enabled: false,
            block_cache_hybrid_disk_size: None,
            reader: ReaderConfigEntry::default(),
            base_file_size: 64 * 1024 * 1024,
            sst_bloom_filter_enabled: false,
            sst_bloom_bits_per_key: 10,
            sst_partitioned_index: false,
            data_file_type: DataFileType::SSTable,
            parquet_row_group_size_bytes: 256 * 1024,
            sst_compression_by_level: vec![
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::Lz4,
            ],
            ttl_enabled: false,
            default_ttl_seconds: None,
            value_separation_threshold: usize::MAX,
            time_provider: TimeProviderKind::default(),
            log_path: None,
            log_console: false,
            log_level: log::LevelFilter::Info,
            snapshot_on_flush: false,
            active_memtable_incremental_snapshot_ratio: 0.0,
            lsm_split_trigger_level: None,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.85,
            primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::Priority,
            snapshot_retention: None,
        }
    }
}

/// Plan for selecting a volume for hybrid block cache and reserving disk space if needed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HybridCacheVolumePlan {
    pub(crate) volume_idx: usize,
    pub(crate) base_dir: String,
    pub(crate) disk_capacity_bytes: usize,
    pub(crate) shared_with_primary: bool,
}

impl Config {
    pub(crate) fn normalize_volume_paths(&self) -> Result<Self> {
        let mut copied = self.clone();
        for volume in &mut copied.volumes {
            volume.base_dir = normalize_storage_path_to_url(&volume.base_dir)?;
        }
        Ok(copied)
    }
}

impl Config {
    pub(crate) fn hybrid_block_cache_disk_size(&self, memory_capacity: usize) -> Option<usize> {
        if !self.block_cache_hybrid_enabled || memory_capacity == 0 {
            return None;
        }
        let disk = self.block_cache_hybrid_disk_size.unwrap_or(memory_capacity);
        Some(disk)
    }

    /// Select a suitable volume for hybrid block cache based on the config and the required disk capacity.
    pub(crate) fn resolve_hybrid_cache_volume_plan(
        &self,
        memory_capacity: usize,
    ) -> Result<Option<HybridCacheVolumePlan>> {
        let Some(disk_capacity_bytes) = self.hybrid_block_cache_disk_size(memory_capacity) else {
            return Ok(None);
        };
        if disk_capacity_bytes == 0 {
            return Err(crate::error::Error::ConfigError(
                "block_cache_hybrid_disk_size must be greater than 0 when hybrid cache is enabled"
                    .to_string(),
            ));
        }
        let required = disk_capacity_bytes as u64;
        let mut cache_only_candidates: Vec<HybridCacheVolumePlan> = Vec::new();
        let mut shared_candidates: Vec<HybridCacheVolumePlan> = Vec::new();
        let mut has_cache_volume = false;
        let mut has_local_cache_volume = false;

        for (idx, volume) in self.volumes.iter().enumerate() {
            if !volume.supports(VolumeUsageKind::Cache) {
                continue;
            }
            has_cache_volume = true;
            let normalized_base_dir = normalize_storage_path_to_url(&volume.base_dir)?;
            let url = Url::parse(&normalized_base_dir).map_err(|err| {
                crate::error::Error::ConfigError(format!(
                    "Invalid cache volume URL {}: {}",
                    normalized_base_dir, err
                ))
            })?;
            if !url.scheme().eq_ignore_ascii_case("file") {
                continue;
            }
            has_local_cache_volume = true;
            let fits = match volume.size_limit {
                Some(limit) => limit >= required,
                None => true,
            };
            if !fits {
                continue;
            }
            let shared_with_primary = supports_primary_data(volume);
            let plan = HybridCacheVolumePlan {
                volume_idx: idx,
                base_dir: normalized_base_dir,
                disk_capacity_bytes,
                shared_with_primary,
            };
            if !shared_with_primary {
                cache_only_candidates.push(plan);
            } else {
                shared_candidates.push(plan);
            }
        }

        let mut rng = rand::thread_rng();
        if let Some(plan) = cache_only_candidates.choose(&mut rng) {
            return Ok(Some(plan.clone()));
        }
        if let Some(plan) = shared_candidates.choose(&mut rng) {
            return Ok(Some(plan.clone()));
        }
        if !has_cache_volume {
            return Err(crate::error::Error::ConfigError(
                "Hybrid block cache enabled but no volume is configured with cache usage"
                    .to_string(),
            ));
        }
        if !has_local_cache_volume {
            return Err(crate::error::Error::ConfigError(
                "Hybrid block cache requires a local file:// cache volume".to_string(),
            ));
        }
        Err(crate::error::Error::ConfigError(format!(
            "No cache volume has enough capacity for hybrid block cache disk size {} bytes",
            disk_capacity_bytes
        )))
    }

    /// If the selected hybrid cache volume is shared with primary data, adjust the config to
    /// reserve the required disk space for the cache.
    pub(crate) fn apply_hybrid_cache_primary_partition_with_plan(
        &self,
        plan: Option<&HybridCacheVolumePlan>,
    ) -> Result<Self> {
        let Some(plan) = plan else {
            return Ok(self.clone());
        };
        if !plan.shared_with_primary {
            return Ok(self.clone());
        }
        let mut adjusted = self.clone();
        let disk_bytes = plan.disk_capacity_bytes as u64;
        let volume = adjusted.volumes.get_mut(plan.volume_idx).ok_or_else(|| {
            crate::error::Error::ConfigError(format!(
                "Selected hybrid cache volume index {} out of range",
                plan.volume_idx
            ))
        })?;
        if let Some(limit) = volume.size_limit {
            if limit <= disk_bytes {
                return Err(crate::error::Error::ConfigError(format!(
                    "Hybrid cache reservation {} bytes exceeds shared volume limit {} bytes for {}",
                    disk_bytes, limit, volume.base_dir
                )));
            }
            volume.size_limit = Some(limit - disk_bytes);
        }
        Ok(adjusted)
    }

    pub fn from_path(path: impl AsRef<std::path::Path>) -> crate::error::Result<Self> {
        let mut builder = ::config::Config::builder();
        let path = path.as_ref();
        let format = match path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase())
        {
            Some(ext) if ext == "yaml" || ext == "yml" => ::config::FileFormat::Yaml,
            Some(ext) if ext == "ini" => ::config::FileFormat::Ini,
            Some(ext) if ext == "json" => ::config::FileFormat::Json,
            Some(ext) if ext == "toml" => ::config::FileFormat::Toml,
            Some(ext) => {
                return Err(crate::error::Error::ConfigError(format!(
                    "Unsupported config format: {}",
                    ext
                )));
            }
            None => {
                return Err(crate::error::Error::ConfigError(
                    "Config path missing extension".to_string(),
                ));
            }
        };
        builder = builder.add_source(::config::File::from(path).format(format));
        builder
            .build()
            .map_err(|err| crate::error::Error::ConfigError(err.to_string()))?
            .try_deserialize()
            .map_err(|err| crate::error::Error::ConfigError(err.to_string()))
    }

    pub(crate) fn resolved_write_stall_limit(&self) -> usize {
        let default_limit = self
            .l0_file_limit
            .saturating_add(4)
            .min(self.l0_file_limit.saturating_mul(2));
        match self.write_stall_limit {
            Some(limit) => {
                if limit > self.l0_file_limit.saturating_add(1) {
                    limit
                } else {
                    warn!(
                        "write stall limit {} invalid for l0 limit {}; using default as {}",
                        limit, self.l0_file_limit, default_limit
                    );
                    default_limit
                }
            }
            _ => default_limit,
        }
    }

    pub(crate) fn sst_compression_for_level(&self, level: u8) -> SstCompressionAlgorithm {
        if self.sst_compression_by_level.is_empty() {
            return if level >= 2 {
                SstCompressionAlgorithm::Lz4
            } else {
                SstCompressionAlgorithm::None
            };
        }
        let idx = level as usize;
        if idx < self.sst_compression_by_level.len() {
            self.sst_compression_by_level[idx]
        } else {
            *self
                .sst_compression_by_level
                .last()
                .expect("compression config not empty")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Config, MemtableType, PrimaryVolumeOffloadPolicyKind, ReaderConfigEntry, VolumeDescriptor,
        VolumeUsageKind,
    };
    use crate::SstCompressionAlgorithm;
    use crate::data_file::DataFileType;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::Builder;

    #[test]
    fn test_config_from_file_round_trip() {
        let config = Config {
            volumes: vec![VolumeDescriptor::new(
                "file:///tmp/cobble".to_string(),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Meta,
                ],
            )],
            memtable_capacity: 1024,
            memtable_buffer_count: 3,
            memtable_type: MemtableType::Vec,
            num_columns: 2,
            total_buckets: 1024,
            l0_file_limit: 5,
            write_stall_limit: Some(12),
            l1_base_bytes: 8 * 1024,
            level_size_multiplier: 7,
            max_level: 4,
            compaction_policy: super::CompactionPolicyKind::MinOverlap,
            block_cache_size: 256,
            block_cache_hybrid_enabled: true,
            block_cache_hybrid_disk_size: Some(1024),
            reader: ReaderConfigEntry {
                pin_partition_in_memory_count: 2,
                block_cache_size: 2048,
                reload_tolerance_seconds: 5,
            },
            base_file_size: 512,
            sst_bloom_filter_enabled: true,
            sst_bloom_bits_per_key: 11,
            sst_partitioned_index: true,
            data_file_type: DataFileType::Parquet,
            parquet_row_group_size_bytes: 4096,
            sst_compression_by_level: vec![
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::Lz4,
            ],
            ttl_enabled: true,
            default_ttl_seconds: Some(120),
            value_separation_threshold: 4096,
            time_provider: crate::time::TimeProviderKind::Manual,
            log_path: Some("/tmp/cobble.log".to_string()),
            log_console: true,
            log_level: log::LevelFilter::Debug,
            snapshot_on_flush: true,
            active_memtable_incremental_snapshot_ratio: 0.5,
            lsm_split_trigger_level: Some(2),
            primary_volume_write_stop_watermark: 0.93,
            primary_volume_offload_trigger_watermark: 0.82,
            primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::LargestFile,
            snapshot_retention: Some(3),
            compaction_read_ahead_enabled: false,
            compaction_remote_addr: Some("127.0.0.1:9999".to_string()),
            compaction_threads: 6,
            compaction_remote_timeout_ms: 120_000,
        };

        let serialized = serde_json::to_string(&config).expect("Cannot serialize config");
        let mut json_file = Builder::new()
            .suffix(".json")
            .tempfile()
            .expect("Should create temp json");
        json_file
            .write_all(serialized.as_bytes())
            .expect("Should able to write json");
        json_file.flush().expect("Should able to flush json");
        let decoded: Config = Config::from_path(json_file.path()).expect("Cannot deserialize json");

        assert_eq!(decoded.volumes.len(), 1);
        assert!(decoded.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
        assert!(decoded.volumes[0].supports(VolumeUsageKind::Meta));
        assert_eq!(decoded.memtable_capacity, 1024);
        assert_eq!(decoded.memtable_type, MemtableType::Vec);
        assert_eq!(decoded.total_buckets, 1024);
        assert_eq!(decoded.write_stall_limit, Some(12));
        assert_eq!(
            decoded.compaction_policy,
            super::CompactionPolicyKind::MinOverlap
        );
        assert!(decoded.sst_partitioned_index);
        assert_eq!(decoded.time_provider, crate::time::TimeProviderKind::Manual);
        assert_eq!(decoded.log_level, log::LevelFilter::Debug);
        assert_eq!(decoded.snapshot_retention, Some(3));
        assert_eq!(decoded.active_memtable_incremental_snapshot_ratio, 0.5);
        assert_eq!(decoded.lsm_split_trigger_level, Some(2));
        assert_eq!(decoded.primary_volume_write_stop_watermark, 0.93);
        assert_eq!(decoded.primary_volume_offload_trigger_watermark, 0.82);
        assert_eq!(
            decoded.primary_volume_offload_policy,
            PrimaryVolumeOffloadPolicyKind::LargestFile
        );
        assert_eq!(decoded.value_separation_threshold, 4096);
        assert_eq!(decoded.data_file_type, DataFileType::Parquet);
        assert_eq!(decoded.parquet_row_group_size_bytes, 4096);
        assert_eq!(decoded.reader.block_cache_size, 2048);
        assert_eq!(decoded.reader.reload_tolerance_seconds, 5);
        assert!(decoded.block_cache_hybrid_enabled);
        assert_eq!(decoded.block_cache_hybrid_disk_size, Some(1024));

        let yaml = serde_yaml::to_string(&config).expect("Cannot serialize yaml");
        let mut yaml_file = Builder::new()
            .suffix(".yaml")
            .tempfile()
            .expect("Should create temp yaml");
        yaml_file
            .write_all(yaml.as_bytes())
            .expect("Should able to write yaml");
        yaml_file.flush().expect("Should able to flush yaml");
        let decoded_yaml: Config =
            Config::from_path(yaml_file.path()).expect("Cannot deserialize yaml");
        assert_eq!(decoded_yaml.reader.block_cache_size, 2048);

        let mut path_buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path_buf.push("tests/testdata/config.ini");

        let decoded_ini =
            Config::from_path(path_buf.as_path()).expect("Cannot deserialize ini config");
        assert_eq!(decoded_ini.reader.reload_tolerance_seconds, 5);
        assert!(decoded_ini.volumes[0].supports(VolumeUsageKind::Meta));
    }

    #[test]
    fn test_volume_descriptor_kinds_list() {
        let json = r#"{
            "base_dir": "file:///tmp/cobble",
            "kinds": ["meta", "primary_data_priority_high", "snapshot", "cache"]
        }"#;

        let volume: VolumeDescriptor =
            serde_json::from_str(json).expect("Cannot deserialize volume descriptor");
        assert!(volume.supports(VolumeUsageKind::Meta));
        assert!(volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh));
        assert!(volume.supports(VolumeUsageKind::Snapshot));
        assert!(volume.supports(VolumeUsageKind::Cache));
    }

    #[test]
    fn test_volume_descriptor_kinds_readonly() {
        let json = r#"{
            "base_dir": "file:///tmp/cobble-readonly",
            "kinds": ["readonly"]
        }"#;
        let volume: VolumeDescriptor =
            serde_json::from_str(json).expect("Cannot deserialize readonly volume descriptor");
        assert!(volume.supports(VolumeUsageKind::Readonly));
        assert!(!volume.supports(VolumeUsageKind::Snapshot));
        assert!(!volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh));
    }

    #[test]
    fn test_normalize_volume_paths_converts_local_absolute_path() {
        let mut config = Config::default();
        let local = std::env::temp_dir().join("cobble-config-normalize");
        config.volumes = VolumeDescriptor::single_volume(local.to_string_lossy().to_string());
        let config = config.normalize_volume_paths().unwrap();
        assert!(config.volumes[0].base_dir.starts_with("file://"));
    }

    #[test]
    fn test_hybrid_cache_prefers_cache_only_volume() {
        let mut config = Config::default();
        config.block_cache_hybrid_enabled = true;
        config.block_cache_hybrid_disk_size = Some(1024);
        config.volumes = vec![
            VolumeDescriptor::new(
                "file:///tmp/primary-shared".to_string(),
                vec![
                    VolumeUsageKind::PrimaryDataPriorityHigh,
                    VolumeUsageKind::Cache,
                    VolumeUsageKind::Meta,
                ],
            ),
            VolumeDescriptor::new(
                "file:///tmp/cache-only".to_string(),
                vec![VolumeUsageKind::Cache],
            ),
        ];
        let plan = config
            .resolve_hybrid_cache_volume_plan(2048)
            .unwrap()
            .unwrap();
        assert_eq!(plan.volume_idx, 1);
        assert!(!plan.shared_with_primary);
        assert_eq!(plan.disk_capacity_bytes, 1024);
    }

    #[test]
    fn test_hybrid_cache_partitions_shared_volume_limit() {
        let mut config = Config::default();
        config.block_cache_hybrid_enabled = true;
        config.block_cache_hybrid_disk_size = Some(1024);
        let mut shared = VolumeDescriptor::new(
            "file:///tmp/shared".to_string(),
            vec![
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Cache,
                VolumeUsageKind::Meta,
            ],
        );
        shared.size_limit = Some(8192);
        config.volumes = vec![shared];
        let plan = config.resolve_hybrid_cache_volume_plan(4096).unwrap();
        let adjusted = config
            .apply_hybrid_cache_primary_partition_with_plan(plan.as_ref())
            .unwrap();
        assert_eq!(adjusted.volumes[0].size_limit, Some(7168));
    }

    #[test]
    fn test_hybrid_cache_rejects_non_local_cache_volume() {
        let mut config = Config::default();
        config.block_cache_hybrid_enabled = true;
        config.block_cache_hybrid_disk_size = Some(1024);
        config.volumes = vec![VolumeDescriptor::new(
            "s3://bucket/cache".to_string(),
            vec![VolumeUsageKind::Cache],
        )];
        let err = config.resolve_hybrid_cache_volume_plan(2048).unwrap_err();
        assert!(matches!(err, crate::error::Error::ConfigError(_)));
    }

    #[test]
    fn test_data_file_type_defaults_to_sst_when_missing() {
        let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "num_columns": 1
        }"#;
        let decoded: Config =
            serde_json::from_str(json).expect("Cannot deserialize partial config");
        assert_eq!(decoded.data_file_type, DataFileType::SSTable);
        assert_eq!(decoded.parquet_row_group_size_bytes, 256 * 1024);
    }

    #[test]
    fn test_data_file_type_parquet_round_trip() {
        let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "data_file_type":"parquet",
            "parquet_row_group_size_bytes": 8192
        }"#;
        let decoded: Config =
            serde_json::from_str(json).expect("Cannot deserialize parquet config");
        assert_eq!(decoded.data_file_type, DataFileType::Parquet);
        assert_eq!(decoded.parquet_row_group_size_bytes, 8192);
    }
}
