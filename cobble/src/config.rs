use crate::SstCompressionAlgorithm;
use crate::data_file::DataFileType;
use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::time::TimeProviderKind;
use crate::util::{normalize_storage_path_to_url, size_to_u64, size_to_usize};
use arc_swap::ArcSwapOption;
use config::{Config as ConfigLoader, File as ConfigFile, FileFormat as ConfigFileFormat};
use log::warn;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use size::Size;
use std::collections::HashMap;
use std::sync::Arc;
use toml::Value as TomlValue;
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
pub struct VolumeDescriptor {
    /// Base directory URL for the volume.
    pub base_dir: String,
    /// Optional access id used to connect.
    pub access_id: Option<String>,
    /// Optional secret key used to connect.
    pub secret_key: Option<String>,
    /// Optional size limit for the volume in bytes.
    pub size_limit: Option<Size>,
    /// Optional custom key-value options for backend-specific initialization.
    pub custom_options: Option<HashMap<String, String>>,
    /// Usage kinds supported by the volume (bitmask of VolumeUsageKind).
    #[serde(deserialize_with = "deserialize_volume_kinds")]
    pub kinds: u8,
}

impl VolumeDescriptor {
    pub fn new(base_dir: impl Into<String>, kinds: Vec<VolumeUsageKind>) -> Self {
        let mut descriptor = Self {
            base_dir: base_dir.into(),
            access_id: None,
            secret_key: None,
            size_limit: None,
            custom_options: None,
            kinds: 0,
        };
        for kind in kinds {
            descriptor.set_usage(kind);
        }
        descriptor
    }

    /// Helper to create a single volume config for both primary data and meta.
    pub fn single_volume(base_dir: impl Into<String>) -> Vec<Self> {
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

    pub(crate) fn size_limit_bytes(&self) -> Result<Option<u64>> {
        self.size_limit
            .map(|size| size_to_u64("volumes[].size_limit", size))
            .transpose()
            .map_err(Error::ConfigError)
    }
}

fn supports_primary_data(volume: &VolumeDescriptor) -> bool {
    volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh)
        || volume.supports(VolumeUsageKind::PrimaryDataPriorityMedium)
        || volume.supports(VolumeUsageKind::PrimaryDataPriorityLow)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReaderConfigEntry {
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: Size,
    pub reload_tolerance_seconds: u64,
}

impl Default for ReaderConfigEntry {
    fn default() -> Self {
        Self {
            pin_partition_in_memory_count: 1,
            block_cache_size: Size::from_mib(512),
            reload_tolerance_seconds: DEFAULT_READ_PROXY_RELOAD_TOLERANCE_SECONDS,
        }
    }
}

impl ReaderConfigEntry {
    pub(crate) fn block_cache_size_bytes(&self) -> Result<usize> {
        size_to_usize("reader.block_cache_size", self.block_cache_size).map_err(Error::ConfigError)
    }
}

#[derive(Clone, Debug)]
pub struct ReadOptions {
    pub column_family: Option<String>,
    pub column_indices: Option<Vec<usize>>,
    max_index: Option<usize>,
    cached_masks: Arc<ArcSwapOption<ReadOptionsMasks>>,
    cached_column_family_id: Arc<ArcSwapOption<ColumnFamilyCacheEntry>>,
}

#[derive(Clone)]
pub struct ScanOptions {
    pub read_ahead_bytes: Size,
    pub column_indices: Option<Vec<usize>>,
    pub column_family: Option<String>,
    max_index: Option<usize>,
    max_rows: Option<usize>,
    cached_resolution: Arc<ArcSwapOption<ScanOptionsCacheEntry>>,
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub ttl_seconds: Option<u32>,
    pub column_family: Option<String>,
    cached_column_family_id: Arc<ArcSwapOption<ColumnFamilyCacheEntry>>,
}

#[derive(Clone, Debug)]
struct ColumnFamilyCacheEntry {
    schema_version: u64,
    column_family_id: u8,
}

#[derive(Clone)]
pub(crate) struct ScanOptionsResolved {
    pub(crate) column_family_id: u8,
    pub(crate) effective_schema: Arc<Schema>,
}

#[derive(Clone)]
struct ScanOptionsCacheEntry {
    schema_version: u64,
    resolved: ScanOptionsResolved,
}

impl WriteOptions {
    pub fn with_ttl(ttl_seconds: u32) -> Self {
        Self {
            ttl_seconds: Some(ttl_seconds),
            column_family: None,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn with_column_family(column_family: impl Into<String>) -> Self {
        Self {
            ttl_seconds: None,
            column_family: Some(column_family.into()),
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn resolve_column_family_id_cached(&self, schema: &Schema) -> Result<u8> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_column_family_id.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.column_family_id);
        }
        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        self.cached_column_family_id
            .store(Some(Arc::new(ColumnFamilyCacheEntry {
                schema_version,
                column_family_id,
            })));
        Ok(column_family_id)
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
            column_family: None,
            column_indices: None,
            max_index: None,
            cached_masks: Arc::new(ArcSwapOption::empty()),
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            read_ahead_bytes: Size::from_const(0),
            column_indices: None,
            column_family: None,
            max_index: None,
            max_rows: None,
            cached_resolution: Arc::new(ArcSwapOption::empty()),
        }
    }
}

impl std::fmt::Debug for ScanOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanOptions")
            .field("read_ahead_bytes", &self.read_ahead_bytes)
            .field("column_indices", &self.column_indices)
            .field("column_family", &self.column_family)
            .field("max_index", &self.max_index)
            .field("max_rows", &self.max_rows)
            .finish()
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            ttl_seconds: None,
            column_family: None,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
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
            read_ahead_bytes: Size::from_const(0),
            column_indices,
            column_family: None,
            max_index,
            max_rows: None,
            cached_resolution: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.column_family = Some(column_family.into());
        self.invalidate_caches();
        self
    }

    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        assert!(max_rows > 0, "max_rows must be > 0");
        self.max_rows = Some(max_rows);
        self
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub fn max_rows(&self) -> Option<usize> {
        self.max_rows
    }

    pub fn set_max_rows(&mut self, max_rows: usize) {
        assert!(max_rows > 0, "max_rows must be > 0");
        self.max_rows = Some(max_rows);
    }

    pub fn clear_max_rows(&mut self) {
        self.max_rows = None;
    }

    pub(crate) fn read_ahead_bytes(&self) -> Result<usize> {
        size_to_usize("scan.read_ahead_bytes", self.read_ahead_bytes).map_err(Error::ConfigError)
    }

    pub(crate) fn resolve_cached(&self, schema: &Arc<Schema>) -> Result<ScanOptionsResolved> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_resolution.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.resolved.clone());
        }

        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        let effective_schema = if let Some(columns) = self.columns() {
            schema.project_in_family(column_family_id, columns)
        } else {
            Arc::clone(schema)
        };
        let resolved = ScanOptionsResolved {
            column_family_id,
            effective_schema,
        };
        self.cached_resolution
            .store(Some(Arc::new(ScanOptionsCacheEntry {
                schema_version,
                resolved: resolved.clone(),
            })));
        Ok(resolved)
    }

    fn invalidate_caches(&mut self) {
        self.cached_resolution = Arc::new(ArcSwapOption::empty());
    }
}

impl ReadOptions {
    pub fn for_column(column_index: usize) -> Self {
        Self::new_with_indices(None, Some(vec![column_index]))
    }

    pub fn for_columns(column_indices: Vec<usize>) -> Self {
        Self::new_with_indices(None, Some(column_indices))
    }

    pub fn for_column_in_family(column_family: impl Into<String>, column_index: usize) -> Self {
        Self::new_with_indices(Some(column_family.into()), Some(vec![column_index]))
    }

    pub fn for_columns_in_family(
        column_family: impl Into<String>,
        column_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_indices(Some(column_family.into()), Some(column_indices))
    }

    fn new_with_indices(column_family: Option<String>, column_indices: Option<Vec<usize>>) -> Self {
        let max_index = column_indices
            .as_ref()
            .and_then(|indices| indices.iter().max().cloned());
        Self {
            column_family,
            column_indices,
            max_index,
            cached_masks: Arc::new(ArcSwapOption::empty()),
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn with_column_family(mut self, column_family: impl Into<String>) -> Self {
        self.column_family = Some(column_family.into());
        self.invalidate_caches();
        self
    }

    pub(crate) fn columns(&self) -> Option<&[usize]> {
        self.column_indices.as_deref()
    }

    pub(crate) fn column_family(&self) -> Option<&str> {
        self.column_family.as_deref()
    }

    pub(crate) fn max_index(&self) -> Option<usize> {
        self.max_index
    }

    pub(crate) fn masks(&self, num_columns: usize) -> ReadOptionsMasks {
        if let Some(mask) = self.cached_masks.load_full()
            && mask.num_columns == num_columns
        {
            return mask.as_ref().clone();
        }
        let mask = Arc::new(self.build_masks(num_columns));
        self.cached_masks.store(Some(Arc::clone(&mask)));
        mask.as_ref().clone()
    }

    pub(crate) fn resolve_column_family_id_cached(&self, schema: &Schema) -> Result<u8> {
        let schema_version = schema.version();
        if let Some(cache) = self.cached_column_family_id.load_full()
            && cache.schema_version == schema_version
        {
            return Ok(cache.column_family_id);
        }
        let column_family_id = schema.resolve_column_family_id(self.column_family())?;
        self.cached_column_family_id
            .store(Some(Arc::new(ColumnFamilyCacheEntry {
                schema_version,
                column_family_id,
            })));
        Ok(column_family_id)
    }

    fn invalidate_caches(&mut self) {
        self.cached_masks = Arc::new(ArcSwapOption::empty());
        self.cached_column_family_id = Arc::new(ArcSwapOption::empty());
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
pub struct Config {
    /// Storage volume descriptors for this database.
    pub volumes: Vec<VolumeDescriptor>,
    /// Memtable capacity in bytes.
    pub memtable_capacity: Size,
    /// Number of memtable buffers to keep in memory.
    pub memtable_buffer_count: usize,
    /// Memtable implementation type.
    pub memtable_type: MemtableType,
    /// Initial number of columns in the default column family when creating a brand-new DB.
    /// Existing DBs load their schema from persisted metadata instead of this setting.
    pub num_columns: usize,
    /// Total number of buckets in the cluster. Should be 1~65536.
    pub total_buckets: u32,
    /// Maximum number of L0 files before triggering compaction.
    pub l0_file_limit: usize,
    /// Maximum number of immutables + L0 files before write stall.
    /// If None, uses min(l0_file_limit + 4, l0_file_limit * 2).
    pub write_stall_limit: Option<usize>,
    /// Base size for level 1.
    pub l1_base_bytes: Size,
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
    /// Maximum number of concurrent requests the remote compaction server will process.
    pub compaction_server_max_concurrent: usize,
    /// Maximum number of queued requests before the server rejects new connections.
    pub compaction_server_max_queued: usize,
    /// Size of the block cache in bytes. If zero, cache is disabled.
    pub block_cache_size: Size,
    /// Enable foyer hybrid block cache (memory + local disk).
    pub block_cache_hybrid_enabled: bool,
    /// Optional disk capacity for hybrid block cache in bytes.
    /// If unset, defaults to the in-memory block cache size.
    pub block_cache_hybrid_disk_size: Option<Size>,
    /// Read proxy configuration overrides.
    pub reader: ReaderConfigEntry,
    /// Target base SST file size in bytes.
    pub base_file_size: Size,
    /// Enable bloom filter in SST files.
    pub sst_bloom_filter_enabled: bool,
    /// Bits per key for SST bloom filter when enabled.
    pub sst_bloom_bits_per_key: u32,
    /// Whether to enable two-level index and filter blocks in SST files.
    pub sst_partitioned_index: bool,
    /// Output data-file format used by flush/compaction writers.
    pub data_file_type: DataFileType,
    /// Target parquet row-group size in bytes when parquet output format is selected.
    pub parquet_row_group_size_bytes: Size,
    /// Compression algorithm per level (index by level number).
    pub sst_compression_by_level: Vec<SstCompressionAlgorithm>,
    /// Whether TTL is enabled. If false, TTL metadata is ignored.
    pub ttl_enabled: bool,
    /// Default TTL duration (in seconds). None means no expiration by default.
    pub default_ttl_seconds: Option<u32>,
    /// Values larger than this threshold are marked for value-log separation.
    /// None disables value-log separation.
    pub value_separation_threshold: Option<Size>,
    /// Time provider to use for TTL.
    pub time_provider: TimeProviderKind,
    /// Optional log file path. If None, logs go to console only. Must be a local path.
    pub log_path: Option<String>,
    /// Maximum size of the active log file before rolling.
    pub log_max_file_size: Size,
    /// Total number of log files to keep, including the active file.
    pub log_keep_files: usize,
    /// Size in bytes of each JNI direct buffer used by Java direct-buffer APIs.
    pub jni_direct_buffer_size: Size,
    /// Maximum number of JNI direct buffers kept in the Java-side pool.
    pub jni_direct_buffer_pool_size: usize,
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
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble"),
            memtable_capacity: Size::from_mib(64),
            memtable_buffer_count: 2,
            memtable_type: MemtableType::Hash,
            num_columns: 1,
            total_buckets: 1,
            l0_file_limit: 4,
            write_stall_limit: None,
            l1_base_bytes: Size::from_mib(256),
            level_size_multiplier: 10,
            max_level: 6,
            compaction_policy: CompactionPolicyKind::RoundRobin,
            compaction_read_ahead_enabled: true,
            compaction_remote_addr: None,
            compaction_threads: 4,
            compaction_remote_timeout_ms: 300_000,
            compaction_server_max_concurrent: 4,
            compaction_server_max_queued: 64,
            block_cache_size: Size::from_mib(64),
            block_cache_hybrid_enabled: false,
            block_cache_hybrid_disk_size: None,
            reader: ReaderConfigEntry::default(),
            base_file_size: Size::from_mib(64),
            sst_bloom_filter_enabled: false,
            sst_bloom_bits_per_key: 10,
            sst_partitioned_index: false,
            data_file_type: DataFileType::SSTable,
            parquet_row_group_size_bytes: Size::from_kib(256),
            sst_compression_by_level: vec![
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::Lz4,
            ],
            ttl_enabled: false,
            default_ttl_seconds: None,
            value_separation_threshold: None,
            time_provider: TimeProviderKind::default(),
            log_path: None,
            log_max_file_size: Size::from_mib(10),
            log_keep_files: 3,
            jni_direct_buffer_size: Size::from_kib(2),
            jni_direct_buffer_pool_size: 64,
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
    pub fn from_json_str(contents: &str) -> Result<Self> {
        let provided = serde_json::from_str::<JsonValue>(contents)
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        let schema = serde_json::to_value(Config::default())
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        let unrecognized = collect_unrecognized_entry_paths(&provided, &schema, "");
        for entry in unrecognized {
            warn!("unrecognized entry: {}", entry);
        }

        let default_json = serde_json::to_string(&Config::default())
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        let provided_json =
            serde_json::to_string(&provided).map_err(|err| Error::ConfigError(err.to_string()))?;

        let mut builder = ConfigLoader::builder();
        builder = builder.add_source(ConfigFile::from_str(&default_json, ConfigFileFormat::Json));
        builder = builder.add_source(ConfigFile::from_str(&provided_json, ConfigFileFormat::Json));
        let config: Config = builder
            .build()
            .map_err(|err| Error::ConfigError(err.to_string()))?
            .try_deserialize()
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        config.validate_sizes()?;
        Ok(config)
    }

    pub(crate) fn memtable_capacity_bytes(&self) -> Result<usize> {
        size_to_usize("memtable_capacity", self.memtable_capacity).map_err(Error::ConfigError)
    }

    pub(crate) fn l1_base_bytes_bytes(&self) -> Result<usize> {
        size_to_usize("l1_base_bytes", self.l1_base_bytes).map_err(Error::ConfigError)
    }

    pub(crate) fn block_cache_size_bytes(&self) -> Result<usize> {
        size_to_usize("block_cache_size", self.block_cache_size).map_err(Error::ConfigError)
    }

    pub(crate) fn block_cache_hybrid_disk_size_bytes(&self) -> Result<Option<usize>> {
        self.block_cache_hybrid_disk_size
            .map(|size| size_to_usize("block_cache_hybrid_disk_size", size))
            .transpose()
            .map_err(Error::ConfigError)
    }

    pub(crate) fn base_file_size_bytes(&self) -> Result<usize> {
        size_to_usize("base_file_size", self.base_file_size).map_err(Error::ConfigError)
    }

    pub(crate) fn parquet_row_group_size_bytes(&self) -> Result<usize> {
        size_to_usize(
            "parquet_row_group_size_bytes",
            self.parquet_row_group_size_bytes,
        )
        .map_err(Error::ConfigError)
    }

    pub(crate) fn value_separation_threshold_bytes(&self) -> Result<usize> {
        self.value_separation_threshold
            .map(|size| size_to_usize("value_separation_threshold", size))
            .transpose()
            .map_err(Error::ConfigError)
            .map(|size| size.unwrap_or(0))
    }

    pub(crate) fn log_max_file_size_bytes(&self) -> Result<u64> {
        size_to_u64("log_max_file_size", self.log_max_file_size).map_err(Error::ConfigError)
    }

    pub fn jni_direct_buffer_size_bytes(&self) -> Result<usize> {
        size_to_usize("jni_direct_buffer_size", self.jni_direct_buffer_size)
            .map_err(Error::ConfigError)
    }

    pub(crate) fn hybrid_block_cache_disk_size(
        &self,
        memory_capacity: usize,
    ) -> Result<Option<usize>> {
        if !self.block_cache_hybrid_enabled || memory_capacity == 0 {
            return Ok(None);
        }
        let disk = self
            .block_cache_hybrid_disk_size
            .map(|size| size_to_usize("block_cache_hybrid_disk_size", size))
            .transpose()
            .map_err(Error::ConfigError)?
            .unwrap_or(memory_capacity);
        Ok(Some(disk))
    }

    /// Select a suitable volume for hybrid block cache based on the config and the required disk capacity.
    pub(crate) fn resolve_hybrid_cache_volume_plan(
        &self,
        memory_capacity: usize,
    ) -> Result<Option<HybridCacheVolumePlan>> {
        let Some(disk_capacity_bytes) = self.hybrid_block_cache_disk_size(memory_capacity)? else {
            return Ok(None);
        };
        if disk_capacity_bytes == 0 {
            return Err(Error::ConfigError(
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
                Error::ConfigError(format!(
                    "Invalid cache volume URL {}: {}",
                    normalized_base_dir, err
                ))
            })?;
            if !url.scheme().eq_ignore_ascii_case("file") {
                continue;
            }
            has_local_cache_volume = true;
            let volume_limit = volume
                .size_limit
                .map(|limit| size_to_u64(&format!("volumes[{idx}].size_limit"), limit))
                .transpose()
                .map_err(Error::ConfigError)?;
            let fits = match volume_limit {
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
            return Err(Error::ConfigError(
                "Hybrid block cache enabled but no volume is configured with cache usage"
                    .to_string(),
            ));
        }
        if !has_local_cache_volume {
            return Err(Error::ConfigError(
                "Hybrid block cache requires a local file:// cache volume".to_string(),
            ));
        }
        Err(Error::ConfigError(format!(
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
            Error::ConfigError(format!(
                "Selected hybrid cache volume index {} out of range",
                plan.volume_idx
            ))
        })?;
        if let Some(limit) = volume.size_limit {
            let limit = size_to_u64(&format!("volumes[{}].size_limit", plan.volume_idx), limit)
                .map_err(Error::ConfigError)?;
            if limit <= disk_bytes {
                return Err(Error::ConfigError(format!(
                    "Hybrid cache reservation {} bytes exceeds shared volume limit {} bytes for {}",
                    disk_bytes, limit, volume.base_dir
                )));
            }
            volume.size_limit = Some(Size::from_const((limit - disk_bytes) as i64));
        }
        Ok(adjusted)
    }

    pub fn from_path(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase())
            .ok_or_else(|| Error::ConfigError("Config path missing extension".to_string()))?;
        let format = match extension.as_str() {
            "yaml" | "yml" => ConfigFileFormat::Yaml,
            "ini" => ConfigFileFormat::Ini,
            "json" => ConfigFileFormat::Json,
            "toml" => ConfigFileFormat::Toml,
            _ => {
                return Err(Error::ConfigError(format!(
                    "Unsupported config format: {}",
                    extension
                )));
            }
        };

        let provided = match extension.as_str() {
            "json" => {
                let contents = std::fs::read_to_string(path)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                let parsed = serde_json::from_str::<JsonValue>(&contents)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                Some(parsed)
            }
            "yaml" | "yml" => {
                let contents = std::fs::read_to_string(path)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                let parsed = serde_yaml::from_str::<serde_yaml::Value>(&contents)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                Some(
                    serde_json::to_value(parsed)
                        .map_err(|err| Error::ConfigError(err.to_string()))?,
                )
            }
            "toml" => {
                let contents = std::fs::read_to_string(path)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                let parsed = toml::from_str::<TomlValue>(&contents)
                    .map_err(|err| Error::ConfigError(err.to_string()))?;
                Some(
                    serde_json::to_value(parsed)
                        .map_err(|err| Error::ConfigError(err.to_string()))?,
                )
            }
            _ => None,
        };

        let schema = serde_json::to_value(Config::default())
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        if let Some(provided) = provided.as_ref() {
            let unrecognized = collect_unrecognized_entry_paths(provided, &schema, "");
            for entry in unrecognized {
                warn!("unrecognized entry: {}", entry);
            }
        }

        let default_json = serde_json::to_string(&Config::default())
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        let mut builder = ConfigLoader::builder();
        builder = builder.add_source(ConfigFile::from_str(&default_json, ConfigFileFormat::Json));
        builder = builder.add_source(ConfigFile::from(path).format(format));
        let config: Config = builder
            .build()
            .map_err(|err| Error::ConfigError(err.to_string()))?
            .try_deserialize()
            .map_err(|err| Error::ConfigError(err.to_string()))?;
        config.validate_sizes()?;
        Ok(config)
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

    fn validate_sizes(&self) -> Result<()> {
        self.memtable_capacity_bytes()?;
        self.l1_base_bytes_bytes()?;
        self.block_cache_size_bytes()?;
        self.block_cache_hybrid_disk_size_bytes()?;
        self.reader.block_cache_size_bytes()?;
        self.base_file_size_bytes()?;
        self.parquet_row_group_size_bytes()?;
        self.value_separation_threshold_bytes()?;
        if self.jni_direct_buffer_size_bytes()? == 0 {
            return Err(Error::ConfigError(
                "jni_direct_buffer_size must be greater than 0".to_string(),
            ));
        }
        if self.log_max_file_size_bytes()? == 0 {
            return Err(Error::ConfigError(
                "log_max_file_size must be greater than 0".to_string(),
            ));
        }
        if self.log_keep_files == 0 {
            return Err(Error::ConfigError(
                "log_keep_files must be greater than 0".to_string(),
            ));
        }
        if self.jni_direct_buffer_pool_size == 0 {
            return Err(Error::ConfigError(
                "jni_direct_buffer_pool_size must be greater than 0".to_string(),
            ));
        }
        for (idx, volume) in self.volumes.iter().enumerate() {
            if let Some(limit) = volume.size_limit {
                size_to_u64(&format!("volumes[{idx}].size_limit"), limit)
                    .map_err(Error::ConfigError)?;
            }
        }
        Ok(())
    }
}

fn collect_unrecognized_entry_paths(
    provided: &JsonValue,
    schema: &JsonValue,
    path: &str,
) -> Vec<String> {
    match (provided, schema) {
        (JsonValue::Object(provided_map), JsonValue::Object(schema_map)) => {
            let mut unknown = Vec::new();
            for (key, value) in provided_map {
                let current_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", path, key)
                };
                if let Some(schema_value) = schema_map.get(key) {
                    unknown.extend(collect_unrecognized_entry_paths(
                        value,
                        schema_value,
                        &current_path,
                    ));
                } else {
                    unknown.push(current_path);
                }
            }
            unknown
        }
        (JsonValue::Array(provided_items), JsonValue::Array(schema_items)) => {
            let mut unknown = Vec::new();
            if let Some(schema_item) = schema_items.first() {
                for (idx, provided_item) in provided_items.iter().enumerate() {
                    let current_path = format!("{}[{}]", path, idx);
                    unknown.extend(collect_unrecognized_entry_paths(
                        provided_item,
                        schema_item,
                        &current_path,
                    ));
                }
            }
            unknown
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Config, Error, MemtableType, PrimaryVolumeOffloadPolicyKind, ReadOptions,
        ReaderConfigEntry, ScanOptions, VolumeDescriptor, VolumeUsageKind, WriteOptions,
    };
    use crate::SstCompressionAlgorithm;
    use crate::data_file::DataFileType;
    use crate::schema::Schema;
    use size::Size;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::Builder;

    #[test]
    fn test_config_from_file_round_trip() {
        let mut volume = VolumeDescriptor::new(
            "file:///tmp/cobble".to_string(),
            vec![
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Meta,
            ],
        );
        volume.custom_options = Some(
            [
                ("endpoint".to_string(), "http://127.0.0.1:9000".to_string()),
                ("region".to_string(), "us-east-1".to_string()),
            ]
            .into_iter()
            .collect(),
        );
        let config = Config {
            volumes: vec![volume],
            memtable_capacity: Size::from_kib(1),
            memtable_buffer_count: 3,
            memtable_type: MemtableType::Vec,
            num_columns: 2,
            total_buckets: 1024,
            l0_file_limit: 5,
            write_stall_limit: Some(12),
            l1_base_bytes: Size::from_kib(8),
            level_size_multiplier: 7,
            max_level: 4,
            compaction_policy: super::CompactionPolicyKind::MinOverlap,
            block_cache_size: Size::from_const(256),
            block_cache_hybrid_enabled: true,
            block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
            reader: ReaderConfigEntry {
                pin_partition_in_memory_count: 2,
                block_cache_size: Size::from_kib(2),
                reload_tolerance_seconds: 5,
            },
            base_file_size: Size::from_const(512),
            sst_bloom_filter_enabled: true,
            sst_bloom_bits_per_key: 11,
            sst_partitioned_index: true,
            data_file_type: DataFileType::Parquet,
            parquet_row_group_size_bytes: Size::from_kib(4),
            sst_compression_by_level: vec![
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::Lz4,
            ],
            ttl_enabled: true,
            default_ttl_seconds: Some(120),
            value_separation_threshold: Some(Size::from_kib(4)),
            time_provider: crate::time::TimeProviderKind::Manual,
            log_path: Some("/tmp/cobble.log".to_string()),
            log_max_file_size: Size::from_mib(16),
            log_keep_files: 5,
            jni_direct_buffer_size: Size::from_kib(8),
            jni_direct_buffer_pool_size: 32,
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
            compaction_server_max_concurrent: 8,
            compaction_server_max_queued: 32,
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
        assert_eq!(
            decoded.volumes[0]
                .custom_options
                .as_ref()
                .and_then(|v| v.get("endpoint")),
            Some(&"http://127.0.0.1:9000".to_string())
        );
        assert_eq!(decoded.memtable_capacity, Size::from_kib(1));
        assert_eq!(decoded.memtable_type, MemtableType::Vec);
        assert_eq!(decoded.total_buckets, 1024);
        assert_eq!(decoded.write_stall_limit, Some(12));
        assert_eq!(
            decoded.compaction_policy,
            super::CompactionPolicyKind::MinOverlap
        );
        assert!(decoded.sst_partitioned_index);
        assert_eq!(decoded.time_provider, crate::time::TimeProviderKind::Manual);
        assert_eq!(decoded.log_max_file_size, Size::from_mib(16));
        assert_eq!(decoded.log_keep_files, 5);
        assert_eq!(decoded.jni_direct_buffer_size, Size::from_kib(8));
        assert_eq!(decoded.jni_direct_buffer_pool_size, 32);
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
        assert_eq!(decoded.value_separation_threshold, Some(Size::from_kib(4)));
        assert_eq!(decoded.compaction_server_max_concurrent, 8);
        assert_eq!(decoded.compaction_server_max_queued, 32);
        assert_eq!(decoded.data_file_type, DataFileType::Parquet);
        assert_eq!(decoded.parquet_row_group_size_bytes, Size::from_kib(4));
        assert_eq!(decoded.reader.block_cache_size, Size::from_kib(2));
        assert_eq!(decoded.reader.reload_tolerance_seconds, 5);
        assert!(decoded.block_cache_hybrid_enabled);
        assert_eq!(
            decoded.block_cache_hybrid_disk_size,
            Some(Size::from_kib(1))
        );

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
        assert_eq!(decoded_yaml.reader.block_cache_size, Size::from_kib(2));

        let mut path_buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path_buf.push("tests/testdata/config.ini");

        let decoded_ini = Config::from_path(path_buf.as_path()).expect("Cannot deserialize ini");
        assert_eq!(decoded_ini.memtable_capacity, Size::from_kib(1));
        assert_eq!(decoded_ini.reader.block_cache_size, Size::from_kib(2));
        assert_eq!(decoded_ini.data_file_type, DataFileType::SSTable);
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
    fn test_read_options_column_family_constructors() {
        let options = ReadOptions::for_columns_in_family("metrics", vec![2, 0]);
        assert_eq!(options.column_family(), Some("metrics"));
        assert_eq!(options.columns(), Some(&[2, 0][..]));

        let options = ReadOptions::for_column(1).with_column_family("default");
        assert_eq!(options.column_family(), Some("default"));
        assert_eq!(options.columns(), Some(&[1][..]));
    }

    #[test]
    fn test_scan_options_column_family_builder() {
        let options = ScanOptions::for_columns(vec![2, 0]).with_column_family("metrics");
        assert_eq!(options.column_family(), Some("metrics"));
        assert_eq!(options.columns(), Some(&[2, 0][..]));

        let options = ScanOptions::for_column(1).with_column_family("default");
        assert_eq!(options.column_family(), Some("default"));
        assert_eq!(options.columns(), Some(&[1][..]));
    }

    #[test]
    fn test_write_options_column_family_constructor() {
        let options = WriteOptions::with_column_family("metrics");
        assert_eq!(options.column_family(), Some("metrics"));
        assert_eq!(options.ttl_seconds, None);
    }

    #[test]
    fn test_write_options_column_family_cache_invalidates_on_schema_change() {
        let schema_v1 = Schema::new_for_column_family(1, 1, vec![], vec![]);
        let schema_v2 = Schema::new_for_column_family(2, 2, vec![], vec![]);
        let options = WriteOptions::with_column_family("remote-cf-1");

        let resolved = options.resolve_column_family_id_cached(&schema_v1).unwrap();
        assert_eq!(resolved, 1);

        let err = options
            .resolve_column_family_id_cached(&schema_v2)
            .expect_err("schema version change should invalidate cache");
        assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
    }

    #[test]
    fn test_read_options_cache_invalidates_via_with_column_family() {
        let schema = Schema::new_for_column_family(1, 1, vec![], vec![]);
        let options = ReadOptions::for_column(0);
        assert_eq!(options.resolve_column_family_id_cached(&schema).unwrap(), 0);

        let options = options.with_column_family("remote-cf-1");
        assert_eq!(options.resolve_column_family_id_cached(&schema).unwrap(), 1);

        let options = options.with_column_family("missing");
        let err = options
            .resolve_column_family_id_cached(&schema)
            .expect_err("with_column_family should invalidate and re-resolve");
        assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
    }

    #[test]
    fn test_scan_options_cache_invalidates_via_with_column_family() {
        let schema = Arc::new(Schema::new(7, 3, vec![]));
        let options = ScanOptions::for_column(0);

        let first = options.resolve_cached(&schema).unwrap();
        assert_eq!(first.effective_schema.num_columns_in_family(0), Some(1));

        let options = options.with_column_family("missing");
        let err = match options.resolve_cached(&schema) {
            Ok(_) => panic!("with_column_family should invalidate and re-resolve"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::IoError(msg) if msg.contains("Unknown column family")));
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
        let config = Config {
            block_cache_hybrid_enabled: true,
            block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
            volumes: vec![
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
            ],
            ..Config::default()
        };
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
        let mut shared = VolumeDescriptor::new(
            "file:///tmp/shared".to_string(),
            vec![
                VolumeUsageKind::PrimaryDataPriorityHigh,
                VolumeUsageKind::Cache,
                VolumeUsageKind::Meta,
            ],
        );
        shared.size_limit = Some(Size::from_kib(8));
        let config = Config {
            block_cache_hybrid_enabled: true,
            block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
            volumes: vec![shared],
            ..Config::default()
        };
        let plan = config.resolve_hybrid_cache_volume_plan(4096).unwrap();
        let adjusted = config
            .apply_hybrid_cache_primary_partition_with_plan(plan.as_ref())
            .unwrap();
        assert_eq!(adjusted.volumes[0].size_limit, Some(Size::from_kib(7)));
    }

    #[test]
    fn test_hybrid_cache_rejects_non_local_cache_volume() {
        let config = Config {
            block_cache_hybrid_enabled: true,
            block_cache_hybrid_disk_size: Some(Size::from_kib(1)),
            volumes: vec![VolumeDescriptor::new(
                "s3://bucket/cache".to_string(),
                vec![VolumeUsageKind::Cache],
            )],
            ..Config::default()
        };
        let err = config.resolve_hybrid_cache_volume_plan(2048).unwrap_err();
        assert!(matches!(err, Error::ConfigError(_)));
    }

    #[test]
    fn test_data_file_type_missing_field_is_rejected() {
        let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "num_columns": 1
        }"#;
        let err = serde_json::from_str::<Config>(json).unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn test_data_file_type_parquet_round_trip() {
        let expected = Config {
            data_file_type: DataFileType::Parquet,
            parquet_row_group_size_bytes: Size::from_kib(8),
            ..Config::default()
        };
        let json = serde_json::to_string(&expected).expect("Cannot serialize config");
        let decoded: Config =
            serde_json::from_str(&json).expect("Cannot deserialize parquet config");
        assert_eq!(decoded.data_file_type, DataFileType::Parquet);
        assert_eq!(decoded.parquet_row_group_size_bytes, Size::from_kib(8));
    }

    #[test]
    fn test_config_from_path_allows_partial_entries() {
        let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "memtable_capacity": 2048
        }"#;
        let mut json_file = Builder::new()
            .suffix(".json")
            .tempfile()
            .expect("Should create temp json");
        json_file
            .write_all(json.as_bytes())
            .expect("Should be able to write json");
        json_file.flush().expect("Should be able to flush json");

        let decoded = Config::from_path(json_file.path()).expect("Cannot deserialize partial json");
        assert_eq!(decoded.memtable_capacity, Size::from_kib(2));
        assert_eq!(decoded.num_columns, Config::default().num_columns);
        assert_eq!(decoded.data_file_type, Config::default().data_file_type);
    }

    #[test]
    fn test_config_from_json_str_allows_partial_entries() {
        let json = r#"{
            "volumes": [{"base_dir":"file:///tmp/cobble","kinds":["meta","primary_data_priority_high"]}],
            "memtable_capacity": 2048
        }"#;
        let decoded = Config::from_json_str(json).expect("Cannot deserialize partial json");
        assert_eq!(decoded.memtable_capacity, Size::from_kib(2));
        assert_eq!(decoded.num_columns, Config::default().num_columns);
        assert_eq!(decoded.data_file_type, Config::default().data_file_type);
    }

    #[test]
    fn test_config_from_path_parses_human_readable_sizes() {
        let yaml = r#"
        volumes:
          - base_dir: "file:///tmp/cobble"
            kinds: ["meta", "primary_data_priority_high"]
            size_limit: "2GiB"
        memtable_capacity: "64MB"
        l1_base_bytes: "128MiB"
        block_cache_size: "32MB"
        block_cache_hybrid_disk_size: "1GiB"
        reader:
          pin_partition_in_memory_count: 1
          block_cache_size: "512MB"
          reload_tolerance_seconds: 10
        base_file_size: "64MiB"
        parquet_row_group_size_bytes: "256KB"
        value_separation_threshold: "4MB"
        "#;
        let mut file = Builder::new()
            .suffix(".yaml")
            .tempfile()
            .expect("should create temp yaml");
        file.write_all(yaml.as_bytes())
            .expect("should write temp yaml");
        file.flush().expect("should flush temp yaml");

        let decoded = Config::from_path(file.path()).expect("should parse human-readable sizes");
        assert_eq!(decoded.memtable_capacity, Size::from_const(64_000_000));
        assert_eq!(decoded.l1_base_bytes, Size::from_mib(128));
        assert_eq!(decoded.block_cache_size, Size::from_const(32_000_000));
        assert_eq!(
            decoded.block_cache_hybrid_disk_size,
            Some(Size::from_gib(1))
        );
        assert_eq!(
            decoded.reader.block_cache_size,
            Size::from_const(512_000_000)
        );
        assert_eq!(decoded.base_file_size, Size::from_mib(64));
        assert_eq!(
            decoded.parquet_row_group_size_bytes,
            Size::from_const(256_000)
        );
        assert_eq!(
            decoded.value_separation_threshold,
            Some(Size::from_const(4_000_000))
        );
        assert_eq!(decoded.volumes[0].size_limit, Some(Size::from_gib(2)));
    }

    #[test]
    fn test_config_from_path_rejects_invalid_size_unit() {
        let yaml = r#"
        volumes:
          - base_dir: "file:///tmp/cobble"
            kinds: ["meta", "primary_data_priority_high"]
        memtable_capacity: "64MEGA"
        "#;
        let mut file = Builder::new()
            .suffix(".yaml")
            .tempfile()
            .expect("should create temp yaml");
        file.write_all(yaml.as_bytes())
            .expect("should write temp yaml");
        file.flush().expect("should flush temp yaml");

        let err = Config::from_path(file.path()).expect_err("invalid unit should be rejected");
        assert!(matches!(err, Error::ConfigError(_)));
    }

    #[test]
    fn test_collect_unrecognized_entry_paths() {
        let provided = serde_json::json!({
            "num_columns": 1,
            "unknown_top": 1,
            "reader": {
                "block_cache_size": 1024,
                "unknown_nested": true
            },
            "volumes": [{
                "base_dir": "file:///tmp/cobble",
                "kinds": ["meta", "primary_data_priority_high"],
                "unknown_volume_key": "x"
            }]
        });
        let schema = serde_json::to_value(Config::default()).expect("serialize default config");
        let unknown = super::collect_unrecognized_entry_paths(&provided, &schema, "");
        assert!(unknown.contains(&"unknown_top".to_string()));
        assert!(unknown.contains(&"reader.unknown_nested".to_string()));
        assert!(unknown.contains(&"volumes[0].unknown_volume_key".to_string()));
    }

    #[test]
    fn test_template_config_yaml_is_valid() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../template/config.yaml");
        let parsed = Config::from_path(path).expect("template/config.yaml should be valid");
        assert_eq!(parsed.total_buckets, 1);
        assert_eq!(parsed.memtable_type, MemtableType::Hash);
        assert_eq!(parsed.data_file_type, DataFileType::SSTable);
        assert_eq!(
            parsed.primary_volume_offload_policy,
            PrimaryVolumeOffloadPolicyKind::Priority
        );
        assert_eq!(parsed.volumes.len(), 1);
        assert!(parsed.volumes[0].supports(VolumeUsageKind::PrimaryDataPriorityHigh));
        assert!(parsed.volumes[0].supports(VolumeUsageKind::Meta));
    }
}
