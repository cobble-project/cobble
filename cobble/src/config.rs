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

fn default_wal_flush_interval_ms() -> u64 {
    5
}

fn deserialize_optional_sst_level<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match Option::<i16>::deserialize(deserializer)? {
        None | Some(-1) => Ok(None),
        Some(level @ 0..=255) => Ok(Some(level as u8)),
        Some(level) => Err(serde::de::Error::custom(format!(
            "SST level must be between -1 and 255, but was {level}"
        ))),
    }
}

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
                "wal" => add_kind(&mut mask, VolumeUsageKind::Wal),
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
    ScorePriority,
}

/// How the DB reacts when a remote compaction attempt fails.
///
/// Remote compaction is best-effort: the DB must stay open and writable even when the compactor is
/// down. Failures are classified as transient (connect refused, timeout, connection reset, I/O) or
/// permanent (protocol incompatible, unsupported merge operator, malformed schema, config error).
/// Transient failures are handled according to this mode; permanent failures always give up the
/// current attempt without falling back, so a misconfiguration is surfaced rather than silently
/// masked by local compaction.
///
/// - `FallbackLocal` (default): run the compaction locally instead.
/// - `Skip`: abandon this compaction attempt, release the pending slot, and leave the DB healthy.
///   The next flush or write that re-triggers compaction will retry remote, so a recovered
///   compactor is picked up automatically.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCompactionFailureMode {
    #[default]
    FallbackLocal,
    Skip,
}

/// Compaction execution mode.
///
/// - `Embedded` (default): compaction runs in-process via a local or remote worker.
/// - `Dedicated`: compaction is performed by a separate dedicated compactor process. The writer
///   disables all in-process compaction (local and remote) and auto-split, and instead polls the
///   shared volume for compaction result files. Runtime manifests are enabled by default; when
///   disabled, the existing snapshot-driven publication path remains available. `Dedicated` and
///   `compaction_remote_addr` are mutually exclusive.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionMode {
    #[default]
    Embedded,
    Dedicated,
}

/// Controls publication of durable manifests for the persisted runtime LSM state.
///
/// `Auto` enables them for dedicated compaction and leaves them off for embedded compaction.
/// `Enabled` and `Disabled` explicitly override that default.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeManifestMode {
    #[default]
    Auto,
    Enabled,
    Disabled,
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
///
/// `Adaptive` is a strategy, not a concrete memtable: when selected, the memtable manager starts
/// with `Skiplist` and an [`crate::memtable::AdaptiveMemtableController`] that monitors read/write/
/// scan patterns and switches the concrete type at runtime. The controller transitions toward
/// `Vec` on pure-write windows and toward `Hash` on point-read-heavy windows (with no scans); it
/// rolls back to `Skiplist` (flushing the current memtable) when a specialized type encounters an
/// unsupported pattern (`Vec` under any reads, `Hash` under any scan), and otherwise keeps the
/// current concrete type.
///
/// At runtime, [`crate::Db::switch_memtable_type`] accepts both concrete types and `Adaptive`:
/// switching to a concrete type pins the memtable to that type and **disables** adaptive
/// statistics; switching to `Adaptive` **re-enables** statistics and resumes from the controller's
/// last known concrete type.
///
/// `Adaptive` is the default for both Rust and Java.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemtableType {
    Hash,
    Skiplist,
    Vec,
    #[default]
    Adaptive,
}

impl MemtableType {
    /// Resolves a strategy type to the concrete memtable type used to build the first buffer.
    /// `Adaptive` resolves to `Skiplist`; all other variants are returned as-is.
    pub(crate) fn resolve(self) -> Self {
        match self {
            Self::Adaptive => Self::Skiplist,
            other => other,
        }
    }
}

/// Caching policy for decoded SST footer and index-partition metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SstReadMetadataCacheMode {
    /// Build and attach decoded metadata while writing new SST files.
    #[default]
    Eager,
    /// Decode and cache metadata when an SST is first read.
    Lazy,
    /// Decode metadata separately for every reader.
    Off,
}

impl SstReadMetadataCacheMode {
    pub(crate) fn caches_reads(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub(crate) fn embeds_on_write(self) -> bool {
        matches!(self, Self::Eager)
    }
}

/// Governance coordination mode used during writable DB open.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceMode {
    #[default]
    Filesystem,
    Noop,
}

/// Volume usage classification.
/// Used to indicate which volumes support which kinds of data.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeUsageKind {
    // Metadata storage (manifests, snapshots, etc).
    Meta = 0,
    // Primary data storage with the highest priority (SST files).
    PrimaryDataPriorityHigh = 1,
    // Primary data storage with medium priority (SST files).
    PrimaryDataPriorityMedium = 2,
    // Primary data storage with low priority (SST files).
    PrimaryDataPriorityLow = 3,
    // Snapshot materialization storage (snapshot manifests, schema, and uploaded snapshot data).
    Snapshot = 4,
    // Block cache storage. e.g. foryer cache files.
    Cache = 5,
    // Read-only source volume used only for loading historical snapshot data.
    Readonly = 6,
    // Write-ahead log storage.
    Wal = 7,
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

/// Returns a volume descriptor safe to persist in metadata.
pub(crate) fn sanitize_volume_descriptor(volume: &VolumeDescriptor) -> VolumeDescriptor {
    let mut sanitized = volume.clone();
    sanitized.access_id = None;
    sanitized.secret_key = None;
    if let Some(options) = &mut sanitized.custom_options {
        options.retain(|key, _| !is_sensitive_volume_option(key));
        if options.is_empty() {
            sanitized.custom_options = None;
        }
    }
    if let Ok(mut url) = Url::parse(&sanitized.base_dir) {
        let _ = url.set_username("");
        let _ = url.set_password(None);
        if url.query().is_some() {
            let retained: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(key, _)| !is_sensitive_volume_option(key))
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect();
            url.set_query(None);
            if !retained.is_empty() {
                url.query_pairs_mut().extend_pairs(retained);
            }
        }
        sanitized.base_dir = url.to_string();
    }
    sanitized
}

/// Resolves credentials for a persisted volume descriptor from the current process configuration.
pub(crate) fn resolve_volume_descriptor_credentials(
    route: &VolumeDescriptor,
    current_config: &Config,
) -> VolumeDescriptor {
    let mut resolved = route.clone();
    let route_identity = volume_descriptor_identity(route);
    if let Some(current) = current_config
        .volumes
        .iter()
        .find(|candidate| volume_descriptor_identity(candidate) == route_identity)
    {
        resolved.base_dir = current.base_dir.clone();
        resolved.access_id = current.access_id.clone();
        resolved.secret_key = current.secret_key.clone();
        if let Some(options) = current.custom_options.as_ref() {
            let target = resolved.custom_options.get_or_insert_with(HashMap::new);
            for (key, value) in options {
                if is_sensitive_volume_option(key) {
                    target.insert(key.clone(), value.clone());
                }
            }
        }
    }
    resolved
}

pub(crate) fn volume_descriptor_identity(volume: &VolumeDescriptor) -> String {
    let sanitized = sanitize_volume_descriptor(volume);
    normalize_storage_path_to_url(&sanitized.base_dir).unwrap_or(sanitized.base_dir)
}

fn is_sensitive_volume_option(key: &str) -> bool {
    matches!(
        key.trim().to_ascii_lowercase().replace('-', "_").as_str(),
        "access_id"
            | "access_key"
            | "access_key_id"
            | "aws_access_key_id"
            | "secret_key"
            | "secret_access_key"
            | "aws_secret_access_key"
            | "session_token"
            | "aws_session_token"
    )
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
    preload_scan_cursor_block: bool,
    should_stop_at_block_boundary: bool,
    max_index: Option<usize>,
    max_rows: Option<usize>,
    cached_resolution: Arc<ArcSwapOption<ScanOptionsCacheEntry>>,
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub ttl_seconds: Option<u32>,
    pub column_family: Option<String>,
    /// When WAL is enabled, wait until this write's WAL segment is durably published.
    ///
    /// Disabling this keeps the write immediately visible in the current process, but a crash
    /// may lose the not-yet-published WAL tail.
    pub await_durable: bool,
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
            await_durable: true,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }

    pub fn with_column_family(column_family: impl Into<String>) -> Self {
        Self {
            ttl_seconds: None,
            column_family: Some(column_family.into()),
            await_durable: true,
            cached_column_family_id: Arc::new(ArcSwapOption::empty()),
        }
    }

    /// Sets whether a WAL-backed write waits for durable publication before returning.
    pub fn with_await_durable(mut self, await_durable: bool) -> Self {
        self.await_durable = await_durable;
        self
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
            preload_scan_cursor_block: false,
            should_stop_at_block_boundary: false,
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
            .field("preload_scan_cursor_block", &self.preload_scan_cursor_block)
            .field(
                "should_stop_at_block_boundary",
                &self.should_stop_at_block_boundary,
            )
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
            await_durable: true,
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
            preload_scan_cursor_block: false,
            should_stop_at_block_boundary: false,
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

    /// Preload the next SST data block while a scan advances.
    ///
    /// This is intentionally opt-in: short cursor-driven scans such as priority-queue
    /// polling benefit from keeping the block after the cursor warm, while broad scans
    /// should avoid the extra synchronous cache lookup by default.
    pub fn with_preload_scan_cursor_block(mut self, enabled: bool) -> Self {
        self.preload_scan_cursor_block = enabled;
        self
    }

    /// Stop scanning after crossing the next physical block boundary.
    ///
    /// This is primarily useful for batch-oriented consumers such as priority
    /// queues that want each poll to stay close to the storage layout instead of
    /// reading arbitrarily far ahead. A stop is surfaced through the iterator's
    /// `stopped_at_block_boundary()` signal, and callers must clear that signal
    /// before resuming iteration.
    ///
    /// The exact boundary depends on the underlying reader: SST scans stop at
    /// data-block boundaries, Parquet scans stop at row-group boundaries, and
    /// wrapper iterators propagate that event upward.
    pub fn with_stop_at_block_boundary(mut self, enabled: bool) -> Self {
        self.should_stop_at_block_boundary = enabled;
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

    pub fn preload_scan_cursor_block(&self) -> bool {
        self.preload_scan_cursor_block
    }

    /// Whether boundary-aware stopping is enabled for scans created from these
    /// options.
    ///
    /// When this returns `true`, the scan iterator may pause after a physical
    /// storage boundary and report that pause through
    /// `KvIterator::stopped_at_block_boundary()`.
    pub fn should_stop_at_block_boundary(&self) -> bool {
        self.should_stop_at_block_boundary
    }

    pub fn set_max_rows(&mut self, max_rows: usize) {
        assert!(max_rows > 0, "max_rows must be > 0");
        self.max_rows = Some(max_rows);
    }

    pub fn clear_max_rows(&mut self) {
        self.max_rows = None;
    }

    pub fn set_preload_scan_cursor_block(&mut self, enabled: bool) {
        self.preload_scan_cursor_block = enabled;
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
    /// If None, uses max(l0_file_limit + 2, 32).
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
    /// How to react when a transient remote compaction failure occurs (compactor down, connect
    /// refused, timeout, I/O error). Defaults to falling back to local compaction. Permanent
    /// failures (protocol/schema/config) always give up without falling back.
    pub compaction_remote_failure_mode: RemoteCompactionFailureMode,
    /// Maximum number of concurrent requests the remote compaction server will process.
    pub compaction_server_max_concurrent: usize,
    /// Maximum number of queued requests before the server rejects new connections.
    pub compaction_server_max_queued: usize,
    /// Compaction execution mode. `Dedicated` disables in-process compaction and uses a separate
    /// compactor process that communicates via the shared volume.
    pub compaction_mode: CompactionMode,
    /// Controls durable runtime-manifest publication for external observers.
    pub runtime_manifest_mode: RuntimeManifestMode,
    /// Poll interval (milliseconds) for the writer's dedicated compaction result poller.
    pub compaction_dedicated_poll_interval_ms: u64,
    /// Minimum age (milliseconds) for orphan compaction job directories before they can be
    /// swept by the writer.
    pub compaction_orphan_min_age_ms: u64,
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
    /// Caching policy for decoded SST footer and index-partition descriptors.
    pub sst_read_metadata_cache_mode: SstReadMetadataCacheMode,
    /// Highest LSM level whose SST read metadata stays pinned for the file lifetime.
    /// `None` keeps all metadata in the normal block-cache path.
    #[serde(deserialize_with = "deserialize_optional_sst_level")]
    pub sst_pinned_metadata_max_level: Option<u8>,
    /// Whether pinned metadata also includes second-level index and filter partitions.
    pub sst_pinned_metadata_partitions_enabled: bool,
    /// Number of entries between restart points in SST data-block encoding.
    /// Values > 1 enable prefix compression; value 1 disables prefix compression.
    pub sst_data_block_restart_interval: usize,
    /// Output data-file format used by flush/compaction writers.
    pub data_file_type: DataFileType,
    /// Record CRC32 checksums for newly written SST data blocks.
    /// Parquet page checksums are not currently supported.
    pub block_checksum_enabled: bool,
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
    /// Whether VLog files newly created or copied into primary use the lowest-priority primary
    /// tier instead of the normal highest-priority tier.
    pub vlog_low_priority_primary_enabled: bool,
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
    /// Enable durable write-ahead log segments. Disabled by default.
    #[serde(default)]
    pub wal_enabled: bool,
    /// Maximum interval before the WAL group-commit buffer is frozen for publication.
    #[serde(default = "default_wal_flush_interval_ms")]
    pub wal_flush_interval_ms: u64,
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
    /// Usage ratio below which a higher-priority primary volume pulls referenced files back
    /// from lower-priority volumes. Maximum: 0.80. The effective value also remains below the
    /// offload trigger watermark to prevent immediate movement back to a lower tier.
    pub primary_volume_backfill_trigger_watermark: f64,
    /// Maximum number of background file transfers executed concurrently for this database.
    pub file_transfer_concurrency: usize,
    /// Offload policy for selecting candidate files on pressured primary volumes.
    pub primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind,
    /// Auto-expire snapshots after this many newer snapshots are completed.
    /// None disables auto-expiration.
    pub snapshot_retention: Option<usize>,
    /// Track snapshot lifecycle metadata only.
    /// When true, DB does not auto-expire snapshots by retention.
    pub snapshot_only_track: bool,
    /// Disable incremental manifest base linking.
    /// When true, each new snapshot is materialized without referencing a base snapshot.
    pub snapshot_disable_incremental_base_link: bool,
    /// Governance coordination mode for writable DB registration.
    pub governance_mode: GovernanceMode,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble"),
            memtable_capacity: Size::from_mib(64),
            memtable_buffer_count: 2,
            memtable_type: MemtableType::Adaptive,
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
            compaction_remote_failure_mode: RemoteCompactionFailureMode::FallbackLocal,
            compaction_server_max_concurrent: 4,
            compaction_server_max_queued: 64,
            compaction_mode: CompactionMode::Embedded,
            runtime_manifest_mode: RuntimeManifestMode::Auto,
            compaction_dedicated_poll_interval_ms: 1_000,
            compaction_orphan_min_age_ms: 300_000,
            block_cache_size: Size::from_mib(64),
            block_cache_hybrid_enabled: false,
            block_cache_hybrid_disk_size: None,
            reader: ReaderConfigEntry::default(),
            base_file_size: Size::from_mib(64),
            sst_bloom_filter_enabled: false,
            sst_bloom_bits_per_key: 10,
            sst_partitioned_index: false,
            sst_read_metadata_cache_mode: SstReadMetadataCacheMode::Eager,
            sst_pinned_metadata_max_level: Some(2),
            sst_pinned_metadata_partitions_enabled: false,
            sst_data_block_restart_interval: 16,
            data_file_type: DataFileType::SSTable,
            block_checksum_enabled: true,
            parquet_row_group_size_bytes: Size::from_kib(256),
            sst_compression_by_level: vec![
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::None,
                SstCompressionAlgorithm::Lz4,
            ],
            ttl_enabled: false,
            default_ttl_seconds: None,
            value_separation_threshold: None,
            vlog_low_priority_primary_enabled: false,
            time_provider: TimeProviderKind::default(),
            log_path: None,
            log_max_file_size: Size::from_mib(10),
            log_keep_files: 3,
            jni_direct_buffer_size: Size::from_kib(2),
            jni_direct_buffer_pool_size: 64,
            log_console: false,
            log_level: log::LevelFilter::Info,
            snapshot_on_flush: false,
            wal_enabled: false,
            wal_flush_interval_ms: default_wal_flush_interval_ms(),
            active_memtable_incremental_snapshot_ratio: 0.0,
            lsm_split_trigger_level: None,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.85,
            primary_volume_backfill_trigger_watermark: 0.40,
            file_transfer_concurrency: 4,
            primary_volume_offload_policy: PrimaryVolumeOffloadPolicyKind::Priority,
            snapshot_retention: None,
            snapshot_only_track: false,
            snapshot_disable_incremental_base_link: false,
            governance_mode: GovernanceMode::Filesystem,
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
        self.validate_wal()?;
        self.validate_snapshot_retention()?;
        let mut copied = self.clone();
        for volume in &mut copied.volumes {
            volume.base_dir = normalize_storage_path_to_url(&volume.base_dir)?;
        }
        Ok(copied)
    }
}

impl Config {
    /// Returns whether this DB should publish runtime manifests.
    pub fn runtime_manifests_enabled(&self) -> bool {
        match self.runtime_manifest_mode {
            RuntimeManifestMode::Auto => self.compaction_mode == CompactionMode::Dedicated,
            RuntimeManifestMode::Enabled => true,
            RuntimeManifestMode::Disabled => false,
        }
    }

    /// Returns whether a standalone dedicated compactor must use runtime manifests.
    ///
    /// The compactor process is dedicated by definition, so `Auto` resolves independently of
    /// the writer's `compaction_mode` value in the shared config file.
    pub(crate) fn runtime_manifests_enabled_for_dedicated_compactor(&self) -> bool {
        self.runtime_manifest_mode != RuntimeManifestMode::Disabled
    }

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

    pub(crate) fn jni_direct_buffer_size_bytes(&self) -> Result<usize> {
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
        let default_limit = self.l0_file_limit.saturating_add(2).max(32);
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
        if self.sst_data_block_restart_interval == 0 {
            return Err(Error::ConfigError(
                "sst_data_block_restart_interval must be greater than 0".to_string(),
            ));
        }
        if self.sst_data_block_restart_interval > u16::MAX as usize {
            return Err(Error::ConfigError(
                "sst_data_block_restart_interval must be less than or equal to 65535".to_string(),
            ));
        }
        for (idx, volume) in self.volumes.iter().enumerate() {
            if let Some(limit) = volume.size_limit {
                size_to_u64(&format!("volumes[{idx}].size_limit"), limit)
                    .map_err(Error::ConfigError)?;
            }
        }
        if self.compaction_mode == CompactionMode::Dedicated
            && self.compaction_remote_addr.is_some()
        {
            return Err(Error::ConfigError(
                "compaction_mode=dedicated cannot be used with compaction_remote_addr".to_string(),
            ));
        }
        self.validate_wal()?;
        self.validate_snapshot_retention()?;
        self.validate_dedicated_compaction()?;
        Ok(())
    }

    pub(crate) fn validate_snapshot_retention(&self) -> Result<()> {
        if self.snapshot_retention == Some(0) {
            return Err(Error::ConfigError(
                "snapshot_retention must be greater than 0 when configured".to_string(),
            ));
        }
        Ok(())
    }

    /// Validates the explicit WAL-volume contract.
    pub(crate) fn validate_wal(&self) -> Result<()> {
        if !self.wal_enabled {
            return Ok(());
        }
        let count = self
            .volumes
            .iter()
            .filter(|volume| volume.supports(VolumeUsageKind::Wal))
            .count();
        if count != 1 {
            return Err(Error::ConfigError(format!(
                "wal_enabled requires exactly one volume with wal usage, found {count}"
            )));
        }
        if self.wal_flush_interval_ms == 0 {
            return Err(Error::ConfigError(
                "wal_flush_interval_ms must be greater than 0 when WAL is enabled".to_string(),
            ));
        }
        Ok(())
    }

    /// Validates dedicated-compaction-specific config constraints.
    ///
    /// This is public so callers can revalidate CLI overrides, such as `--poll-interval`, that
    /// are applied after `Config::from_path`.
    pub fn validate_dedicated_compaction(&self) -> Result<()> {
        if self.compaction_mode != CompactionMode::Dedicated {
            return Ok(());
        }
        self.validate_dedicated_compactor()
    }

    /// Validates settings required by a standalone dedicated compactor.
    pub fn validate_dedicated_compactor(&self) -> Result<()> {
        // Reject zero poll interval - the poller would busy-loop.
        if self.compaction_dedicated_poll_interval_ms == 0 {
            return Err(Error::ConfigError(
                "compaction_dedicated_poll_interval_ms must be > 0 in dedicated mode".to_string(),
            ));
        }
        // Reject zero orphan min age - the sweep would delete active jobs.
        if self.compaction_orphan_min_age_ms == 0 {
            return Err(Error::ConfigError(
                "compaction_orphan_min_age_ms must be > 0 in dedicated mode".to_string(),
            ));
        }
        // The compactor's lease heartbeat refreshes at heartbeat_interval (derived from
        // orphan_min_age / 3, capped at poll_interval). If poll_interval is not significantly
        // shorter than orphan_min_age, a long compaction could have its outputs swept before
        // the heartbeat refreshes. Require at least a 3x margin.
        //
        // Note: the orphan sweep compares age in seconds (filesystem mtime granularity),
        // so we compare in seconds here too. poll_interval must be < orphan_min_age / 3.
        let poll_secs = self.compaction_dedicated_poll_interval_ms.div_ceil(1000);
        let min_age_secs = self.compaction_orphan_min_age_ms.div_ceil(1000);
        if poll_secs * 3 >= min_age_secs {
            return Err(Error::ConfigError(format!(
                "compaction_dedicated_poll_interval_ms ({}) must be significantly shorter \
                 than compaction_orphan_min_age_ms ({}) in dedicated mode; require \
                 poll_interval < orphan_min_age / 3 (in seconds: {} * 3 < {})",
                self.compaction_dedicated_poll_interval_ms,
                self.compaction_orphan_min_age_ms,
                poll_secs,
                min_age_secs,
            )));
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
#[path = "../tests/unit/config.rs"]
mod tests;
