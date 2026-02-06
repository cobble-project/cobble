use crate::time::TimeProviderKind;
use log::warn;

/// Compaction policy selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionPolicyKind {
    RoundRobin,
    MinOverlap,
}

/// Volume usage classification.
/// Used to indicate which volumes support which kinds of data.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VolumeUsageKind {
    // Metadata storage (manifests, snapshots, etc).
    Meta = 0,
    // Primary data storage (SST files, write-ahead log).
    PrimaryData = 1,
    // Block cache storage. e.g. foryer cache files.
    Cache = 5,
}

impl VolumeUsageKind {
    fn mask(self) -> u8 {
        1 << (self as u8)
    }
}

/// Descriptor for a storage volume.
#[derive(Clone, Debug)]
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
            vec![VolumeUsageKind::PrimaryData, VolumeUsageKind::Meta],
        )]
    }

    pub fn set_usage(&mut self, kind: VolumeUsageKind) {
        self.kinds |= kind.mask();
    }

    pub fn supports(&self, kind: VolumeUsageKind) -> bool {
        (self.kinds & kind.mask()) != 0
    }
}

/// Config for opening the database.
#[derive(Clone, Debug)]
pub struct Config {
    /// Storage volume descriptors for this database.
    pub volumes: Vec<VolumeDescriptor>,
    /// Memtable capacity in bytes.
    pub memtable_capacity: usize,
    /// Number of memtable buffers to keep in memory.
    pub memtable_buffer_count: usize,
    /// Number of columns in the value schema.
    pub num_columns: usize,
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
    /// Size of the block cache in bytes. If zero, cache is disabled.
    pub block_cache_size: usize,
    /// Target base SST file size in bytes.
    pub base_file_size: usize,
    /// Enable bloom filter in SST files.
    pub sst_bloom_filter_enabled: bool,
    /// Bits per key for SST bloom filter when enabled.
    pub sst_bloom_bits_per_key: u32,
    /// Whether to enable two-level index and filter blocks in SST files.
    pub sst_partitioned_index: bool,
    /// Whether TTL is enabled. If false, TTL metadata is ignored.
    pub ttl_enabled: bool,
    /// Default TTL duration (in seconds). None means no expiration by default.
    pub default_ttl_seconds: Option<u32>,
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
            num_columns: 1,
            l0_file_limit: 4,
            write_stall_limit: None,
            l1_base_bytes: 64 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 6,
            compaction_policy: CompactionPolicyKind::RoundRobin,
            block_cache_size: 64 * 1024 * 1024,
            base_file_size: 64 * 1024 * 1024,
            sst_bloom_filter_enabled: false,
            sst_bloom_bits_per_key: 10,
            sst_partitioned_index: false,
            ttl_enabled: false,
            default_ttl_seconds: None,
            time_provider: TimeProviderKind::default(),
            log_path: None,
            log_console: false,
            log_level: log::LevelFilter::Info,
            snapshot_on_flush: false,
            snapshot_retention: None,
        }
    }
}

impl Config {
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
}
