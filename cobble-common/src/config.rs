use crate::SstCompressionAlgorithm;
use crate::time::TimeProviderKind;
use log::warn;
use serde::{Deserialize, Serialize};

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
                "cache" => add_kind(&mut mask, VolumeUsageKind::Cache),
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

/// Memtable implementation selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MemtableType {
    #[default]
    Hash,
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
    // Block cache storage. e.g. foryer cache files.
    Cache = 5,
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct ReadProxyConfigEntry {
    pub pin_partition_in_memory_count: usize,
    pub block_cache_size: usize,
    pub reload_tolerance_seconds: u64,
}

impl Default for ReadProxyConfigEntry {
    fn default() -> Self {
        Self {
            pin_partition_in_memory_count: 1,
            block_cache_size: 512 * 1024 * 1024,
            reload_tolerance_seconds: DEFAULT_READ_PROXY_RELOAD_TOLERANCE_SECONDS,
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
    /// Read proxy configuration overrides.
    pub read_proxy: ReadProxyConfigEntry,
    /// Target base SST file size in bytes.
    pub base_file_size: usize,
    /// Enable bloom filter in SST files.
    pub sst_bloom_filter_enabled: bool,
    /// Bits per key for SST bloom filter when enabled.
    pub sst_bloom_bits_per_key: u32,
    /// Whether to enable two-level index and filter blocks in SST files.
    pub sst_partitioned_index: bool,
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
            read_proxy: ReadProxyConfigEntry::default(),
            base_file_size: 64 * 1024 * 1024,
            sst_bloom_filter_enabled: false,
            sst_bloom_bits_per_key: 10,
            sst_partitioned_index: false,
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
            snapshot_retention: None,
        }
    }
}

impl Config {
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
    use super::{Config, MemtableType, ReadProxyConfigEntry, VolumeDescriptor, VolumeUsageKind};
    use crate::SstCompressionAlgorithm;
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
            l0_file_limit: 5,
            write_stall_limit: Some(12),
            l1_base_bytes: 8 * 1024,
            level_size_multiplier: 7,
            max_level: 4,
            compaction_policy: super::CompactionPolicyKind::MinOverlap,
            block_cache_size: 256,
            read_proxy: ReadProxyConfigEntry {
                pin_partition_in_memory_count: 2,
                block_cache_size: 2048,
                reload_tolerance_seconds: 5,
            },
            base_file_size: 512,
            sst_bloom_filter_enabled: true,
            sst_bloom_bits_per_key: 11,
            sst_partitioned_index: true,
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
        assert_eq!(decoded.write_stall_limit, Some(12));
        assert_eq!(
            decoded.compaction_policy,
            super::CompactionPolicyKind::MinOverlap
        );
        assert!(decoded.sst_partitioned_index);
        assert_eq!(decoded.time_provider, crate::time::TimeProviderKind::Manual);
        assert_eq!(decoded.log_level, log::LevelFilter::Debug);
        assert_eq!(decoded.snapshot_retention, Some(3));
        assert_eq!(decoded.value_separation_threshold, 4096);
        assert_eq!(decoded.read_proxy.block_cache_size, 2048);
        assert_eq!(decoded.read_proxy.reload_tolerance_seconds, 5);

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
        assert_eq!(decoded_yaml.read_proxy.block_cache_size, 2048);

        let mut path_buf = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path_buf.push("tests/testdata/config.ini");

        let decoded_ini =
            Config::from_path(path_buf.as_path()).expect("Cannot deserialize ini config");
        assert_eq!(decoded_ini.read_proxy.reload_tolerance_seconds, 5);
        assert!(decoded_ini.volumes[0].supports(VolumeUsageKind::Meta));
    }

    #[test]
    fn test_volume_descriptor_kinds_list() {
        let json = r#"{
            "base_dir": "file:///tmp/cobble",
            "kinds": ["meta", "primary_data_priority_high", "cache"]
        }"#;

        let volume: VolumeDescriptor =
            serde_json::from_str(json).expect("Cannot deserialize volume descriptor");
        assert!(volume.supports(VolumeUsageKind::Meta));
        assert!(volume.supports(VolumeUsageKind::PrimaryDataPriorityHigh));
        assert!(volume.supports(VolumeUsageKind::Cache));
    }
}
