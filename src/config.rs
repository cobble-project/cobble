/// Compaction policy selection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompactionPolicyKind {
    RoundRobin,
    MinOverlap,
}

/// Config for opening the database.
#[derive(Clone, Debug)]
pub struct Config {
    /// Filesystem path for the database.
    pub path: String,
    /// Memtable capacity in bytes.
    pub memtable_capacity: usize,
    /// Number of memtable buffers to keep in memory.
    pub memtable_buffer_count: usize,
    /// Number of columns in the value schema.
    pub num_columns: usize,
    /// Maximum number of L0 files before triggering compaction.
    pub l0_file_limit: usize,
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
    /// Optional log file path. If None, logs go to console only. Must be a local path.
    pub log_path: Option<String>,
    /// Whether to enable console logging.
    pub log_console: bool,
    /// Log level filter (trace, debug, info, warn, error, off).
    pub log_level: log::LevelFilter,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: "file:///tmp/".into(),
            memtable_capacity: 64 * 1024 * 1024,
            memtable_buffer_count: 2,
            num_columns: 1,
            l0_file_limit: 4,
            l1_base_bytes: 64 * 1024 * 1024,
            level_size_multiplier: 10,
            max_level: 6,
            compaction_policy: CompactionPolicyKind::RoundRobin,
            block_cache_size: 64 * 1024 * 1024,
            base_file_size: 64 * 1024 * 1024,
            log_path: None,
            log_console: false,
            log_level: log::LevelFilter::Info,
        }
    }
}
