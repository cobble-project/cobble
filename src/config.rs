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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: "file:///tmp/".into(),
            memtable_capacity: 64 * 1024 * 1024,
            memtable_buffer_count: 2,
            num_columns: 1,
        }
    }
}
