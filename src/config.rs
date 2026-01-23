/// Config for opening the database.
#[derive(Clone, Debug)]
pub struct Config {
    /// Filesystem path for the database.
    pub path: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: "file:///tmp/".into(),
        }
    }
}
