/// Config for opening the maintainer node.
#[derive(Clone, Debug)]
pub struct MaintainerConfig {
    /// Filesystem path for shared snapshot storage.
    pub path: String,
}

impl Default for MaintainerConfig {
    fn default() -> Self {
        Self {
            path: "file:///tmp/".into(),
        }
    }
}
