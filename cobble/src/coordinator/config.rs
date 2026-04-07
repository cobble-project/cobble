use crate::VolumeDescriptor;

/// Config for opening the coordinator node.
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Storage volume descriptors for snapshot storage.
    pub volumes: Vec<VolumeDescriptor>,
    /// Auto-expire global snapshots after this many are retained.
    /// None disables auto-expiration.
    pub snapshot_retention: Option<usize>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble"),
            snapshot_retention: None,
        }
    }
}

impl CoordinatorConfig {
    pub fn from_config(config: &crate::Config) -> Self {
        Self {
            volumes: config.volumes.clone(),
            snapshot_retention: config.snapshot_retention,
        }
    }
}
