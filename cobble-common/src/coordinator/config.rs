use crate::VolumeDescriptor;

/// Config for opening the coordinator node.
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Storage volume descriptors for snapshot storage.
    pub volumes: Vec<VolumeDescriptor>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble".to_string()),
        }
    }
}

impl CoordinatorConfig {
    pub fn from_config(config: &crate::Config) -> Self {
        Self {
            volumes: config.volumes.clone(),
        }
    }
}
