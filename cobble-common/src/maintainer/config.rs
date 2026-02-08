use crate::VolumeDescriptor;

/// Config for opening the maintainer node.
#[derive(Clone, Debug)]
pub struct MaintainerConfig {
    /// Storage volume descriptors for snapshot storage.
    pub volumes: Vec<VolumeDescriptor>,
}

impl Default for MaintainerConfig {
    fn default() -> Self {
        Self {
            volumes: VolumeDescriptor::single_volume("file:///tmp/cobble".to_string()),
        }
    }
}

impl MaintainerConfig {
    pub fn from_config(config: &crate::Config) -> Self {
        Self {
            volumes: config.volumes.clone(),
        }
    }
}
