//! Cluster integration crate for Cobble.
//!
//! This crate currently exposes a standalone TCP coordinator/shard mode:
//! - `StandaloneCoordinator`: materializes global snapshots from shard snapshots.
//! - `StandaloneShardNode`: wraps a local `Db` and keeps a long-lived connection
//!   to the coordinator for registration and checkpoint serving.
//!
//! # Basic usage
//!
//! ```rust,ignore
//! use cobble::{Config, VolumeDescriptor};
//! use cobble_cluster::{StandaloneCoordinator, StandaloneShardNode};
//!
//! let mut config = Config::default();
//! config.total_buckets = 1;
//! config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-cluster".to_string());
//!
//! let mut coordinator = StandaloneCoordinator::open(config.clone())?;
//! coordinator.serve("127.0.0.1:9900")?;
//!
//! let mut shard = StandaloneShardNode::open(config, vec![0u16..=0u16], "127.0.0.1:9900")?;
//! shard.serve()?;
//! # Ok::<(), cobble::Error>(())
//! ```
//!
pub mod standalone;

pub use standalone::{StandaloneCoordinator, StandaloneShardNode};
