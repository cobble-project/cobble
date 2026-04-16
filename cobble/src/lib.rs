//! Cobble core Rust library.
//!
//! Cobble is an LSM-based key-value storage engine with snapshots, distributed
//! coordination primitives, read proxies, and distributed scan planning.
//!
//! This crate supports step-0 config guidance + five common usage flows:
//!
//! # 0) Config and volume layout
//!
//! Define `Config` first, especially `volumes`:
//!
//! - `PrimaryDataPriorityHigh/Medium/Low`: primary data files (SST/parquet/VLOG)
//! - `Meta`: metadata files (manifests, pointers, schema files)
//! - `Snapshot`: snapshot materialization target
//! - `Cache`: disk tier for hybrid block cache
//! - `Readonly`: read-only source volume for historical loading
//!
//! Minimum practical configuration:
//! `VolumeDescriptor::single_volume("file:///path")`.
//!
//! IMPORTANT: for any restore/resume flow, the runtime must be able to access all
//! files referenced by the target snapshot manifests. Missing/inaccessible files
//! will cause restore to fail.
//!
//! # 1) Single-machine embedded (`SingleDb`)
//!
//! Use `SingleDb` when you want one-process/one-node write+read with automatic
//! single-node global snapshot materialization.
//!
//! ```rust,ignore
//! use cobble::{Config, SingleDb, VolumeDescriptor};
//!
//! let mut config = Config::default();
//! config.num_columns = 2; // initial width of the default column family
//! config.total_buckets = 1;
//! config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-single");
//!
//! let db = SingleDb::open(config.clone())?;
//! db.put(0, b"k1", 0, b"v1")?;
//! let global_snapshot_id = db.snapshot()?;
//!
//! // Recovery/read-write path from a global snapshot:
//! let resumed = SingleDb::resume(config, global_snapshot_id)?;
//! let row = resumed.get(0, b"k1")?;
//! # Ok::<(), cobble::Error>(())
//! ```
//!
//! # 2) Distributed write path (`N x Db` + `1 x DbCoordinator`)
//!
//! Each shard runs one `Db` over its bucket ranges. A shared `DbCoordinator`
//! materializes global snapshots from all shard snapshots.
//!
//! - Write/read normally on each shard `Db`.
//! - Trigger `Db::snapshot` (or `snapshot_with_callback`) on each shard.
//! - Build `ShardSnapshotInput` list and call:
//!   - `DbCoordinator::take_global_snapshot(...)`
//!   - `DbCoordinator::materialize_global_snapshot(...)`
//!
//! Restore sequence for historical recovery:
//! - restore coordinator view first (load global snapshot / current pointer),
//! - then restore each shard with `Db::open_from_snapshot(config, shard_snapshot_id, db_id)`,
//! - continue read/write on restored shard DBs.
//!
//! Remote compaction (optional):
//! - start `RemoteCompactionServer::new(server_config)?.serve("host:port")`,
//! - set writer `Config.compaction_remote_addr = Some("host:port".to_string())`,
//! - open `Db` as normal; compaction tasks are sent to remote worker.
//!
//! # 3) Real-time reading while writing (`Reader`)
//!
//! Use `Reader` to serve reads while writers keep writing and periodically
//! materializing new global snapshots.
//!
//! - `Reader::open_current(reader_config)` follows current pointer.
//! - call `reader.refresh()` to pick newer global snapshot.
//! - use `reader.get(...)` / `reader.scan(...)` for query traffic.
//!
//! # 4) Distributed scan on one snapshot (`ScanPlan` / `ScanSplit`)
//!
//! Given one `GlobalSnapshotManifest`:
//! - build `ScanPlan::new(manifest)`,
//! - optionally set `with_start`/`with_end`,
//! - distribute `ScanSplit`s to workers,
//! - each worker calls `split.create_scanner(config, &scan_options)`.
//!
//! # 5) Structured wrappers
//!
//! For typed column wrappers, see `cobble-data-structure`:
//! `StructuredDb`, `StructuredSingleDb`, `StructuredReader`,
//! `StructuredReadOnlyDb`, `StructuredScanPlan`, and `StructuredScanSplit`.
//!
#![crate_type = "lib"]
#![allow(dead_code)]

mod block_cache;
mod cache;
mod compaction;
mod config;
mod coordinator;
mod data_file;
mod db;
mod db_builder;
mod db_iter;
mod db_state;
mod db_status;
mod error;
mod file;
mod format;
mod governance;
mod iterator;
mod lru;
mod lsm;
mod memtable;
mod merge_operator;
mod metrics_manager;
mod metrics_registry;
mod parquet;
pub mod paths;
mod read_only_db;
mod reader;
mod scan;
mod schema;
mod single_db;
mod snapshot;
mod sst;
mod time;
mod ttl;
mod r#type;
mod util;
mod vlog;
mod write_batch;
mod writer_options;

pub use compaction::RemoteCompactionServer;
pub use config::{
    CompactionPolicyKind, Config, MemtableType, PrimaryVolumeOffloadPolicyKind, ReadOptions,
    ScanOptions, VolumeDescriptor, VolumeUsageKind, WriteOptions,
};
pub use coordinator::{
    CoordinatorConfig, DbCoordinator, GlobalSnapshotManifest, ShardSnapshotInput, ShardSnapshotRef,
};
pub use db::Db;
pub use db_builder::DbBuilder;
pub use db_iter::DbIterator;
pub use error::{Error, Result};
pub use governance::{DbGovernance, FileSystemDbGovernance};
pub use merge_operator::{
    BytesMergeOperator, MergeOperator, MergeOperatorResolver, U32CounterMergeOperator,
    U64CounterMergeOperator, merge_operator_by_id,
};
pub use metrics_manager::MetricsManager;
pub use metrics_registry::{HistogramSnapshot, MetricSample, MetricValue};
pub use read_only_db::ReadOnlyDb;
pub use reader::{GlobalSnapshotSummary, Reader, ReaderConfig};
pub use scan::{ScanPlan, ScanSplit, ScanSplitScanner};
pub use schema::{Schema, SchemaBuilder};
pub use single_db::SingleDb;
pub use sst::SstCompressionAlgorithm;
pub use time::{ManualTimeProvider, SystemTimeProvider, TimeProvider, TimeProviderKind};
pub use r#type::ValueType;
pub use write_batch::WriteBatch;

#[doc(hidden)]
pub mod test_utils {
    pub use crate::file::metadata_io::{
        encode_metadata_payload_for_test, read_metadata_payload_from_path_for_test,
    };
}

pub fn build_commit_short_id() -> &'static str {
    util::build_commit_short_id()
}

pub fn build_version_string() -> &'static str {
    util::build_version_string()
}
