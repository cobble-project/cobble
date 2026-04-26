//! Structured wrapper crate on top of `cobble`.
//!
//! This crate mirrors Cobble's main usage flows, but with typed structured
//! columns (`Bytes` / `List`) and typed decode/encode wrappers.
//!
//! # 1) Single-machine embedded (`StructuredSingleDb`)
//!
//! ```rust,ignore
//! use cobble::{Config, VolumeDescriptor};
//! use cobble_data_structure::{ListConfig, ListRetainMode, StructuredSingleDb};
//!
//! let mut config = Config::default();
//! config.num_columns = 1;
//! config.total_buckets = 1;
//! config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-ssingle");
//!
//! let mut db = StructuredSingleDb::open(config)?;
//! db.update_schema().add_list_column(
//!     None,
//!     1,
//!     ListConfig {
//!         max_elements: Some(100),
//!         retain_mode: ListRetainMode::Last,
//!         preserve_element_ttl: false,
//!     },
//! ).commit()?;
//! db.put(0, b"k1", 0, b"v1".to_vec())?;
//! let snapshot_id = db.snapshot()?;
//! let snapshots = db.list_snapshots()?;
//! # let _ = (snapshot_id, snapshots);
//! # Ok::<(), cobble::Error>(())
//! ```
//!
//! # 2) Distributed write path wrappers
//!
//! Use `StructuredDb` on shard writers and keep coordinator/global snapshot flow
//! identical to `cobble::Db` + `cobble::DbCoordinator`.
//!
//! # 3) Snapshot-following reading wrappers (`StructuredReader`)
//!
//! `StructuredReader` follows global snapshots like `cobble::Reader`, but returns
//! typed structured rows.
//!
//! # 4) Distributed scan wrappers
//!
//! Use `StructuredScanPlan` / `StructuredScanSplit` / `StructuredScanSplitScanner`
//! for snapshot-based distributed scan with structured row decoding.
//!
//! # 5) Fixed snapshot read wrappers
//!
//! Use `StructuredReadOnlyDb` for pinned-snapshot reads with structured decoding.
//!
#![crate_type = "lib"]

mod list;
mod structured_db;
mod structured_read_only_db;
mod structured_reader;
mod structured_remote_compaction_server;
mod structured_scan;
mod structured_single_db;

pub use list::{ListConfig, ListRetainMode};
pub use structured_db::{
    DataStructureDb, StructuredColumnFamilySchema, StructuredColumnType, StructuredColumnValue,
    StructuredDb, StructuredDbIterator, StructuredReadOptions, StructuredScanOptions,
    StructuredSchema, StructuredSchemaBuilder, StructuredWriteBatch, StructuredWriteOptions,
    structured_merge_operator_resolver, structured_resolvable_operator_ids,
};
pub use structured_read_only_db::StructuredReadOnlyDb;
pub use structured_reader::StructuredReader;
pub use structured_remote_compaction_server::StructuredRemoteCompactionServer;
pub use structured_scan::{StructuredScanPlan, StructuredScanSplit, StructuredScanSplitScanner};
pub use structured_single_db::StructuredSingleDb;
