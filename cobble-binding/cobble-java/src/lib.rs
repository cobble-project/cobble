//! JNI bridge crate for Cobble Java binding.
//!
//! Address-based direct-buffer entry points also receive their Java owners. These
//! otherwise unused JNI object arguments keep the buffers reachable until native
//! return, including on Java 8, without re-resolving addresses or acquiring locks.
//!
//! This crate is internal to Java binding packaging and exports JNI symbols
//! consumed by `cobble-binding/cobble-java/java` classes.
//! End users should use the Java APIs under `io.cobble.*` instead of calling
//! these Rust modules directly.
//!
#![allow(dead_code)]

mod catalog;
mod coordinator;
mod db;
mod dedicated_compaction;
mod metrics;
mod process_file_systems;
mod read_only_db;
mod read_options;
mod reader;
mod scan;
mod schema;
mod single_db;
mod snapshot_tools;
mod structured;
mod structured_read_options;
mod structured_scan_options;
mod structured_single_db;
mod structured_write_batch;
mod structured_write_options;
mod table;
mod table_direct;
mod table_reader;
mod table_scan;
mod table_snapshot;
mod util;
mod write_options;
