//! Private Rust surface for the C++ raw-KV binding.
//!
//! The public C++ API deliberately lives behind a PImpl wrapper. This bridge
//! exposes only opaque Rust owners, shared option/result values, and borrowed
//! byte slices to that wrapper.

mod coordinator;
mod database;
mod distributed_scan;
mod encoding;
mod error;
mod lifecycle;
mod metrics;
mod multi_get;
mod options;
mod read_only_db;
mod reader;
mod scan;
mod schema;
mod sharded_db;
mod snapshot;
mod write_batch;

use coordinator::*;
use database::{
    NativeDatabase, NativeRow, native_database_close, native_database_delete,
    native_database_expire_snapshot, native_database_get, native_database_get_column_into,
    native_database_list_snapshots, native_database_merge, native_database_open,
    native_database_open_file, native_database_put, native_database_resume,
    native_database_resume_file, native_database_retain_snapshot, native_database_set_time,
    native_database_snapshot, native_database_snapshot_manifest_json, native_database_version,
    native_database_write_batch,
};
use distributed_scan::*;
use error::BridgeResult;
use lifecycle::{
    native_database_load_readonly_files_to_primary, native_database_now_seconds,
    native_database_switch_memtable_type, native_sharded_database_load_readonly_files_to_primary,
    native_sharded_database_now_seconds, native_sharded_database_set_time,
    native_sharded_database_switch_memtable_type,
};
use metrics::{native_database_metrics, native_sharded_database_metrics};
use multi_get::{
    NativeMultiGetResult, native_database_multi_get, native_multi_get_column,
    native_multi_get_column_count, native_multi_get_found, native_multi_get_has_column,
    native_multi_get_row_count, native_sharded_database_multi_get,
};
use read_only_db::*;
use reader::*;
use scan::{
    NativeBatch, NativeScanCursor, native_batch_bucket, native_batch_column,
    native_batch_column_count, native_batch_end, native_batch_has_column, native_batch_key,
    native_batch_row_count, native_batch_stopped_at_block_boundary, native_database_scan,
    native_row_column, native_row_column_count, native_row_found, native_row_has_column,
    native_scan_cursor_next_batch_into, native_scan_cursor_next_owned,
    native_scan_cursor_resume_after_block_boundary, native_sharded_database_scan,
};
use schema::{
    NativeSchemaBuilder, native_database_current_schema, native_database_update_schema,
    native_schema_builder_add_column, native_schema_builder_commit,
    native_schema_builder_delete_column, native_schema_builder_set_column_family_ttl,
    native_schema_builder_set_column_operator, native_sharded_database_current_schema,
    native_sharded_database_update_schema,
};
use sharded_db::*;
use snapshot::{
    NativePendingShardSnapshot, NativePendingSnapshot, native_database_get_snapshot_typed,
    native_database_list_snapshots_typed, native_database_start_snapshot,
    native_database_take_snapshot, native_pending_shard_snapshot_id,
    native_pending_shard_snapshot_wait, native_pending_snapshot_id, native_pending_snapshot_wait,
    native_sharded_database_cancel_snapshot, native_sharded_database_expire_snapshot,
    native_sharded_database_get_shard_snapshot, native_sharded_database_retain_snapshot,
    native_sharded_database_snapshot, native_sharded_database_start_snapshot,
    native_sharded_database_take_snapshot,
};
use write_batch::{
    NativeWriteBatch, native_write_batch_delete, native_write_batch_len, native_write_batch_merge,
    native_write_batch_new, native_write_batch_put,
};

#[allow(clippy::too_many_arguments)]
#[cxx::bridge(namespace = "cobble::ffi")]
mod ffi {
    struct NativeReadOptions {
        column_family: String,
        columns: Vec<u64>,
    }
    struct NativeWriteOptions {
        has_ttl_seconds: bool,
        ttl_seconds: u32,
        column_family: String,
        await_durable: bool,
    }
    struct NativeScanOptions {
        column_family: String,
        columns: Vec<u64>,
        read_ahead_bytes: u64,
        has_max_rows: bool,
        max_rows: u64,
        preload_scan_cursor_block: bool,
        stop_at_block_boundary: bool,
    }
    struct NativeBufferResult {
        status: u8,
        bytes_written: u64,
        bytes_required: u64,
        row_count: u64,
    }
    struct NativeRange {
        first: u16,
        last: u16,
    }
    struct NativeFamily {
        name: String,
        id: u8,
    }
    struct NativeShardSnapshot {
        ranges: Vec<NativeRange>,
        families: Vec<NativeFamily>,
        db_id: String,
        snapshot_id: u64,
        manifest_path: String,
        timestamp_seconds: u32,
        data_size_bytes: u64,
        incremental_data_size_bytes: u64,
    }
    struct NativeSnapshot {
        version: u32,
        id: u64,
        total_buckets: u32,
        families: Vec<NativeFamily>,
        shards: Vec<NativeShardSnapshot>,
        watermark_seconds: u32,
    }
    struct NativeScanSplit {
        shard: NativeShardSnapshot,
        has_start: bool,
        start: Vec<u8>,
        has_end: bool,
        end: Vec<u8>,
        has_start_after: bool,
        start_after_bucket: u16,
        start_after_key: Vec<u8>,
        has_end_at: bool,
        end_at_bucket: u16,
        end_at_key: Vec<u8>,
    }
    struct NativeMetric {
        name: String,
        labels: Vec<NativeMetricLabel>,
        kind: u8,
        counter: u64,
        gauge: f64,
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    }
    struct NativeMetricLabel {
        key: String,
        value: String,
    }
    struct NativeMergeOperator {
        id: String,
        has_metadata: bool,
        metadata_json: String,
    }
    struct NativeSchemaFamily {
        name: String,
        id: u8,
        column_count: u64,
        value_has_ttl: bool,
        merge_operators: Vec<NativeMergeOperator>,
    }
    struct NativeSchema {
        version: u64,
        column_families: Vec<NativeSchemaFamily>,
    }

    extern "Rust" {
        type NativeDatabase;
        type NativeShardedDatabase;
        type NativeReadOnlyDatabase;
        type NativeReader;
        type NativeCoordinator;
        type NativeWriteBatch;
        type NativeRow;
        type NativeScanCursor;
        type NativeBatch;
        type NativeMultiGetResult;
        type NativePendingSnapshot;
        type NativePendingShardSnapshot;
        type NativeSchemaBuilder;
        fn native_database_open(config_json: &str) -> Result<Box<NativeDatabase>>;
        fn native_database_open_file(config_path: &str) -> Result<Box<NativeDatabase>>;
        fn native_database_resume(
            config_json: &str,
            snapshot_id: u64,
            recovery_mode: u8,
        ) -> Result<Box<NativeDatabase>>;
        fn native_database_resume_file(
            config_path: &str,
            snapshot_id: u64,
            recovery_mode: u8,
        ) -> Result<Box<NativeDatabase>>;
        fn native_database_close(db: &NativeDatabase) -> Result<()>;
        fn native_database_version() -> String;
        fn native_database_put(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_delete(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_merge(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_database_write_batch(
            db: &NativeDatabase,
            batch: Box<NativeWriteBatch>,
            await_durable: bool,
        ) -> Result<()>;
        fn native_database_get(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            options: &NativeReadOptions,
        ) -> Result<Box<NativeRow>>;
        fn native_database_get_column_into(
            db: &NativeDatabase,
            bucket: u16,
            key: &[u8],
            output: &mut [u8],
            options: &NativeReadOptions,
        ) -> Result<NativeBufferResult>;
        fn native_database_scan(
            db: &NativeDatabase,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_scan_cursor_next_owned(
            cursor: &mut NativeScanCursor,
            max_rows: u64,
        ) -> Result<Box<NativeBatch>>;
        fn native_scan_cursor_next_batch_into(
            cursor: &mut NativeScanCursor,
            max_rows: u64,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_scan_cursor_resume_after_block_boundary(
            cursor: &mut NativeScanCursor,
        ) -> Result<()>;
        fn native_write_batch_new() -> Box<NativeWriteBatch>;
        fn native_write_batch_len(batch: &NativeWriteBatch) -> u64;
        fn native_write_batch_put(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        );
        fn native_write_batch_delete(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        );
        fn native_write_batch_merge(
            batch: &mut NativeWriteBatch,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        );
        fn native_row_found(row: &NativeRow) -> bool;
        fn native_row_column_count(row: &NativeRow) -> u64;
        fn native_row_has_column(row: &NativeRow, column: u64) -> bool;
        fn native_row_column(row: &NativeRow, column: u64) -> Result<&[u8]>;
        fn native_batch_row_count(batch: &NativeBatch) -> u64;
        fn native_batch_end(batch: &NativeBatch) -> bool;
        fn native_batch_stopped_at_block_boundary(batch: &NativeBatch) -> bool;
        fn native_batch_bucket(batch: &NativeBatch, row: u64) -> Result<u16>;
        fn native_batch_key(batch: &NativeBatch, row: u64) -> Result<&[u8]>;
        fn native_batch_column_count(batch: &NativeBatch, row: u64) -> Result<u64>;
        fn native_batch_has_column(batch: &NativeBatch, row: u64, column: u64) -> bool;
        fn native_batch_column(batch: &NativeBatch, row: u64, column: u64) -> Result<&[u8]>;
        fn native_database_snapshot(db: &NativeDatabase) -> Result<u64>;
        fn native_database_retain_snapshot(db: &NativeDatabase, snapshot_id: u64) -> Result<bool>;
        fn native_database_expire_snapshot(db: &NativeDatabase, snapshot_id: u64) -> Result<bool>;
        fn native_database_list_snapshots(db: &NativeDatabase) -> Result<Vec<u64>>;
        fn native_database_snapshot_manifest_json(
            db: &NativeDatabase,
            snapshot_id: u64,
        ) -> Result<String>;
        fn native_database_set_time(db: &NativeDatabase, unix_seconds: u32);
        fn native_database_multi_get(
            db: &NativeDatabase,
            descriptors: usize,
            count: u64,
            options: &NativeReadOptions,
        ) -> Result<Box<NativeMultiGetResult>>;
        fn native_multi_get_row_count(rows: &NativeMultiGetResult) -> u64;
        fn native_multi_get_found(rows: &NativeMultiGetResult, row: u64) -> bool;
        fn native_multi_get_column_count(rows: &NativeMultiGetResult, row: u64) -> Result<u64>;
        fn native_multi_get_has_column(rows: &NativeMultiGetResult, row: u64, column: u64) -> bool;
        fn native_multi_get_column(
            rows: &NativeMultiGetResult,
            row: u64,
            column: u64,
        ) -> Result<&[u8]>;
        fn native_database_start_snapshot(
            db: &NativeDatabase,
        ) -> Result<Box<NativePendingSnapshot>>;
        fn native_pending_snapshot_id(pending: &NativePendingSnapshot) -> u64;
        fn native_pending_snapshot_wait(
            pending: &mut NativePendingSnapshot,
        ) -> Result<NativeSnapshot>;
        fn native_database_take_snapshot(db: &NativeDatabase) -> Result<NativeSnapshot>;
        fn native_database_get_snapshot_typed(
            db: &NativeDatabase,
            snapshot_id: u64,
        ) -> Result<NativeSnapshot>;
        fn native_database_list_snapshots_typed(db: &NativeDatabase)
        -> Result<Vec<NativeSnapshot>>;
        fn native_database_now_seconds(db: &NativeDatabase) -> u32;
        fn native_database_switch_memtable_type(
            db: &NativeDatabase,
            kind: u8,
            flush_current: bool,
        ) -> Result<()>;
        fn native_database_load_readonly_files_to_primary(db: &NativeDatabase) -> Result<u64>;
        fn native_database_current_schema(db: &NativeDatabase) -> Result<NativeSchema>;
        fn native_database_update_schema(db: &NativeDatabase) -> Box<NativeSchemaBuilder>;
        fn native_schema_builder_set_column_operator(
            builder: &mut NativeSchemaBuilder,
            has_family: bool,
            family: &str,
            column: u64,
            operator_id: &str,
            has_metadata: bool,
            metadata_json: &str,
        ) -> Result<()>;
        fn native_schema_builder_add_column(
            builder: &mut NativeSchemaBuilder,
            column: u64,
            has_operator: bool,
            operator_id: &str,
            has_metadata: bool,
            metadata_json: &str,
            has_default_value: bool,
            default_value: &[u8],
            has_family: bool,
            family: &str,
        ) -> Result<()>;
        fn native_schema_builder_delete_column(
            builder: &mut NativeSchemaBuilder,
            has_family: bool,
            family: &str,
            column: u64,
        ) -> Result<()>;
        fn native_schema_builder_set_column_family_ttl(
            builder: &mut NativeSchemaBuilder,
            has_family: bool,
            family: &str,
            value_has_ttl: bool,
        ) -> Result<()>;
        fn native_schema_builder_commit(builder: Box<NativeSchemaBuilder>) -> Result<NativeSchema>;
        fn native_database_metrics(db: &NativeDatabase) -> Vec<NativeMetric>;

        fn native_sharded_database_open(config_json: &str) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_open_ranges(
            config_json: &str,
            ranges: Vec<NativeRange>,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_open_file(
            config_path: &str,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_open_file_ranges(
            config_path: &str,
            ranges: Vec<NativeRange>,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_open_from_snapshot(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_open_from_snapshot_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_restore_new(
            config_json: &str,
            snapshot_id: u64,
            source_db_id: &str,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_restore_new_file(
            config_path: &str,
            snapshot_id: u64,
            source_db_id: &str,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_restore_new_from_manifest(
            config_json: &str,
            manifest_path: &str,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_restore_new_from_manifest_file(
            config_path: &str,
            manifest_path: &str,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_resume(
            config_json: &str,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_resume_file(
            config_path: &str,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_resume_from_snapshot(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_resume_from_snapshot_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeShardedDatabase>>;
        fn native_sharded_database_id(db: &NativeShardedDatabase) -> &str;
        fn native_sharded_database_close(db: &NativeShardedDatabase) -> Result<()>;
        fn native_sharded_database_put(
            db: &NativeShardedDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_sharded_database_delete(
            db: &NativeShardedDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_sharded_database_merge(
            db: &NativeShardedDatabase,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_sharded_database_write_batch(
            db: &NativeShardedDatabase,
            batch: Box<NativeWriteBatch>,
            await_durable: bool,
        ) -> Result<()>;
        fn native_sharded_database_get(
            db: &NativeShardedDatabase,
            bucket: u16,
            key: &[u8],
            options: &NativeReadOptions,
        ) -> Result<Box<NativeRow>>;
        fn native_sharded_database_get_column_into(
            db: &NativeShardedDatabase,
            bucket: u16,
            key: &[u8],
            output: &mut [u8],
            options: &NativeReadOptions,
        ) -> Result<NativeBufferResult>;
        fn native_sharded_database_multi_get(
            db: &NativeShardedDatabase,
            descriptors: usize,
            count: u64,
            options: &NativeReadOptions,
        ) -> Result<Box<NativeMultiGetResult>>;
        fn native_sharded_database_scan(
            db: &NativeShardedDatabase,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_sharded_database_current_schema(
            db: &NativeShardedDatabase,
        ) -> Result<NativeSchema>;
        fn native_sharded_database_update_schema(
            db: &NativeShardedDatabase,
        ) -> Box<NativeSchemaBuilder>;
        fn native_sharded_database_metrics(db: &NativeShardedDatabase) -> Vec<NativeMetric>;
        fn native_sharded_database_set_time(db: &NativeShardedDatabase, unix_seconds: u32);
        fn native_sharded_database_now_seconds(db: &NativeShardedDatabase) -> u32;
        fn native_sharded_database_switch_memtable_type(
            db: &NativeShardedDatabase,
            kind: u8,
            flush_current: bool,
        ) -> Result<()>;
        fn native_sharded_database_load_readonly_files_to_primary(
            db: &NativeShardedDatabase,
        ) -> Result<u64>;
        fn native_sharded_database_snapshot(db: &NativeShardedDatabase) -> Result<u64>;
        fn native_sharded_database_start_snapshot(
            db: &NativeShardedDatabase,
        ) -> Result<Box<NativePendingShardSnapshot>>;
        fn native_pending_shard_snapshot_id(pending: &NativePendingShardSnapshot) -> u64;
        fn native_pending_shard_snapshot_wait(
            pending: &mut NativePendingShardSnapshot,
        ) -> Result<NativeShardSnapshot>;
        fn native_sharded_database_take_snapshot(
            db: &NativeShardedDatabase,
        ) -> Result<NativeShardSnapshot>;
        fn native_sharded_database_cancel_snapshot(
            db: &NativeShardedDatabase,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_sharded_database_get_shard_snapshot(
            db: &NativeShardedDatabase,
            snapshot_id: u64,
        ) -> Result<NativeShardSnapshot>;
        fn native_sharded_database_retain_snapshot(
            db: &NativeShardedDatabase,
            snapshot_id: u64,
        ) -> bool;
        fn native_sharded_database_expire_snapshot(
            db: &NativeShardedDatabase,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_sharded_database_switch_to_snapshot(
            db: &mut NativeShardedDatabase,
            snapshot_id: u64,
        ) -> Result<()>;
        fn native_sharded_database_expand_bucket(
            db: &NativeShardedDatabase,
            source_db_id: &str,
            has_snapshot_id: bool,
            snapshot_id: u64,
            has_ranges: bool,
            ranges: Vec<NativeRange>,
            storage_mode: u8,
        ) -> Result<u64>;
        fn native_sharded_database_wait_for_expand_adoption(
            db: &NativeShardedDatabase,
            timeout_millis: i64,
        ) -> Result<()>;
        fn native_sharded_database_shrink_bucket(
            db: &NativeShardedDatabase,
            ranges: Vec<NativeRange>,
        ) -> Result<u64>;

        fn native_read_only_database_open(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
        ) -> Result<Box<NativeReadOnlyDatabase>>;
        fn native_read_only_database_open_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
        ) -> Result<Box<NativeReadOnlyDatabase>>;
        fn native_read_only_database_id(db: &NativeReadOnlyDatabase) -> &str;
        fn native_read_only_database_get(
            db: &NativeReadOnlyDatabase,
            bucket: u16,
            key: &[u8],
            options: &NativeReadOptions,
        ) -> Result<Box<NativeRow>>;
        fn native_read_only_database_get_column_into(
            db: &NativeReadOnlyDatabase,
            bucket: u16,
            key: &[u8],
            output: &mut [u8],
            options: &NativeReadOptions,
        ) -> Result<NativeBufferResult>;
        fn native_read_only_database_multi_get(
            db: &NativeReadOnlyDatabase,
            descriptors: usize,
            count: u64,
            options: &NativeReadOptions,
        ) -> Result<Box<NativeMultiGetResult>>;
        fn native_read_only_database_scan(
            db: &NativeReadOnlyDatabase,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_read_only_database_current_schema(
            db: &NativeReadOnlyDatabase,
        ) -> Result<NativeSchema>;
        fn native_read_only_database_metrics(db: &NativeReadOnlyDatabase) -> Vec<NativeMetric>;

        fn native_reader_open_current(config_json: &str) -> Result<Box<NativeReader>>;
        fn native_reader_open_current_file(config_path: &str) -> Result<Box<NativeReader>>;
        fn native_reader_open(config_json: &str, snapshot_id: u64) -> Result<Box<NativeReader>>;
        fn native_reader_open_file(
            config_path: &str,
            snapshot_id: u64,
        ) -> Result<Box<NativeReader>>;
        fn native_reader_refresh(reader: &mut NativeReader) -> Result<()>;
        fn native_reader_get(
            reader: &mut NativeReader,
            bucket: u16,
            key: &[u8],
            options: &NativeReadOptions,
        ) -> Result<Box<NativeRow>>;
        fn native_reader_get_column_into(
            reader: &mut NativeReader,
            bucket: u16,
            key: &[u8],
            output: &mut [u8],
            options: &NativeReadOptions,
        ) -> Result<NativeBufferResult>;
        fn native_reader_multi_get(
            reader: &mut NativeReader,
            descriptors: usize,
            count: u64,
            options: &NativeReadOptions,
        ) -> Result<Box<NativeMultiGetResult>>;
        fn native_reader_scan(
            reader: &mut NativeReader,
            bucket: u16,
            start: &[u8],
            end: &[u8],
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_reader_mode(reader: &NativeReader) -> u8;
        fn native_reader_has_configured_snapshot(reader: &NativeReader) -> bool;
        fn native_reader_configured_snapshot(reader: &NativeReader) -> u64;
        fn native_reader_current_global_snapshot(reader: &NativeReader) -> NativeSnapshot;
        fn native_reader_list_global_snapshots(
            reader: &NativeReader,
        ) -> Result<Vec<NativeSnapshot>>;

        fn native_coordinator_open(config_json: &str) -> Result<Box<NativeCoordinator>>;
        fn native_coordinator_open_file(config_path: &str) -> Result<Box<NativeCoordinator>>;
        fn native_coordinator_materialize_global_snapshot(
            coordinator: &NativeCoordinator,
            total_buckets: u32,
            snapshot_id: u64,
            shards: Vec<NativeShardSnapshot>,
        ) -> Result<NativeSnapshot>;
        fn native_coordinator_get_global_snapshot(
            coordinator: &NativeCoordinator,
            snapshot_id: u64,
        ) -> Result<NativeSnapshot>;
        fn native_coordinator_list_global_snapshots(
            coordinator: &NativeCoordinator,
        ) -> Result<Vec<NativeSnapshot>>;
        fn native_coordinator_load_current_global_snapshot(
            coordinator: &NativeCoordinator,
        ) -> Result<Vec<NativeSnapshot>>;
        fn native_coordinator_retain_snapshot(
            coordinator: &NativeCoordinator,
            snapshot_id: u64,
        ) -> bool;
        fn native_coordinator_expire_snapshot(
            coordinator: &NativeCoordinator,
            snapshot_id: u64,
        ) -> Result<bool>;

        fn native_scan_split_split_after(
            split: NativeScanSplit,
            bucket: u16,
            key: &[u8],
        ) -> Result<Vec<NativeScanSplit>>;
        fn native_scan_split_to_json(split: NativeScanSplit) -> Result<String>;
        fn native_scan_split_from_json(json: &str) -> Result<NativeScanSplit>;
        fn native_scan_split_open_scanner(
            config_json: &str,
            split: NativeScanSplit,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
        fn native_scan_split_open_scanner_file(
            config_path: &str,
            split: NativeScanSplit,
            options: &NativeScanOptions,
        ) -> Result<Box<NativeScanCursor>>;
    }
}

#[cfg(test)]
mod tests;
