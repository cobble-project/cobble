//! Private Rust surface for the C++ structured binding.

use crate::structured::*;

#[allow(clippy::too_many_arguments)]
#[cxx::bridge(namespace = "cobble::structured_ffi")]
pub(crate) mod ffi {
    struct NativeBucketRange {
        start_inclusive: u16,
        end_inclusive: u16,
    }

    struct NativeWriteOptions {
        has_ttl_seconds: bool,
        ttl_seconds: u32,
        has_column_family: bool,
        column_family: String,
        await_durable: bool,
    }

    struct NativeBytesDescriptor {
        data: usize,
        length: usize,
    }

    struct NativeBufferResult {
        status: u8,
        bytes_written: u64,
        bytes_required: u64,
        row_count: u64,
    }

    struct NativeListConfig {
        has_max_elements: bool,
        max_elements: u64,
        retain_mode: u8,
        preserve_element_ttl: bool,
    }

    struct NativeStructuredColumn {
        index: u16,
        kind: u8,
        list: NativeListConfig,
    }

    struct NativeStructuredFamily {
        name: String,
        id: u8,
        columns: Vec<NativeStructuredColumn>,
    }

    struct NativeStructuredSchema {
        families: Vec<NativeStructuredFamily>,
    }

    struct NativeFamily {
        name: String,
        id: u8,
    }

    struct NativeShardSnapshot {
        ranges: Vec<NativeBucketRange>,
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

    struct NativeStructuredScanSplit {
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

    struct NativeMetricLabel {
        key: String,
        value: String,
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

    extern "Rust" {
        type NativeStructuredDb;
        type NativeStructuredSingleDb;
        type NativeStructuredReadOptions;
        type NativeStructuredScanOptions;
        type NativeStructuredRow;
        type NativeStructuredMultiGetResult;
        type NativeStructuredScanCursor;
        type NativeStructuredBatch;
        type NativeStructuredSchemaEdit;
        type NativeStructuredPriorityQueue;
        type NativePriorityQueueBatch;
        type NativePriorityQueueCursor;
        type NativePendingShardSnapshot;
        type NativePendingSnapshot;

        fn native_structured_db_new_priority_queue(
            db: &mut NativeStructuredDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_db_get_priority_queue(
            db: &NativeStructuredDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_db_get_or_new_priority_queue(
            db: &mut NativeStructuredDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_single_db_new_priority_queue(
            db: &mut NativeStructuredSingleDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_single_db_get_priority_queue(
            db: &NativeStructuredSingleDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_single_db_get_or_new_priority_queue(
            db: &mut NativeStructuredSingleDb,
            name: &str,
        ) -> Result<Box<NativeStructuredPriorityQueue>>;
        fn native_structured_priority_queue_column_family(
            queue: &NativeStructuredPriorityQueue,
        ) -> &str;
        fn native_structured_priority_queue_offer(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
            key: &[u8],
            value: &[u8],
        ) -> Result<()>;
        fn native_structured_priority_queue_delete(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
            key: &[u8],
        ) -> Result<()>;
        fn native_structured_priority_queue_peek(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
        ) -> Result<Box<NativePriorityQueueBatch>>;
        fn native_structured_priority_queue_poll(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
        ) -> Result<Box<NativePriorityQueueBatch>>;
        fn native_structured_priority_queue_peek_batch(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
            has_limit: bool,
            limit: usize,
        ) -> Result<Box<NativePriorityQueueBatch>>;
        fn native_structured_priority_queue_poll_batch(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
            has_limit: bool,
            limit: usize,
        ) -> Result<Box<NativePriorityQueueBatch>>;
        fn native_structured_priority_queue_peek_into(
            queue: &mut NativeStructuredPriorityQueue,
            bucket: u16,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_priority_queue_poll_into(
            queue: &mut NativeStructuredPriorityQueue,
            bucket: u16,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_priority_queue_peek_batch_into(
            queue: &mut NativeStructuredPriorityQueue,
            bucket: u16,
            has_limit: bool,
            limit: usize,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_priority_queue_poll_batch_into(
            queue: &mut NativeStructuredPriorityQueue,
            bucket: u16,
            has_limit: bool,
            limit: usize,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_priority_queue_advance(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
            key: &[u8],
        ) -> Result<()>;
        fn native_structured_priority_queue_cursor(
            queue: &NativeStructuredPriorityQueue,
            bucket: u16,
        ) -> Result<Box<NativePriorityQueueCursor>>;
        fn native_priority_queue_batch_size(batch: &NativePriorityQueueBatch) -> usize;
        fn native_priority_queue_batch_key(
            batch: &NativePriorityQueueBatch,
            index: usize,
        ) -> Result<&[u8]>;
        fn native_priority_queue_batch_value(
            batch: &NativePriorityQueueBatch,
            index: usize,
        ) -> Result<&[u8]>;
        fn native_priority_queue_cursor_has_value(cursor: &NativePriorityQueueCursor) -> bool;
        fn native_priority_queue_cursor_value(cursor: &NativePriorityQueueCursor) -> Result<&[u8]>;

        fn native_structured_db_open(config_json: &str) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_open_ranges(
            config_json: &str,
            ranges: Vec<NativeBucketRange>,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_open_file(config_path: &str) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_open_file_ranges(
            config_path: &str,
            ranges: Vec<NativeBucketRange>,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_open_from_snapshot(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_open_from_snapshot_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_restore_new(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_restore_new_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_restore_new_from_manifest(
            config_json: &str,
            manifest_path: &str,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_restore_new_from_manifest_file(
            config_path: &str,
            manifest_path: &str,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_resume(
            config_json: &str,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_resume_file(
            config_path: &str,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_resume_from_snapshot(
            config_json: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;
        fn native_structured_db_resume_from_snapshot_file(
            config_path: &str,
            snapshot_id: u64,
            db_id: &str,
            recovery_mode: u8,
        ) -> Result<Box<NativeStructuredDb>>;

        fn native_structured_single_db_open(
            config_json: &str,
        ) -> Result<Box<NativeStructuredSingleDb>>;
        fn native_structured_single_db_open_file(
            config_path: &str,
        ) -> Result<Box<NativeStructuredSingleDb>>;

        fn native_structured_read_options_new() -> Box<NativeStructuredReadOptions>;
        fn native_structured_read_options_clone(
            options: &NativeStructuredReadOptions,
        ) -> Box<NativeStructuredReadOptions>;
        fn native_structured_read_options_set_family(
            options: &mut NativeStructuredReadOptions,
            has_family: bool,
            family: &str,
        ) -> Result<()>;
        fn native_structured_read_options_set_columns(
            options: &mut NativeStructuredReadOptions,
            columns: Vec<u64>,
        ) -> Result<()>;
        fn native_structured_scan_options_new() -> Box<NativeStructuredScanOptions>;
        fn native_structured_scan_options_clone(
            options: &NativeStructuredScanOptions,
        ) -> Box<NativeStructuredScanOptions>;
        fn native_structured_scan_options_set_family(
            options: &mut NativeStructuredScanOptions,
            has_family: bool,
            family: &str,
        ) -> Result<()>;
        fn native_structured_scan_options_set_columns(
            options: &mut NativeStructuredScanOptions,
            columns: Vec<u64>,
        ) -> Result<()>;
        fn native_structured_scan_options_set_preload(
            options: &mut NativeStructuredScanOptions,
            enabled: bool,
        );
        fn native_structured_scan_options_set_stop_at_block_boundary(
            options: &mut NativeStructuredScanOptions,
            enabled: bool,
        );

        fn native_structured_db_id(db: &NativeStructuredDb) -> &str;
        fn native_structured_db_put_bytes(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_db_put_list(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            elements: Vec<NativeBytesDescriptor>,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_db_merge_bytes(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_db_merge_list(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            elements: Vec<NativeBytesDescriptor>,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_db_delete(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_db_get(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            options: &NativeStructuredReadOptions,
        ) -> Result<Box<NativeStructuredRow>>;
        fn native_structured_db_get_into(
            db: &NativeStructuredDb,
            bucket: u16,
            key: &[u8],
            options: &NativeStructuredReadOptions,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_db_multi_get(
            db: &NativeStructuredDb,
            descriptor_address: usize,
            count: u64,
            options: &NativeStructuredReadOptions,
        ) -> Result<Box<NativeStructuredMultiGetResult>>;
        fn native_structured_db_multi_get_into(
            db: &NativeStructuredDb,
            descriptor_address: usize,
            count: u64,
            options: &NativeStructuredReadOptions,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_db_write(
            db: &NativeStructuredDb,
            descriptor_address: usize,
            count: u64,
        ) -> Result<()>;
        fn native_structured_db_scan(
            db: &NativeStructuredDb,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeStructuredScanOptions,
        ) -> Result<Box<NativeStructuredScanCursor>>;
        fn native_structured_db_current_schema(db: &NativeStructuredDb) -> NativeStructuredSchema;
        fn native_structured_db_commit_schema(
            db: &mut NativeStructuredDb,
            edit: &mut NativeStructuredSchemaEdit,
        ) -> Result<NativeStructuredSchema>;
        fn native_structured_db_set_time(db: &NativeStructuredDb, unix_seconds: u32);
        fn native_structured_db_now_seconds(db: &NativeStructuredDb) -> u32;
        fn native_structured_db_snapshot(db: &NativeStructuredDb) -> Result<u64>;
        fn native_structured_db_start_snapshot(
            db: &NativeStructuredDb,
        ) -> Result<Box<NativePendingShardSnapshot>>;
        fn native_pending_shard_snapshot_id(pending: &NativePendingShardSnapshot) -> u64;
        fn native_pending_shard_snapshot_wait(
            pending: &mut NativePendingShardSnapshot,
        ) -> Result<NativeShardSnapshot>;
        fn native_structured_db_take_snapshot(
            db: &NativeStructuredDb,
        ) -> Result<NativeShardSnapshot>;
        fn native_structured_db_cancel_snapshot(
            db: &NativeStructuredDb,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_structured_db_get_shard_snapshot(
            db: &NativeStructuredDb,
            snapshot_id: u64,
        ) -> Result<NativeShardSnapshot>;
        fn native_structured_db_retain_snapshot(db: &NativeStructuredDb, snapshot_id: u64) -> bool;
        fn native_structured_db_expire_snapshot(
            db: &NativeStructuredDb,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_structured_db_switch_to_snapshot(
            db: &mut NativeStructuredDb,
            snapshot_id: u64,
        ) -> Result<()>;
        fn native_structured_db_metrics(db: &NativeStructuredDb) -> Vec<NativeMetric>;
        fn native_structured_db_switch_memtable_type(
            db: &NativeStructuredDb,
            memtable_type: u8,
            flush_current: bool,
        ) -> Result<()>;
        fn native_structured_db_load_readonly_files_to_primary(
            db: &NativeStructuredDb,
        ) -> Result<u64>;
        fn native_structured_db_expand_bucket(
            db: &NativeStructuredDb,
            source_db_id: &str,
            has_snapshot_id: bool,
            snapshot_id: u64,
            has_ranges: bool,
            ranges: Vec<NativeBucketRange>,
            storage_mode: u8,
        ) -> Result<u64>;
        fn native_structured_db_wait_for_expand_adoption(
            db: &NativeStructuredDb,
            timeout_millis: i64,
        ) -> Result<()>;
        fn native_structured_db_shrink_bucket(
            db: &NativeStructuredDb,
            ranges: Vec<NativeBucketRange>,
        ) -> Result<u64>;
        fn native_structured_db_close(db: &NativeStructuredDb) -> Result<()>;

        fn native_structured_single_db_put_bytes(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_single_db_put_list(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            elements: Vec<NativeBytesDescriptor>,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_single_db_merge_bytes(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            value: &[u8],
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_single_db_merge_list(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            elements: Vec<NativeBytesDescriptor>,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_single_db_delete(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            column: u16,
            options: &NativeWriteOptions,
        ) -> Result<()>;
        fn native_structured_single_db_get(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            options: &NativeStructuredReadOptions,
        ) -> Result<Box<NativeStructuredRow>>;
        fn native_structured_single_db_get_into(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            key: &[u8],
            options: &NativeStructuredReadOptions,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_single_db_multi_get(
            db: &NativeStructuredSingleDb,
            descriptor_address: usize,
            count: u64,
            options: &NativeStructuredReadOptions,
        ) -> Result<Box<NativeStructuredMultiGetResult>>;
        fn native_structured_single_db_multi_get_into(
            db: &NativeStructuredSingleDb,
            descriptor_address: usize,
            count: u64,
            options: &NativeStructuredReadOptions,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_single_db_write(
            db: &NativeStructuredSingleDb,
            descriptor_address: usize,
            count: u64,
        ) -> Result<()>;
        fn native_structured_single_db_scan(
            db: &NativeStructuredSingleDb,
            bucket: u16,
            start: &[u8],
            has_start: bool,
            end: &[u8],
            has_end: bool,
            options: &NativeStructuredScanOptions,
        ) -> Result<Box<NativeStructuredScanCursor>>;
        fn native_structured_single_db_current_schema(
            db: &NativeStructuredSingleDb,
        ) -> NativeStructuredSchema;
        fn native_structured_single_db_commit_schema(
            db: &mut NativeStructuredSingleDb,
            edit: &mut NativeStructuredSchemaEdit,
        ) -> Result<NativeStructuredSchema>;
        fn native_structured_single_db_set_time(db: &NativeStructuredSingleDb, unix_seconds: u32);
        fn native_structured_single_db_now_seconds(db: &NativeStructuredSingleDb) -> u32;
        fn native_structured_single_db_snapshot(db: &NativeStructuredSingleDb) -> Result<u64>;
        fn native_structured_single_db_start_snapshot(
            db: &NativeStructuredSingleDb,
        ) -> Result<Box<NativePendingSnapshot>>;
        fn native_pending_snapshot_id(pending: &NativePendingSnapshot) -> u64;
        fn native_pending_snapshot_wait(
            pending: &mut NativePendingSnapshot,
        ) -> Result<NativeSnapshot>;
        fn native_structured_single_db_take_snapshot(
            db: &NativeStructuredSingleDb,
        ) -> Result<NativeSnapshot>;
        fn native_structured_single_db_list_snapshots(
            db: &NativeStructuredSingleDb,
        ) -> Result<Vec<NativeSnapshot>>;
        fn native_structured_single_db_retain_snapshot(
            db: &NativeStructuredSingleDb,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_structured_single_db_expire_snapshot(
            db: &NativeStructuredSingleDb,
            snapshot_id: u64,
        ) -> Result<bool>;
        fn native_structured_single_db_switch_memtable_type(
            db: &NativeStructuredSingleDb,
            memtable_type: u8,
            flush_current: bool,
        ) -> Result<()>;
        fn native_structured_single_db_load_readonly_files_to_primary(
            db: &NativeStructuredSingleDb,
        ) -> Result<u64>;
        fn native_structured_single_db_close(db: &NativeStructuredSingleDb) -> Result<()>;

        fn native_structured_row_found(row: &NativeStructuredRow) -> bool;
        fn native_structured_row_column_count(row: &NativeStructuredRow) -> usize;
        fn native_structured_row_has_column(row: &NativeStructuredRow, column: usize) -> bool;
        fn native_structured_row_kind(row: &NativeStructuredRow, column: usize) -> Result<u8>;
        fn native_structured_row_bytes(row: &NativeStructuredRow, column: usize) -> Result<&[u8]>;
        fn native_structured_row_list_size(
            row: &NativeStructuredRow,
            column: usize,
        ) -> Result<usize>;
        fn native_structured_row_list_element(
            row: &NativeStructuredRow,
            column: usize,
            element: usize,
        ) -> Result<&[u8]>;

        fn native_structured_multi_get_row_count(result: &NativeStructuredMultiGetResult) -> usize;
        fn native_structured_multi_get_found(
            result: &NativeStructuredMultiGetResult,
            row: usize,
        ) -> bool;
        fn native_structured_multi_get_column_count(
            result: &NativeStructuredMultiGetResult,
            row: usize,
        ) -> Result<usize>;
        fn native_structured_multi_get_has_column(
            result: &NativeStructuredMultiGetResult,
            row: usize,
            column: usize,
        ) -> bool;
        fn native_structured_multi_get_kind(
            result: &NativeStructuredMultiGetResult,
            row: usize,
            column: usize,
        ) -> Result<u8>;
        fn native_structured_multi_get_bytes(
            result: &NativeStructuredMultiGetResult,
            row: usize,
            column: usize,
        ) -> Result<&[u8]>;
        fn native_structured_multi_get_list_size(
            result: &NativeStructuredMultiGetResult,
            row: usize,
            column: usize,
        ) -> Result<usize>;
        fn native_structured_multi_get_list_element(
            result: &NativeStructuredMultiGetResult,
            row: usize,
            column: usize,
            element: usize,
        ) -> Result<&[u8]>;

        fn native_structured_scan_cursor_next_owned(
            cursor: &mut NativeStructuredScanCursor,
            max_rows: u64,
        ) -> Result<Box<NativeStructuredBatch>>;
        fn native_structured_scan_cursor_next_batch_into(
            cursor: &mut NativeStructuredScanCursor,
            max_rows: u64,
            output: &mut [u8],
        ) -> Result<NativeBufferResult>;
        fn native_structured_scan_cursor_resume_after_block_boundary(
            cursor: &mut NativeStructuredScanCursor,
        ) -> Result<()>;
        fn native_structured_batch_row_count(batch: &NativeStructuredBatch) -> usize;
        fn native_structured_batch_end(batch: &NativeStructuredBatch) -> bool;
        fn native_structured_batch_stopped_at_block_boundary(batch: &NativeStructuredBatch)
        -> bool;
        fn native_structured_batch_bucket(batch: &NativeStructuredBatch, row: usize)
        -> Result<u16>;
        fn native_structured_batch_key(batch: &NativeStructuredBatch, row: usize) -> Result<&[u8]>;
        fn native_structured_batch_column_count(
            batch: &NativeStructuredBatch,
            row: usize,
        ) -> Result<usize>;
        fn native_structured_batch_has_column(
            batch: &NativeStructuredBatch,
            row: usize,
            column: usize,
        ) -> bool;
        fn native_structured_batch_kind(
            batch: &NativeStructuredBatch,
            row: usize,
            column: usize,
        ) -> Result<u8>;
        fn native_structured_batch_bytes(
            batch: &NativeStructuredBatch,
            row: usize,
            column: usize,
        ) -> Result<&[u8]>;
        fn native_structured_batch_list_size(
            batch: &NativeStructuredBatch,
            row: usize,
            column: usize,
        ) -> Result<usize>;
        fn native_structured_batch_list_element(
            batch: &NativeStructuredBatch,
            row: usize,
            column: usize,
            element: usize,
        ) -> Result<&[u8]>;

        fn native_structured_scan_split_split_after(
            split: NativeStructuredScanSplit,
            bucket: u16,
            key: &[u8],
        ) -> Result<Vec<NativeStructuredScanSplit>>;
        fn native_structured_scan_split_to_json(split: NativeStructuredScanSplit)
        -> Result<String>;
        fn native_structured_scan_split_from_json(json: &str) -> Result<NativeStructuredScanSplit>;
        fn native_structured_scan_split_open_scanner(
            config_json: &str,
            split: NativeStructuredScanSplit,
            options: &NativeStructuredScanOptions,
        ) -> Result<Box<NativeStructuredScanCursor>>;
        fn native_structured_scan_split_open_scanner_file(
            config_path: &str,
            split: NativeStructuredScanSplit,
            options: &NativeStructuredScanOptions,
        ) -> Result<Box<NativeStructuredScanCursor>>;

        fn native_structured_schema_edit_new() -> Box<NativeStructuredSchemaEdit>;
        fn native_structured_schema_edit_add_bytes(
            edit: &mut NativeStructuredSchemaEdit,
            has_family: bool,
            family: &str,
            column: u16,
        ) -> Result<()>;
        fn native_structured_schema_edit_add_list(
            edit: &mut NativeStructuredSchemaEdit,
            has_family: bool,
            family: &str,
            column: u16,
            config: &NativeListConfig,
        ) -> Result<()>;
        fn native_structured_schema_edit_delete(
            edit: &mut NativeStructuredSchemaEdit,
            has_family: bool,
            family: &str,
            column: u16,
        ) -> Result<()>;
        fn native_structured_schema_edit_set_family_ttl(
            edit: &mut NativeStructuredSchemaEdit,
            has_family: bool,
            family: &str,
            value_has_ttl: bool,
        ) -> Result<()>;
    }
}
