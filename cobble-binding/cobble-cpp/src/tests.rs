use crate::error::format_cobble_error;
use crate::{
    database::{native_database_close, native_database_put, open_database},
    encoding::{
        STATUS_BUFFER_TOO_SMALL, STATUS_END, STATUS_OK, batch_encoded_len, encode_batch_into,
    },
    ffi, native_database_scan, native_scan_cursor_next_batch_into,
    scan::{NativeBatch, NativeBatchRow},
};
use bytes::Bytes;
use cobble_binding::{Config, Error};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

fn batch(rows: Vec<NativeBatchRow>, end: bool, boundary: bool) -> NativeBatch {
    NativeBatch {
        rows,
        end,
        stopped_at_block_boundary: boundary,
    }
}

#[test]
fn encodes_little_endian_versioned_batch_with_null_columns() {
    let batch = batch(
        vec![NativeBatchRow {
            bucket: 7,
            key: Bytes::from_static(b"key"),
            columns: vec![Some(Bytes::from_static(b"value")), None],
        }],
        true,
        false,
    );
    let mut encoded = vec![0; batch_encoded_len(&batch).unwrap()];
    encode_batch_into(&batch, &mut encoded).unwrap();

    assert_eq!(&encoded[..4], b"CBRB");
    assert_eq!(u16::from_le_bytes(encoded[4..6].try_into().unwrap()), 1);
    assert_eq!(u16::from_le_bytes(encoded[6..8].try_into().unwrap()), 24);
    assert_eq!(u32::from_le_bytes(encoded[8..12].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(encoded[12..16].try_into().unwrap()), 1);
    assert_eq!(
        u64::from_le_bytes(encoded[16..24].try_into().unwrap()) as usize,
        encoded.len()
    );
    assert_eq!(u16::from_le_bytes(encoded[24..26].try_into().unwrap()), 7);
    assert_eq!(u32::from_le_bytes(encoded[28..32].try_into().unwrap()), 3);
    assert_eq!(u32::from_le_bytes(encoded[32..36].try_into().unwrap()), 2);
    assert_eq!(&encoded[36..39], b"key");
    assert_eq!(u64::from_le_bytes(encoded[39..47].try_into().unwrap()), 5);
    assert_eq!(&encoded[47..52], b"value");
    assert_eq!(
        u64::from_le_bytes(encoded[52..60].try_into().unwrap()),
        u64::MAX
    );
}

#[test]
fn too_small_batch_buffer_can_be_checked_without_mutating_output() {
    let batch = batch(Vec::new(), true, false);
    let required = batch_encoded_len(&batch).unwrap();
    let output = vec![0xa5; required - 1];
    assert!(output.len() < required);
    assert_eq!(output, vec![0xa5; required - 1]);
}

#[test]
fn error_prefixes_cover_every_cobble_error_variant() {
    let cases = [
        (Error::FileSystemError("x".to_string()), "CB_FILE_SYSTEM:"),
        (Error::IoError("x".to_string()), "CB_IO:"),
        (
            Error::MemtableFull {
                needed: 2,
                remaining: 1,
            },
            "CB_MEMTABLE_FULL:",
        ),
        (Error::ConfigError("x".to_string()), "CB_CONFIGURATION:"),
        (Error::InputError("x".to_string()), "CB_INPUT:"),
        (
            Error::CoordinationError("x".to_string()),
            "CB_COORDINATION:",
        ),
        (Error::InvalidState("x".to_string()), "CB_INVALID_STATE:"),
        (Error::FileFormatError("x".to_string()), "CB_FILE_FORMAT:"),
        (Error::ChecksumMismatch("x".to_string()), "CB_CHECKSUM:"),
        (Error::CancelledError("x".to_string()), "CB_CANCELLED:"),
    ];
    for (error, prefix) in cases {
        assert!(format_cobble_error(error).starts_with(prefix));
    }
}

#[test]
fn caller_buffer_scan_keeps_pending_batch_after_small_buffer() {
    let root = format!(
        "/tmp/cobble_cpp_pending_batch_{}_{}",
        std::process::id(),
        NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed)
    );
    let _ = std::fs::remove_dir_all(&root);
    let config = Config {
        volumes: cobble_binding::VolumeDescriptor::single_volume(format!("file://{root}")),
        num_columns: 1,
        total_buckets: 1,
        ..Config::default()
    };
    let db = open_database(config).unwrap();
    let write_options = ffi::NativeWriteOptions {
        has_ttl_seconds: false,
        ttl_seconds: 0,
        column_family: String::new(),
        await_durable: true,
    };
    native_database_put(&db, 0, b"key", 0, b"value", &write_options).unwrap();
    let scan_options = ffi::NativeScanOptions {
        column_family: String::new(),
        columns: Vec::new(),
        read_ahead_bytes: 0,
        has_max_rows: false,
        max_rows: 0,
        preload_scan_cursor_block: false,
        stop_at_block_boundary: false,
    };
    let mut cursor = native_database_scan(&db, 0, &[], false, &[], false, &scan_options).unwrap();

    let mut too_small = [0xa5; 1];
    let result = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut too_small).unwrap();
    assert_eq!(result.status, STATUS_BUFFER_TOO_SMALL);
    assert_eq!(too_small, [0xa5; 1]);

    let mut output = vec![0; usize::try_from(result.bytes_required).unwrap()];
    let written = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut output).unwrap();
    assert_eq!(written.status, STATUS_OK);
    assert_eq!(written.row_count, 1);
    assert_eq!(&output[..4], b"CBRB");

    let end = native_scan_cursor_next_batch_into(&mut cursor, 1, &mut []).unwrap();
    assert_eq!(end.status, STATUS_END);
    drop(cursor);
    native_database_close(&db).unwrap();
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
