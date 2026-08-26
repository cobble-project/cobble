use cobble_binding::WriteBatch;

use crate::{ffi, options::to_write_options};

pub(crate) struct NativeWriteBatch {
    pub(crate) batch: WriteBatch,
    count: u64,
}

pub(crate) fn native_write_batch_new() -> Box<NativeWriteBatch> {
    Box::new(NativeWriteBatch {
        batch: WriteBatch::new(),
        count: 0,
    })
}
pub(crate) fn native_write_batch_len(batch: &NativeWriteBatch) -> u64 {
    batch.count
}
pub(crate) fn native_write_batch_put(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .put_with_options(bucket, key, column, value, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}
pub(crate) fn native_write_batch_delete(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .delete_with_options(bucket, key, column, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}
pub(crate) fn native_write_batch_merge(
    batch: &mut NativeWriteBatch,
    bucket: u16,
    key: &[u8],
    column: u16,
    value: &[u8],
    options: &ffi::NativeWriteOptions,
) {
    batch
        .batch
        .merge_with_options(bucket, key, column, value, &to_write_options(options));
    batch.count = batch.count.saturating_add(1);
}
