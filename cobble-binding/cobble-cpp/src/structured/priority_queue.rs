use std::sync::Arc;

use bytes::Bytes;
use cobble_binding::structured::{StructuredDb, StructuredSingleDb, ffi as data_structure_ffi};

use crate::structured_bridge::ffi;

use super::conversion::{format_error, input_error};
use super::encoding::{
    CsrbColumns, CsrbRow, STATUS_BUFFER_TOO_SMALL, STATUS_END, STATUS_NOT_FOUND, STATUS_OK, prepare,
};
use super::multi_get::buffer_result;
use super::{BridgeResult, NativeStructuredDb, NativeStructuredSingleDb};

fn invalid_state(message: impl std::fmt::Display) -> String {
    format!("CB_INVALID_STATE: {message}")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PriorityQueueOperation {
    Peek,
    Poll,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PriorityQueueRequest {
    operation: PriorityQueueOperation,
    bucket: u16,
    limit: Option<usize>,
    single: bool,
}

struct PendingPriorityQueueBatch {
    request: PriorityQueueRequest,
    rows: Vec<(Bytes, Bytes)>,
}

enum NativePriorityQueueOwner {
    Db(Arc<StructuredDb>),
    SingleDb(Arc<StructuredSingleDb>),
}

pub(crate) struct NativeStructuredPriorityQueue {
    descriptor: data_structure_ffi::PriorityQueueDescriptor,
    pending: Option<PendingPriorityQueueBatch>,
    // Declared last so all descriptor/pending data is released before the last
    // database owner. This prevents close-on-drop from waiting on a child.
    owner: NativePriorityQueueOwner,
}

pub(crate) struct NativePriorityQueueBatch {
    rows: Vec<(Bytes, Bytes)>,
}

pub(crate) struct NativePriorityQueueCursor {
    value: Option<Vec<u8>>,
}

impl NativeStructuredPriorityQueue {
    fn ensure_idle(&self) -> BridgeResult<()> {
        if self.pending.is_some() {
            Err(invalid_state(
                "priority queue has a pending caller-buffer result; retry the same operation",
            ))
        } else {
            Ok(())
        }
    }

    fn offer(&self, bucket: u16, key: &[u8], value: &[u8]) -> BridgeResult<()> {
        self.ensure_idle()?;
        match &self.owner {
            NativePriorityQueueOwner::Db(db) => data_structure_ffi::db_priority_queue_offer(
                db,
                &self.descriptor,
                bucket,
                key,
                value,
            ),
            NativePriorityQueueOwner::SingleDb(db) => {
                data_structure_ffi::single_db_priority_queue_offer(
                    db,
                    &self.descriptor,
                    bucket,
                    key,
                    value,
                )
            }
        }
        .map_err(format_error)
    }

    fn delete(&self, bucket: u16, key: &[u8]) -> BridgeResult<()> {
        self.ensure_idle()?;
        match &self.owner {
            NativePriorityQueueOwner::Db(db) => {
                data_structure_ffi::db_priority_queue_delete(db, &self.descriptor, bucket, key)
            }
            NativePriorityQueueOwner::SingleDb(db) => {
                data_structure_ffi::single_db_priority_queue_delete(
                    db,
                    &self.descriptor,
                    bucket,
                    key,
                )
            }
        }
        .map_err(format_error)
    }

    fn peek_batch(&self, bucket: u16, limit: Option<usize>) -> BridgeResult<Vec<(Bytes, Bytes)>> {
        match &self.owner {
            NativePriorityQueueOwner::Db(db) => data_structure_ffi::db_priority_queue_peek_batch(
                db,
                &self.descriptor,
                bucket,
                limit,
            ),
            NativePriorityQueueOwner::SingleDb(db) => {
                data_structure_ffi::single_db_priority_queue_peek_batch(
                    db,
                    &self.descriptor,
                    bucket,
                    limit,
                )
            }
        }
        .map_err(format_error)
    }

    fn advance_internal(&self, bucket: u16, key: &[u8]) -> BridgeResult<()> {
        match &self.owner {
            NativePriorityQueueOwner::Db(db) => {
                data_structure_ffi::db_priority_queue_advance(db, &self.descriptor, bucket, key)
            }
            NativePriorityQueueOwner::SingleDb(db) => {
                data_structure_ffi::single_db_priority_queue_advance(
                    db,
                    &self.descriptor,
                    bucket,
                    key,
                )
            }
        }
        .map_err(format_error)
    }

    fn cursor_internal(&self, bucket: u16) -> BridgeResult<Option<Vec<u8>>> {
        match &self.owner {
            NativePriorityQueueOwner::Db(db) => {
                data_structure_ffi::db_priority_queue_cursor(db, &self.descriptor, bucket)
            }
            NativePriorityQueueOwner::SingleDb(db) => {
                data_structure_ffi::single_db_priority_queue_cursor(db, &self.descriptor, bucket)
            }
        }
        .map_err(format_error)
    }

    fn owned_batch(
        &self,
        request: PriorityQueueRequest,
    ) -> BridgeResult<Box<NativePriorityQueueBatch>> {
        self.ensure_idle()?;
        let rows = self.peek_batch(request.bucket, request.limit)?;
        if request.operation == PriorityQueueOperation::Poll
            && let Some((key, _)) = rows.last()
        {
            self.advance_internal(request.bucket, key)?;
        }
        Ok(Box::new(NativePriorityQueueBatch { rows }))
    }

    fn batch_into(
        &mut self,
        request: PriorityQueueRequest,
        output: &mut [u8],
    ) -> BridgeResult<ffi::NativeBufferResult> {
        match self.pending.as_ref() {
            Some(pending) if pending.request != request => {
                return Err(invalid_state(
                    "priority queue caller-buffer retry must use the same operation, bucket, and limit",
                ));
            }
            Some(_) => {}
            None => {
                let rows = self.peek_batch(request.bucket, request.limit)?;
                self.pending = Some(PendingPriorityQueueBatch { request, rows });
            }
        }

        let pending = self.pending.as_ref().expect("pending batch initialized");
        let csrb_rows = pending
            .rows
            .iter()
            .map(|(key, value)| CsrbRow {
                bucket: request.bucket,
                key,
                columns: CsrbColumns::PriorityQueue(value),
            })
            .collect::<Vec<_>>();
        let prepared = prepare(&csrb_rows, pending.rows.is_empty(), false)?;
        let required = prepared.required_len();
        if output.len() < required {
            return Ok(buffer_result(
                STATUS_BUFFER_TOO_SMALL,
                0,
                required,
                pending.rows.len(),
            ));
        }

        // The buffer is still untouched here. Complete the externally visible
        // poll first so an advance failure cannot expose rows as consumed.
        if request.operation == PriorityQueueOperation::Poll
            && let Some((key, _)) = pending.rows.last()
        {
            self.advance_internal(request.bucket, key)?;
        }
        let written = prepared.encode_into(output);
        let row_count = pending.rows.len();
        self.pending = None;
        Ok(buffer_result(
            if row_count == 0 {
                if request.single {
                    STATUS_NOT_FOUND
                } else {
                    STATUS_END
                }
            } else {
                STATUS_OK
            },
            written,
            written,
            row_count,
        ))
    }
}

fn exclusive_db(db: &mut NativeStructuredDb) -> BridgeResult<&mut StructuredDb> {
    Arc::get_mut(&mut db.db).ok_or_else(|| {
        invalid_state(
            "priority queue creation requires exclusive ownership; release every structured scan cursor, schema builder, and priority queue first",
        )
    })
}

fn exclusive_single_db(db: &mut NativeStructuredSingleDb) -> BridgeResult<&mut StructuredSingleDb> {
    Arc::get_mut(&mut db.db).ok_or_else(|| {
        invalid_state(
            "priority queue creation requires exclusive ownership; release every structured scan cursor, schema builder, and priority queue first",
        )
    })
}

fn native_queue(
    descriptor: data_structure_ffi::PriorityQueueDescriptor,
    owner: NativePriorityQueueOwner,
) -> Box<NativeStructuredPriorityQueue> {
    Box::new(NativeStructuredPriorityQueue {
        descriptor,
        pending: None,
        owner,
    })
}

pub(crate) fn native_structured_db_new_priority_queue(
    db: &mut NativeStructuredDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor =
        data_structure_ffi::db_new_priority_queue_descriptor(exclusive_db(db)?, name.to_owned())
            .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::Db(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_db_get_priority_queue(
    db: &NativeStructuredDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor = data_structure_ffi::db_get_priority_queue_descriptor(&db.db, name.to_owned())
        .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::Db(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_db_get_or_new_priority_queue(
    db: &mut NativeStructuredDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor = data_structure_ffi::db_get_or_new_priority_queue_descriptor(
        exclusive_db(db)?,
        name.to_owned(),
    )
    .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::Db(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_single_db_new_priority_queue(
    db: &mut NativeStructuredSingleDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor = data_structure_ffi::single_db_new_priority_queue_descriptor(
        exclusive_single_db(db)?,
        name.to_owned(),
    )
    .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::SingleDb(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_single_db_get_priority_queue(
    db: &NativeStructuredSingleDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor =
        data_structure_ffi::single_db_get_priority_queue_descriptor(&db.db, name.to_owned())
            .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::SingleDb(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_single_db_get_or_new_priority_queue(
    db: &mut NativeStructuredSingleDb,
    name: &str,
) -> BridgeResult<Box<NativeStructuredPriorityQueue>> {
    let descriptor = data_structure_ffi::single_db_get_or_new_priority_queue_descriptor(
        exclusive_single_db(db)?,
        name.to_owned(),
    )
    .map_err(format_error)?;
    Ok(native_queue(
        descriptor,
        NativePriorityQueueOwner::SingleDb(Arc::clone(&db.db)),
    ))
}

pub(crate) fn native_structured_priority_queue_column_family(
    queue: &NativeStructuredPriorityQueue,
) -> &str {
    queue.descriptor.column_family()
}

pub(crate) fn native_structured_priority_queue_offer(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
    key: &[u8],
    value: &[u8],
) -> BridgeResult<()> {
    queue.offer(bucket, key, value)
}

pub(crate) fn native_structured_priority_queue_delete(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
    key: &[u8],
) -> BridgeResult<()> {
    queue.delete(bucket, key)
}

fn request(
    operation: PriorityQueueOperation,
    bucket: u16,
    has_limit: bool,
    limit: usize,
    single: bool,
) -> PriorityQueueRequest {
    PriorityQueueRequest {
        operation,
        bucket,
        limit: has_limit.then_some(limit),
        single,
    }
}

pub(crate) fn native_structured_priority_queue_peek(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
) -> BridgeResult<Box<NativePriorityQueueBatch>> {
    queue.owned_batch(request(PriorityQueueOperation::Peek, bucket, true, 1, true))
}

pub(crate) fn native_structured_priority_queue_poll(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
) -> BridgeResult<Box<NativePriorityQueueBatch>> {
    queue.owned_batch(request(PriorityQueueOperation::Poll, bucket, true, 1, true))
}

pub(crate) fn native_structured_priority_queue_peek_batch(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
    has_limit: bool,
    limit: usize,
) -> BridgeResult<Box<NativePriorityQueueBatch>> {
    queue.owned_batch(request(
        PriorityQueueOperation::Peek,
        bucket,
        has_limit,
        limit,
        false,
    ))
}

pub(crate) fn native_structured_priority_queue_poll_batch(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
    has_limit: bool,
    limit: usize,
) -> BridgeResult<Box<NativePriorityQueueBatch>> {
    queue.owned_batch(request(
        PriorityQueueOperation::Poll,
        bucket,
        has_limit,
        limit,
        false,
    ))
}

pub(crate) fn native_structured_priority_queue_peek_into(
    queue: &mut NativeStructuredPriorityQueue,
    bucket: u16,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    queue.batch_into(
        request(PriorityQueueOperation::Peek, bucket, true, 1, true),
        output,
    )
}

pub(crate) fn native_structured_priority_queue_poll_into(
    queue: &mut NativeStructuredPriorityQueue,
    bucket: u16,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    queue.batch_into(
        request(PriorityQueueOperation::Poll, bucket, true, 1, true),
        output,
    )
}

pub(crate) fn native_structured_priority_queue_peek_batch_into(
    queue: &mut NativeStructuredPriorityQueue,
    bucket: u16,
    has_limit: bool,
    limit: usize,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    queue.batch_into(
        request(
            PriorityQueueOperation::Peek,
            bucket,
            has_limit,
            limit,
            false,
        ),
        output,
    )
}

pub(crate) fn native_structured_priority_queue_poll_batch_into(
    queue: &mut NativeStructuredPriorityQueue,
    bucket: u16,
    has_limit: bool,
    limit: usize,
    output: &mut [u8],
) -> BridgeResult<ffi::NativeBufferResult> {
    queue.batch_into(
        request(
            PriorityQueueOperation::Poll,
            bucket,
            has_limit,
            limit,
            false,
        ),
        output,
    )
}

pub(crate) fn native_structured_priority_queue_advance(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
    key: &[u8],
) -> BridgeResult<()> {
    queue.ensure_idle()?;
    queue.advance_internal(bucket, key)
}

pub(crate) fn native_structured_priority_queue_cursor(
    queue: &NativeStructuredPriorityQueue,
    bucket: u16,
) -> BridgeResult<Box<NativePriorityQueueCursor>> {
    queue.ensure_idle()?;
    Ok(Box::new(NativePriorityQueueCursor {
        value: queue.cursor_internal(bucket)?,
    }))
}

pub(crate) fn native_priority_queue_batch_size(batch: &NativePriorityQueueBatch) -> usize {
    batch.rows.len()
}

pub(crate) fn native_priority_queue_batch_key(
    batch: &NativePriorityQueueBatch,
    index: usize,
) -> BridgeResult<&[u8]> {
    batch
        .rows
        .get(index)
        .map(|(key, _)| key.as_ref())
        .ok_or_else(|| input_error("priority queue batch index is out of bounds"))
}

pub(crate) fn native_priority_queue_batch_value(
    batch: &NativePriorityQueueBatch,
    index: usize,
) -> BridgeResult<&[u8]> {
    batch
        .rows
        .get(index)
        .map(|(_, value)| value.as_ref())
        .ok_or_else(|| input_error("priority queue batch index is out of bounds"))
}

pub(crate) fn native_priority_queue_cursor_has_value(cursor: &NativePriorityQueueCursor) -> bool {
    cursor.value.is_some()
}

pub(crate) fn native_priority_queue_cursor_value(
    cursor: &NativePriorityQueueCursor,
) -> BridgeResult<&[u8]> {
    cursor
        .value
        .as_deref()
        .ok_or_else(|| input_error("priority queue cursor is empty"))
}

#[cfg(test)]
mod tests {
    use cobble_binding::{Config, VolumeDescriptor};

    use super::*;

    #[test]
    fn caller_buffer_poll_retry_is_transactional() {
        let root = std::env::temp_dir().join(format!(
            "cobble_cpp_priority_queue_retry_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = Config {
            volumes: VolumeDescriptor::single_volume(format!("file://{}", root.to_string_lossy())),
            total_buckets: 1,
            ..Config::default()
        };
        let db = StructuredSingleDb::open(config).unwrap();
        let mut native_db = NativeStructuredSingleDb { db: Arc::new(db) };
        let mut queue =
            native_structured_single_db_new_priority_queue(&mut native_db, "jobs").unwrap();
        native_structured_priority_queue_offer(&queue, 0, b"k1", b"v1").unwrap();
        native_structured_priority_queue_offer(&queue, 0, b"k2", b"v2").unwrap();

        let mut small = [0xa5; 4];
        let before = small;
        let first =
            native_structured_priority_queue_poll_batch_into(&mut queue, 0, true, 1, &mut small)
                .unwrap();
        assert_eq!(first.status, STATUS_BUFFER_TOO_SMALL);
        assert_eq!(small, before);
        assert!(native_structured_priority_queue_peek(&queue, 0).is_err());
        assert!(
            native_structured_priority_queue_poll_batch_into(
                &mut queue,
                0,
                true,
                2,
                &mut vec![0; first.bytes_required as usize],
            )
            .is_err()
        );

        let mut output = vec![0; first.bytes_required as usize];
        let retry =
            native_structured_priority_queue_poll_batch_into(&mut queue, 0, true, 1, &mut output)
                .unwrap();
        assert_eq!(retry.status, STATUS_OK);
        assert_eq!(retry.row_count, 1);
        assert_eq!(&output[..4], b"CSRB");
        assert_eq!(
            native_structured_priority_queue_cursor(&queue, 0)
                .unwrap()
                .value
                .as_deref(),
            Some(b"k1".as_slice())
        );
        let next = native_structured_priority_queue_peek(&queue, 0).unwrap();
        assert_eq!(next.rows[0].0.as_ref(), b"k2");

        drop(queue);
        native_db.db.close().unwrap();
        drop(native_db);
        let _ = std::fs::remove_dir_all(root);
    }
}
