use crate::error::{Error, Result};
use crate::memtable::Memtable;
use crate::r#type::{RefColumn, RefValue, ValueType};
use crate::vlog::{VlogPointer, VlogStore, VlogWriter};
use bytes::{BufMut, Bytes};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) enum RewrittenColumn<'a> {
    Missing,
    Inline(RefColumn<'a>),
    Pointer {
        value_type: ValueType,
        data: [u8; 8],
    },
}

/// A plan for rewriting a RefValue with some columns potentially rewritten to use VLOG pointers.
/// This plan can be used to write a new value to the memtable with the rewritten columns, and also
/// to calculate the encoded length of the rewritten value for size estimation purposes.
pub(crate) struct RewrittenValuePlan<'a> {
    columns: Vec<RewrittenColumn<'a>>,
    expired_at: Option<u32>,
}

impl<'a> RewrittenValuePlan<'a> {
    pub(crate) fn new(columns: Vec<RewrittenColumn<'a>>, expired_at: Option<u32>) -> Self {
        Self {
            columns,
            expired_at,
        }
    }

    pub(crate) fn columns(&self) -> &[RewrittenColumn<'a>] {
        &self.columns
    }

    pub(crate) fn expired_at(&self) -> Option<u32> {
        self.expired_at
    }

    pub(crate) fn encoded_len(&self, num_columns: usize) -> usize {
        let bitmap_len = if num_columns <= 1 {
            0
        } else {
            num_columns.div_ceil(8)
        };
        let present_count = self
            .columns
            .iter()
            .take(num_columns)
            .filter(|column| column.is_present())
            .count();
        let mut total = 4 + bitmap_len;
        let mut present_idx = 0usize;
        for column in self.columns.iter().take(num_columns) {
            if !column.is_present() {
                continue;
            }
            present_idx += 1;
            total += 1;
            if present_idx < present_count {
                total += 4;
            }
            total += column.data_len();
        }
        total
    }
}

impl RewrittenColumn<'_> {
    pub(crate) fn is_present(&self) -> bool {
        !matches!(self, Self::Missing)
    }

    pub(crate) fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::Missing => None,
            Self::Inline(column) => Some(column.value_type),
            Self::Pointer { value_type, .. } => Some(*value_type),
        }
    }

    pub(crate) fn data_len(&self) -> usize {
        match self {
            Self::Missing => 0,
            Self::Inline(column) => column.data().len(),
            Self::Pointer { data, .. } => data.len(),
        }
    }

    pub(crate) fn write_data(&self, dst: &mut impl bytes::BufMut) {
        match self {
            Self::Missing => {}
            Self::Inline(column) => dst.put_slice(column.data()),
            Self::Pointer { data, .. } => dst.put_slice(data),
        }
    }
}

/// Recorder for VLOG entries associated with a memtable. This tracks the file sequence and offsets for
/// values that have been written to the memtable but not yet flushed to a VLOG file, allowing them to be
/// flushed to a VLOG file when the memtable is flushed, and also rolled back if the memtable write is aborted.
#[derive(Clone)]
pub(crate) struct MemtableVlogRecorder {
    file_seq: u32,
    next_offset: u32,
    entries: BTreeMap<u32, (usize, usize)>,
}

impl MemtableVlogRecorder {
    pub(crate) fn new(file_seq: u32) -> Self {
        Self {
            file_seq,
            next_offset: 0,
            entries: BTreeMap::new(),
        }
    }

    pub(crate) fn has_entries(&self) -> bool {
        !self.entries.is_empty()
    }

    pub(crate) fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn file_seq(&self) -> u32 {
        self.file_seq
    }

    pub(crate) fn checkpoint(&self) -> u32 {
        self.next_offset
    }

    pub(crate) fn rollback(&mut self, checkpoint: u32) {
        self.entries.retain(|offset, _| *offset < checkpoint);
        self.next_offset = checkpoint;
    }

    pub(crate) fn append_value(
        &mut self,
        memtable: &mut impl Memtable,
        value: &[u8],
    ) -> Result<VlogPointer> {
        let value_len = u32::try_from(value.len())
            .map_err(|_| Error::IoError(format!("VLOG value too large: {} bytes", value.len())))?;
        let offset = self.next_offset;
        let next_offset = offset
            .checked_add(4)
            .and_then(|v| v.checked_add(value_len))
            .ok_or_else(|| Error::IoError("VLOG offset overflow".to_string()))?;
        let start = memtable.append_blob(value)?;
        self.entries.insert(offset, (start, value.len()));
        self.next_offset = next_offset;
        Ok(VlogPointer::new(self.file_seq, offset))
    }

    pub(crate) fn flush_to_writer(
        &self,
        memtable: &impl Memtable,
        writer: &mut VlogWriter<Box<dyn crate::file::SequentialWriteFile>>,
    ) -> Result<()> {
        for (expected_offset, (start, len)) in &self.entries {
            let payload = memtable.read_blob(*start, *len).ok_or_else(|| {
                Error::IoError(format!(
                    "VLOG recorder payload out of range at {} (len {})",
                    start, len
                ))
            })?;
            let pointer = writer.add_value(payload)?;
            if pointer.file_seq() != self.file_seq || pointer.offset() != *expected_offset {
                return Err(Error::IoError(format!(
                    "VLOG pointer mismatch for seq {}: expected offset {}, got {}",
                    self.file_seq,
                    expected_offset,
                    pointer.offset()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn read_pointer(
        &self,
        memtable: &impl Memtable,
        pointer: VlogPointer,
    ) -> Result<Option<Bytes>> {
        if pointer.file_seq() != self.file_seq {
            return Ok(None);
        }
        let Some((start, len)) = self.entries.get(&pointer.offset()) else {
            return Ok(None);
        };
        let payload = memtable.read_blob(*start, *len).ok_or_else(|| {
            Error::IoError(format!(
                "VLOG recorder payload out of range at {} (len {})",
                start, len
            ))
        })?;
        Ok(Some(Bytes::copy_from_slice(payload)))
    }
}

/// Ensures there is an active MemtableVlogRecorder, creating one if needed
/// by allocating a new file sequence from the VlogStore.
fn ensure_active_recorder<'a>(
    vlog_store: &Arc<VlogStore>,
    recorder: &'a mut Option<MemtableVlogRecorder>,
) -> &'a mut MemtableVlogRecorder {
    recorder.get_or_insert_with(|| MemtableVlogRecorder::new(vlog_store.allocate_file_seq()))
}

/// Rewrites a RefValue for memtable storage, potentially replacing large column values with VLOG pointers.
/// Returns a RewrittenValuePlan if any columns were rewritten, or None if the original value
/// can be stored as-is without rewriting.
pub(crate) fn rewrite_ref_value_for_memtable<'a>(
    value: &'a RefValue<'a>,
    vlog_store: &Arc<VlogStore>,
    memtable: &mut impl Memtable,
    recorder: &mut Option<MemtableVlogRecorder>,
    num_columns: usize,
) -> Result<Option<RewrittenValuePlan<'a>>> {
    let mut changed = false;
    let mut rewritten_columns = Vec::with_capacity(num_columns);
    for column in value.columns().iter().take(num_columns) {
        let rewritten = match column {
            Some(column) => match column.value_type {
                ValueType::Put if vlog_store.should_separate(column.data().len()) => {
                    let recorder = ensure_active_recorder(vlog_store, recorder);
                    let pointer = recorder.append_value(memtable, column.data())?;
                    changed = true;
                    RewrittenColumn::Pointer {
                        value_type: ValueType::PutSeparated,
                        data: pointer.to_bytes(),
                    }
                }
                ValueType::Merge if vlog_store.should_separate(column.data().len()) => {
                    let recorder = ensure_active_recorder(vlog_store, recorder);
                    let pointer = recorder.append_value(memtable, column.data())?;
                    changed = true;
                    RewrittenColumn::Pointer {
                        value_type: ValueType::MergeSeparated,
                        data: pointer.to_bytes(),
                    }
                }
                ValueType::Put | ValueType::Merge | ValueType::Delete => {
                    RewrittenColumn::Inline(*column)
                }
                ValueType::PutSeparated
                | ValueType::MergeSeparated
                | ValueType::MergeSeparatedArray => {
                    return Err(Error::InputError(format!(
                        "User writes must not use separated value types: {:?}",
                        column.value_type
                    )));
                }
            },
            None => RewrittenColumn::Missing,
        };
        rewritten_columns.push(rewritten);
    }
    if rewritten_columns.len() < num_columns {
        rewritten_columns.resize_with(num_columns, || RewrittenColumn::Missing);
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(RewrittenValuePlan::new(
        rewritten_columns,
        value.expired_at(),
    )))
}

/// Encodes a rewritten value according to the RewrittenValuePlan.
/// Same encoding with `encode_value_ref_into`, there is no good reason to have two separate
/// encodings but this keeps the logic separate for now since the rewritten value encoding is only
/// used for memtable writes and can be optimized for that case if needed.
pub(crate) fn encode_rewritten_value(
    plan: &RewrittenValuePlan<'_>,
    num_columns: usize,
    dst: &mut [u8],
) {
    let mut out = dst;
    // Write expiration timestamp (seconds since epoch, 0 if None)
    out.put_u32_le(plan.expired_at().unwrap_or(0));

    // Write bitmap for present columns if more than 1 column
    // (not needed for single column since presence can be inferred)
    if num_columns > 1 {
        let mut bitmap = vec![0u8; num_columns.div_ceil(8)];
        for (idx, column) in plan.columns().iter().take(num_columns).enumerate() {
            if column.is_present() {
                bitmap[idx / 8] |= 1 << (idx % 8);
            }
        }
        out.put_slice(&bitmap);
    }
    // Count present columns to identify the last one
    let present_count = plan
        .columns()
        .iter()
        .take(num_columns)
        .filter(|column| column.is_present())
        .count();

    // Write present columns (skip data_len for the last one)
    let mut present_idx = 0usize;
    for column in plan.columns().iter().take(num_columns) {
        let Some(value_type) = column.value_type() else {
            continue;
        };
        present_idx += 1;
        out.put_u8(value_type.encode_tag());
        // Write data length for all but the last present column
        // (length can be inferred from remaining bytes for the last one)
        if present_idx < present_count {
            out.put_u32_le(column.data_len() as u32);
        }
        column.write_data(&mut out);
    }
}
