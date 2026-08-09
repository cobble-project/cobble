//! Durable, immutable write-ahead-log segments.
//!
//! WAL deliberately bypasses `FileManager`: it is neither an LSM data file nor a snapshot
//! replica, and therefore must not participate in data-file ownership or tiering.

use crate::config::{Config, VolumeUsageKind};
use crate::db_status::DbLifecycle;
use crate::error::{Error, Result};
use crate::file::{
    File, FileManager, FileSystem, FileSystemRegistry, MetadataReader, MetadataWriter,
};
use crate::memtable::{
    decode_vec_entry_stream, encode_vec_entry_stream_entry, encode_vec_entry_stream_ref,
};
use crate::schema::SchemaManager;
use crate::r#type::{RefKey, RefValue};
use bytes::Bytes;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use uuid::Uuid;

const WAL_DIR: &str = "wal";
const WAL_FILE_PREFIX: &str = "WAL-";
const WAL_MAGIC: &[u8; 4] = b"cwl1";
const WAL_SEGMENT_VERSION: u32 = 1;
const DATA_SEGMENT_KIND: u8 = 1;
const TRUNCATION_SEGMENT_KIND: u8 = 2;
const WAL_BUFFER_SIZE_BYTES: usize = 4 * 1024 * 1024;
const DATA_SEGMENT_HEADER_SIZE: usize = 37;

pub(crate) type WalId = u64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct WalTruncationCursor {
    pub(crate) bucket: u16,
    pub(crate) column_family_id: u8,
    pub(crate) key: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum WalSegment {
    Data {
        wal_id: WalId,
        schema_id: u64,
        entry_count: u32,
        entry_bytes: Bytes,
    },
    TruncationCursor {
        wal_id: WalId,
        edits: Vec<WalTruncationCursor>,
    },
}

impl WalSegment {
    pub(crate) fn data(
        wal_id: WalId,
        schema_id: u64,
        entry_count: u32,
        entry_bytes: Bytes,
    ) -> Self {
        Self::Data {
            wal_id,
            schema_id,
            entry_count,
            entry_bytes,
        }
    }

    pub(crate) fn data_from_entries(
        wal_id: WalId,
        schema_id: u64,
        entries: impl IntoIterator<Item = (Bytes, Bytes)>,
    ) -> Self {
        let mut entry_bytes = Vec::new();
        let mut entry_count = 0u32;
        for (key, value) in entries {
            encode_vec_entry_stream_entry(&mut entry_bytes, &key, &value);
            entry_count += 1;
        }
        Self::data(wal_id, schema_id, entry_count, Bytes::from(entry_bytes))
    }

    pub(crate) fn wal_id(&self) -> WalId {
        match self {
            Self::Data { wal_id, .. } | Self::TruncationCursor { wal_id, .. } => *wal_id,
        }
    }

    pub(crate) fn data_entries(&self) -> Result<Option<Vec<(Bytes, Bytes)>>> {
        match self {
            Self::Data { entry_bytes, .. } => decode_vec_entry_stream(entry_bytes).map(Some),
            Self::TruncationCursor { .. } => Ok(None),
        }
    }
}

pub(crate) struct WalStore {
    fs: std::sync::Arc<dyn FileSystem>,
    dir: String,
}

pub(crate) struct WalCompletion {
    result: Mutex<Option<Result<()>>>,
    ready: Condvar,
}

impl WalCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        })
    }

    fn complete(&self, result: Result<()>) {
        let mut current = self.result.lock().unwrap();
        if current.is_none() {
            *current = Some(result);
            self.ready.notify_all();
        }
    }

    fn wait(&self) -> Result<()> {
        let mut current = self.result.lock().unwrap();
        while current.is_none() {
            current = self.ready.wait(current).unwrap();
        }
        current.as_ref().unwrap().clone()
    }
}

struct PendingDataSegment {
    wal_id: WalId,
    schema_id: u64,
    entry_count: u32,
    entry_bytes: Vec<u8>,
    completion: Arc<WalCompletion>,
}

enum PendingSegment {
    Data(PendingDataSegment),
    Truncation {
        wal_id: WalId,
        edits: Vec<WalTruncationCursor>,
        completion: Arc<WalCompletion>,
    },
}

impl PendingSegment {
    fn completion(&self) -> &Arc<WalCompletion> {
        match self {
            Self::Data(segment) => &segment.completion,
            Self::Truncation { completion, .. } => completion,
        }
    }

    fn into_wal_segment(self) -> WalSegment {
        match self {
            Self::Data(segment) => WalSegment::data(
                segment.wal_id,
                segment.schema_id,
                segment.entry_count,
                Bytes::from(segment.entry_bytes),
            ),
            Self::Truncation { wal_id, edits, .. } => {
                WalSegment::TruncationCursor { wal_id, edits }
            }
        }
    }
}

struct WalWriterState {
    next_wal_id: WalId,
    current: Option<PendingDataSegment>,
    pending: VecDeque<PendingSegment>,
    inflight: Option<Arc<WalCompletion>>,
    /// Blocks new append guards while a snapshot captures a durable WAL boundary.
    snapshot_barrier: bool,
    /// Highest successfully published WAL id. This remains monotonic after old files are pruned.
    last_published_wal_id: WalId,
    stopping: bool,
    failed: Option<Error>,
}

/// Append-only WAL buffer with a single short sequencer mutex.
///
/// Callers hold [`WalAppendGuard`] while applying their corresponding mutation to the memtable,
/// then append the same encoded row before releasing the guard. The publisher only sees frozen
/// buffers and writes them in FIFO order.
pub(crate) struct WalWriter {
    store: WalStore,
    file_manager: Arc<FileManager>,
    schema_manager: Arc<SchemaManager>,
    lifecycle: Arc<DbLifecycle>,
    flush_interval: Duration,
    state: Mutex<WalWriterState>,
    work_ready: Condvar,
    barrier_done: Condvar,
    worker: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) struct WalAppendGuard<'a> {
    writer: &'a WalWriter,
    state: MutexGuard<'a, WalWriterState>,
}

/// Holds the write sequencer across snapshot capture. The publisher remains free to persist the
/// frozen prefix, while new mutations wait until this guard is dropped.
pub(crate) struct WalSnapshotBarrier<'a> {
    writer: &'a WalWriter,
    checkpoint_id: WalId,
}

impl WalWriter {
    pub(crate) fn open(
        config: &Config,
        db_id: &str,
        file_manager: Arc<FileManager>,
        schema_manager: Arc<SchemaManager>,
        lifecycle: Arc<DbLifecycle>,
        restored_checkpoint_id: WalId,
    ) -> Result<Arc<Self>> {
        let registry = FileSystemRegistry::new();
        let store = WalStore::open(config, db_id, &registry)?.expect("WAL is enabled");
        let next_wal_id = store
            .list()?
            .into_iter()
            .last()
            .unwrap_or(0)
            .max(restored_checkpoint_id)
            .saturating_add(1);
        let writer = Arc::new(Self {
            store,
            file_manager,
            schema_manager,
            lifecycle,
            flush_interval: Duration::from_millis(config.wal_flush_interval_ms),
            state: Mutex::new(WalWriterState {
                next_wal_id,
                current: None,
                pending: VecDeque::new(),
                inflight: None,
                snapshot_barrier: false,
                last_published_wal_id: restored_checkpoint_id,
                stopping: false,
                failed: None,
            }),
            work_ready: Condvar::new(),
            barrier_done: Condvar::new(),
            worker: Mutex::new(None),
        });
        let worker_writer = Arc::clone(&writer);
        let worker = thread::Builder::new()
            .name(format!("cobble-wal-{db_id}"))
            .spawn(move || worker_writer.publish_loop())
            .map_err(|err| Error::IoError(format!("failed to start WAL publisher: {err}")))?;
        *writer.worker.lock().unwrap() = Some(worker);
        Ok(writer)
    }

    pub(crate) fn lock(&self) -> Result<WalAppendGuard<'_>> {
        let mut state = self.state.lock().unwrap();
        while state.snapshot_barrier && !state.stopping {
            state = self.barrier_done.wait(state).unwrap();
        }
        if let Some(err) = &state.failed {
            return Err(err.clone());
        }
        if state.stopping {
            return Err(Error::InvalidState("WAL writer is stopping".to_string()));
        }
        Ok(WalAppendGuard {
            writer: self,
            state,
        })
    }

    /// Acquires the write sequencer with a buffer that matches `schema_id`.
    /// A schema change cuts and durably publishes the preceding group before accepting rows for
    /// the new schema.
    pub(crate) fn lock_for_schema(&self, schema_id: u64) -> Result<WalAppendGuard<'_>> {
        loop {
            let mut guard = self.lock()?;
            if !guard.needs_schema_flush(schema_id) {
                return Ok(guard);
            }
            let completion = guard.freeze_current();
            drop(guard);
            if let Some(completion) = completion {
                completion.wait()?;
            }
        }
    }

    pub(crate) fn begin_snapshot_barrier(&self) -> Result<WalSnapshotBarrier<'_>> {
        let completion = {
            let mut state = self.state.lock().unwrap();
            while state.snapshot_barrier && !state.stopping {
                state = self.barrier_done.wait(state).unwrap();
            }
            if let Some(err) = &state.failed {
                return Err(err.clone());
            }
            if state.stopping {
                return Err(Error::InvalidState("WAL writer is stopping".to_string()));
            }
            state.snapshot_barrier = true;
            freeze_current(&mut state);
            let completion = state
                .pending
                .back()
                .map(|segment| Arc::clone(segment.completion()))
                .or_else(|| state.inflight.as_ref().map(Arc::clone));
            self.work_ready.notify_all();
            completion
        };
        if let Some(completion) = completion
            && let Err(err) = completion.wait()
        {
            self.end_snapshot_barrier();
            return Err(err);
        }
        let checkpoint_id = {
            let state = self.state.lock().unwrap();
            if let Some(err) = &state.failed {
                let err = err.clone();
                drop(state);
                self.end_snapshot_barrier();
                return Err(err);
            }
            state.last_published_wal_id
        };
        Ok(WalSnapshotBarrier {
            writer: self,
            checkpoint_id,
        })
    }

    pub(crate) fn truncate_through(&self, checkpoint_id: WalId) -> Result<()> {
        self.store.delete_through(checkpoint_id)
    }

    pub(crate) fn close(&self) -> Result<()> {
        {
            let mut state = self.state.lock().unwrap();
            if !state.stopping {
                state.stopping = true;
                freeze_current(&mut state);
                self.work_ready.notify_all();
            }
        }
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
        self.state
            .lock()
            .unwrap()
            .failed
            .as_ref()
            .map_or(Ok(()), |err| Err(err.clone()))
    }

    pub(crate) fn force_close(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.stopping = true;
            let error = Error::CancelledError("WAL writer stopped".to_string());
            if let Some(current) = state.current.take() {
                current.completion.complete(Err(error.clone()));
            }
            if let Some(inflight) = &state.inflight {
                inflight.complete(Err(error.clone()));
            }
            for segment in state.pending.drain(..) {
                segment.completion().complete(Err(error.clone()));
            }
            self.work_ready.notify_all();
        }
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }

    fn publish_loop(self: Arc<Self>) {
        loop {
            let segment = {
                let mut state = self.state.lock().unwrap();
                while state.pending.is_empty() && !state.stopping {
                    let (next, timeout) = self
                        .work_ready
                        .wait_timeout(state, self.flush_interval)
                        .unwrap();
                    state = next;
                    if timeout.timed_out() {
                        freeze_current(&mut state);
                    }
                }
                if state.stopping {
                    freeze_current(&mut state);
                }
                match state.pending.pop_front() {
                    Some(segment) => {
                        state.inflight = Some(Arc::clone(segment.completion()));
                        segment
                    }
                    None if state.stopping => return,
                    None => continue,
                }
            };

            let completion = Arc::clone(segment.completion());
            let segment = segment.into_wal_segment();
            let wal_id = segment.wal_id();
            let result = (|| {
                if let WalSegment::Data { schema_id, .. } = &segment {
                    self.schema_manager
                        .persist_schemas_up_to(self.file_manager.as_ref(), *schema_id)?;
                }
                self.store.publish(&segment)
            })();
            if result.is_ok() {
                self.record_published(wal_id);
            }
            completion.complete(result.clone());
            self.clear_inflight(&completion);
            if let Err(err) = result {
                self.fail(err);
                return;
            }
        }
    }

    fn fail(&self, err: Error) {
        let mut state = self.state.lock().unwrap();
        if state.failed.is_some() {
            return;
        }
        state.failed = Some(err.clone());
        state.stopping = true;
        if let Some(current) = state.current.take() {
            current.completion.complete(Err(err.clone()));
        }
        if let Some(inflight) = state.inflight.take() {
            inflight.complete(Err(err.clone()));
        }
        for segment in state.pending.drain(..) {
            segment.completion().complete(Err(err.clone()));
        }
        self.lifecycle.mark_error(err);
        self.work_ready.notify_all();
    }

    fn clear_inflight(&self, completion: &Arc<WalCompletion>) {
        let mut state = self.state.lock().unwrap();
        if state
            .inflight
            .as_ref()
            .is_some_and(|inflight| Arc::ptr_eq(inflight, completion))
        {
            state.inflight = None;
        }
    }

    fn record_published(&self, wal_id: WalId) {
        let mut state = self.state.lock().unwrap();
        state.last_published_wal_id = state.last_published_wal_id.max(wal_id);
    }

    fn end_snapshot_barrier(&self) {
        let mut state = self.state.lock().unwrap();
        state.snapshot_barrier = false;
        self.barrier_done.notify_all();
    }
}

impl WalSnapshotBarrier<'_> {
    pub(crate) fn checkpoint_id(&self) -> WalId {
        self.checkpoint_id
    }
}

impl Drop for WalSnapshotBarrier<'_> {
    fn drop(&mut self) {
        self.writer.end_snapshot_barrier();
    }
}

impl WalAppendGuard<'_> {
    fn needs_schema_flush(&self, schema_id: u64) -> bool {
        self.state
            .current
            .as_ref()
            .is_some_and(|current| current.schema_id != schema_id)
    }

    fn freeze_current(&mut self) -> Option<Arc<WalCompletion>> {
        let completion = self
            .state
            .current
            .as_ref()
            .map(|current| Arc::clone(&current.completion));
        freeze_current(&mut self.state);
        if completion.is_some() {
            self.writer.work_ready.notify_all();
        }
        completion
    }

    pub(crate) fn append_ref(
        &mut self,
        schema_id: u64,
        key: &RefKey<'_>,
        value: &RefValue<'_>,
        num_columns: usize,
    ) -> Arc<WalCompletion> {
        if self.needs_schema_flush(schema_id) {
            panic!("WAL schema changed without flushing the current buffer");
        }
        if self.state.current.is_none() {
            let wal_id = state_next_wal_id(&mut self.state);
            self.state.current = Some(PendingDataSegment {
                wal_id,
                schema_id,
                entry_count: 0,
                entry_bytes: Vec::with_capacity(WAL_BUFFER_SIZE_BYTES),
                completion: WalCompletion::new(),
            });
        }
        let (completion, should_freeze) = {
            let current = self.state.current.as_mut().unwrap();
            encode_vec_entry_stream_ref(&mut current.entry_bytes, key, value, num_columns);
            current.entry_count += 1;
            (
                Arc::clone(&current.completion),
                current.entry_bytes.len() >= WAL_BUFFER_SIZE_BYTES,
            )
        };
        if should_freeze {
            freeze_current(&mut self.state);
            self.writer.work_ready.notify_all();
        }
        completion
    }

    pub(crate) fn append_truncation(
        &mut self,
        bucket: u16,
        column_family_id: u8,
        key: &[u8],
    ) -> Arc<WalCompletion> {
        freeze_current(&mut self.state);
        let completion = WalCompletion::new();
        let wal_id = state_next_wal_id(&mut self.state);
        self.state.pending.push_back(PendingSegment::Truncation {
            wal_id,
            edits: vec![WalTruncationCursor {
                bucket,
                column_family_id,
                key: Bytes::copy_from_slice(key),
            }],
            completion: Arc::clone(&completion),
        });
        self.writer.work_ready.notify_all();
        completion
    }
}

impl WalCompletion {
    pub(crate) fn wait_result(&self) -> Result<()> {
        self.wait()
    }
}

fn state_next_wal_id(state: &mut WalWriterState) -> WalId {
    let wal_id = state.next_wal_id;
    state.next_wal_id += 1;
    wal_id
}

fn freeze_current(state: &mut WalWriterState) {
    if let Some(current) = state.current.take() {
        state.pending.push_back(PendingSegment::Data(current));
    }
}

impl WalStore {
    pub(crate) fn open(
        config: &Config,
        db_id: &str,
        registry: &FileSystemRegistry,
    ) -> Result<Option<Self>> {
        if !config.wal_enabled {
            return Ok(None);
        }
        config.validate_wal()?;
        let volume = config
            .volumes
            .iter()
            .find(|volume| volume.supports(VolumeUsageKind::Wal))
            .expect("validated WAL volume exists");
        let fs = registry.get_or_register_volume(volume)?;
        let dir = format!("{db_id}/{WAL_DIR}");
        fs.create_dir(&dir)?;
        Ok(Some(Self { fs, dir }))
    }

    pub(crate) fn publish(&self, segment: &WalSegment) -> Result<()> {
        let final_path = self.path_for(segment.wal_id());
        let temp_path = format!("{}/.tmp-{}", self.dir, Uuid::new_v4());
        let publish_result = (|| {
            let writer = self.fs.open_write(&temp_path)?;
            let mut writer = MetadataWriter::new(writer);
            write_segment(&mut writer, segment)?;
            writer.close()?;
            self.fs.rename(&temp_path, &final_path)
        })();
        if publish_result.is_err() {
            let _ = self.fs.delete(&temp_path);
        }
        publish_result
    }

    pub(crate) fn list(&self) -> Result<Vec<WalId>> {
        let mut ids = self
            .fs
            .list(&self.dir)?
            .into_iter()
            .filter_map(|name| parse_wal_file_name(&name))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        Ok(ids)
    }

    pub(crate) fn read(&self, wal_id: WalId) -> Result<WalSegment> {
        let reader = self.fs.open_read(&self.path_for(wal_id))?;
        let payload = MetadataReader::new(reader).read_all()?;
        let segment = decode_segment(payload.as_ref())?;
        if segment.wal_id() != wal_id {
            return Err(Error::InvalidState(format!(
                "WAL file id {wal_id} contains segment {}",
                segment.wal_id()
            )));
        }
        Ok(segment)
    }

    fn delete_through(&self, checkpoint_id: WalId) -> Result<()> {
        for wal_id in self.list()? {
            if wal_id <= checkpoint_id {
                self.fs.delete(&self.path_for(wal_id))?;
            }
        }
        Ok(())
    }

    fn path_for(&self, wal_id: WalId) -> String {
        format!("{}/{}", self.dir, wal_file_name(wal_id))
    }
}

fn wal_file_name(wal_id: WalId) -> String {
    format!("{WAL_FILE_PREFIX}{wal_id:020}")
}

fn parse_wal_file_name(name: &str) -> Option<WalId> {
    name.strip_prefix(WAL_FILE_PREFIX)?.parse().ok()
}

fn write_all(writer: &mut dyn crate::file::SequentialWriteFile, bytes: &[u8]) -> Result<()> {
    let mut written = 0;
    while written < bytes.len() {
        let count = writer.write(&bytes[written..])?;
        if count == 0 {
            return Err(Error::IoError("WAL writer returned zero bytes".to_string()));
        }
        written += count;
    }
    Ok(())
}

fn write_segment(
    writer: &mut dyn crate::file::SequentialWriteFile,
    segment: &WalSegment,
) -> Result<()> {
    match segment {
        WalSegment::Data {
            wal_id,
            schema_id,
            entry_count,
            entry_bytes,
        } => {
            let mut header = [0; DATA_SEGMENT_HEADER_SIZE];
            header[..4].copy_from_slice(WAL_MAGIC);
            header[4..8].copy_from_slice(&WAL_SEGMENT_VERSION.to_le_bytes());
            header[8] = DATA_SEGMENT_KIND;
            header[9..17].copy_from_slice(&wal_id.to_le_bytes());
            header[17..25].copy_from_slice(&schema_id.to_le_bytes());
            header[25..29].copy_from_slice(&entry_count.to_le_bytes());
            header[29..].copy_from_slice(&(entry_bytes.len() as u64).to_le_bytes());
            write_all(writer, &header)?;
            write_all(writer, entry_bytes)?;
        }
        WalSegment::TruncationCursor { wal_id, edits } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(WAL_MAGIC);
            payload.extend_from_slice(&WAL_SEGMENT_VERSION.to_le_bytes());
            payload.push(TRUNCATION_SEGMENT_KIND);
            payload.extend_from_slice(&wal_id.to_le_bytes());
            payload.extend_from_slice(&(edits.len() as u32).to_le_bytes());
            for edit in edits {
                payload.extend_from_slice(&edit.bucket.to_le_bytes());
                payload.push(edit.column_family_id);
                payload.extend_from_slice(&(edit.key.len() as u32).to_le_bytes());
                payload.extend_from_slice(&edit.key);
            }
            write_all(writer, &payload)?;
        }
    }
    Ok(())
}

fn decode_segment(bytes: &[u8]) -> Result<WalSegment> {
    let mut input = SegmentReader::new(bytes);
    if input.take(4)? != WAL_MAGIC {
        return Err(Error::FileFormatError(
            "invalid WAL segment magic".to_string(),
        ));
    }
    if input.u32()? != WAL_SEGMENT_VERSION {
        return Err(Error::FileFormatError(
            "unsupported WAL segment version".to_string(),
        ));
    }
    let kind = input.u8()?;
    let wal_id = input.u64()?;
    let segment = match kind {
        DATA_SEGMENT_KIND => {
            let schema_id = input.u64()?;
            let expected_entry_count = input.u32()? as usize;
            let entry_bytes_len = input.u64()? as usize;
            let entry_bytes = input.bytes(entry_bytes_len)?;
            let entries = decode_vec_entry_stream(entry_bytes)?;
            if entries.len() != expected_entry_count {
                return Err(Error::FileFormatError(
                    "WAL data entry count does not match payload".to_string(),
                ));
            }
            WalSegment::Data {
                wal_id,
                schema_id,
                entry_count: expected_entry_count as u32,
                entry_bytes: Bytes::copy_from_slice(entry_bytes),
            }
        }
        TRUNCATION_SEGMENT_KIND => {
            let count = input.u32()? as usize;
            let mut edits = Vec::with_capacity(count);
            for _ in 0..count {
                let bucket = input.u16()?;
                let column_family_id = input.u8()?;
                let key_len = input.u32()? as usize;
                let key = Bytes::copy_from_slice(input.bytes(key_len)?);
                edits.push(WalTruncationCursor {
                    bucket,
                    column_family_id,
                    key,
                });
            }
            WalSegment::TruncationCursor { wal_id, edits }
        }
        _ => {
            return Err(Error::FileFormatError(format!(
                "unsupported WAL segment kind {kind}"
            )));
        }
    };
    if !input.is_empty() {
        return Err(Error::FileFormatError(
            "unexpected bytes after WAL segment".to_string(),
        ));
    }
    Ok(segment)
}

struct SegmentReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SegmentReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| Error::FileFormatError("WAL segment length overflow".to_string()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::FileFormatError("truncated WAL segment payload".to_string()))?;
        self.offset = end;
        Ok(value)
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.take(len)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/wal.rs"]
mod tests;
