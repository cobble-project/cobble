use bytes::Bytes;
use std::sync::{Arc, Condvar, Mutex};

use crate::data_file::{DataFile, DataFileType};
use crate::db_state::{DbState, DbStateHandle};
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{DeduplicatingIterator, KvIterator};
use crate::lsm::LSMTree;
use crate::memtable::Memtable;
use crate::memtable::hash::HashMemtable;
use crate::sst::{SSTWriter, SSTWriterOptions};
use log::{debug, trace, warn};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

pub(crate) struct MemtableFlushResult {
    pub(crate) data_file: Arc<DataFile>,
    pub(crate) seq: u64,
}

pub(crate) struct MemtableManagerOptions {
    pub(crate) memtable_capacity: usize,
    pub(crate) buffer_count: usize,
    pub(crate) sst_options: SSTWriterOptions,
    pub(crate) file_builder_factory: Option<Arc<FileBuilderFactory>>,
    pub(crate) num_columns: usize,
    pub(crate) write_stall_limit: usize,
}

impl Default for MemtableManagerOptions {
    fn default() -> Self {
        Self {
            memtable_capacity: 1024 * 1024,
            buffer_count: 2,
            sst_options: SSTWriterOptions::default(),
            file_builder_factory: None,
            num_columns: 1,
            write_stall_limit: 8,
        }
    }
}

pub(crate) struct MemtableManager {
    state: Arc<Mutex<MemtableManagerState>>,
    buffer_ready: Arc<Condvar>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
    lsm_tree: Arc<LSMTree>,
    db_state: Arc<DbStateHandle>,
    write_stall_limit: usize,
    flush_tx: Mutex<Option<mpsc::UnboundedSender<FlushJob>>>,
    runtime: Mutex<Option<Runtime>>,
}

struct MemtableManagerState {
    free_buffers: Vec<Vec<u8>>,
    in_flight: usize,
    next_seq: u64,
    #[cfg(test)]
    flush_results: Vec<Result<MemtableFlushResult>>,
}

struct FlushJob {
    seq: u64,
    memtable: Arc<HashMemtable>,
}

pub(crate) struct ActiveMemtable {
    seq: u64,
    memtable: Option<HashMemtable>,
}

#[derive(Clone)]
pub(crate) struct ImmutableMemtable {
    pub(crate) seq: u64,
    memtable: Arc<HashMemtable>,
}

impl MemtableManager {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        lsm_tree: Arc<LSMTree>,
        options: MemtableManagerOptions,
    ) -> Result<Self> {
        if options.buffer_count == 0 {
            return Err(Error::IoError(
                "buffer_count must be greater than 0".to_string(),
            ));
        }
        let mut buffers = Vec::with_capacity(options.buffer_count);
        for _ in 0..options.buffer_count {
            buffers.push(vec![0u8; options.memtable_capacity]);
        }
        let state = MemtableManagerState {
            free_buffers: buffers,
            in_flight: 0,
            next_seq: 0,
            #[cfg(test)]
            flush_results: Vec::new(),
        };
        let state = Arc::new(Mutex::new(state));
        let buffer_ready = Arc::new(Condvar::new());
        let file_builder_factory = options
            .file_builder_factory
            .unwrap_or_else(|| Arc::new(make_sst_builder_factory(options.sst_options.clone())));
        let db_state = lsm_tree.db_state();
        {
            // init the first active buffer
            let mut state = state.lock().unwrap();
            Self::make_active_buffer(&mut state, &db_state);
        }
        // Initialize the flush runtime
        let (runtime, flush_tx) = Self::init_flush_runtime(
            Arc::clone(&state),
            Arc::clone(&buffer_ready),
            Arc::clone(&file_manager),
            Arc::clone(&file_builder_factory),
            options.num_columns,
            Arc::clone(&lsm_tree),
            Arc::clone(&db_state),
        )?;
        Ok(Self {
            state,
            buffer_ready,
            file_manager,
            file_builder_factory,
            num_columns: options.num_columns,
            lsm_tree,
            db_state,
            write_stall_limit: options.write_stall_limit,
            flush_tx: Mutex::new(Some(flush_tx)),
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn db_state(&self) -> Arc<DbStateHandle> {
        Arc::clone(&self.db_state)
    }

    /// Initializes the flush runtime and returns the runtime and flush sender.
    fn init_flush_runtime(
        state: Arc<Mutex<MemtableManagerState>>,
        buffer_ready: Arc<Condvar>,
        file_manager: Arc<FileManager>,
        file_builder_factory: Arc<FileBuilderFactory>,
        num_columns: usize,
        lsm_tree: Arc<LSMTree>,
        db_state: Arc<DbStateHandle>,
    ) -> Result<(tokio::runtime::Runtime, mpsc::UnboundedSender<FlushJob>)> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("cobble-flush")
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| Error::IoError(format!("Failed to create tokio runtime: {}", e)))?;
        let (flush_tx, mut flush_rx) = mpsc::unbounded_channel::<FlushJob>();
        let state_clone = Arc::clone(&state);
        let buffer_ready_clone = Arc::clone(&buffer_ready);
        let file_manager_clone = Arc::clone(&file_manager);
        let file_builder_factory_clone = Arc::clone(&file_builder_factory);
        let lsm_tree_clone = Arc::clone(&lsm_tree);
        let db_state_clone = Arc::clone(&db_state);
        runtime.spawn(async move {
            while let Some(job) = flush_rx.recv().await {
                trace!("memtable flush start seq={}", job.seq);
                let file_manager = Arc::clone(&file_manager_clone);
                let file_builder_factory = Arc::clone(&file_builder_factory_clone);
                let handle = tokio::task::spawn_blocking(move || {
                    flush_memtable(
                        job.seq,
                        job.memtable,
                        file_manager,
                        file_builder_factory,
                        num_columns,
                    )
                });
                let (result, memtable, completed_seq) = match handle.await {
                    Ok(result) => result,
                    Err(err) => (
                        Err(Error::IoError(format!("Flush task failed: {}", err))),
                        Arc::new(HashMemtable::with_buffer(vec![0u8; 8])),
                        job.seq,
                    ),
                };
                let mut state = state_clone.lock().unwrap();
                state.in_flight = state.in_flight.saturating_sub(1);
                let mut reclaim_buffer = false;
                match result {
                    Ok(res) => {
                        debug!(
                            "memtable flush complete seq={} file_id={} size={}",
                            res.seq, res.data_file.file_id, res.data_file.size
                        );
                        lsm_tree_clone.add_level0_files(res.seq, vec![Arc::clone(&res.data_file)]);
                        reclaim_buffer = true;
                        #[cfg(test)]
                        state.flush_results.push(Ok(res));
                    }
                    Err(err) => {
                        warn!("memtable flush failed seq={} err={}", completed_seq, err);
                        #[cfg(test)]
                        state.flush_results.push(Err(err));
                        #[cfg(not(test))]
                        {
                            let _ = err;
                        }
                    }
                }
                if reclaim_buffer {
                    let buffer = match Arc::try_unwrap(memtable) {
                        Ok(memtable) => memtable.into_buffer(),
                        Err(_) => Vec::new(),
                    };
                    if !buffer.is_empty() {
                        state.free_buffers.push(buffer);
                    }
                    Self::make_active_buffer(&mut state, &db_state_clone);
                }
                buffer_ready_clone.notify_all();
            }
        });
        Ok((runtime, flush_tx))
    }

    /// Makes an active memtable from a free buffer if none exists.
    /// Assumes the caller holds the lock on the state.
    fn make_active_buffer(state: &mut MemtableManagerState, db_state: &DbStateHandle) {
        let _guard = db_state.lock();
        let snapshot = db_state.load();
        if snapshot.active.is_none() && !state.free_buffers.is_empty() {
            let buffer = state.free_buffers.pop().expect("free buffer exists");
            let seq = state.next_seq;
            state.next_seq += 1;
            let active = Arc::new(Mutex::new(ActiveMemtable {
                seq,
                memtable: Some(HashMemtable::with_buffer(buffer)),
            }));
            db_state.cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                DbState::new(
                    db_state,
                    snapshot.lsm_version.clone(),
                    Some(Arc::clone(&active)),
                    snapshot.immutables.clone(),
                )
            });
        }
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        loop {
            // Wait for an active memtable to be available.
            if self.db_state.load().active.is_none() {
                let mut state = self.state.lock().unwrap();
                while self.db_state.load().active.is_none() {
                    state = self.buffer_ready.wait(state).unwrap();
                }
                drop(state);
            }
            let active = self
                .db_state
                .load()
                .active
                .clone()
                .expect("active memtable exists");
            let mut active = active.lock().unwrap();
            let memtable = active.memtable.as_mut().expect("active memtable exists");
            match memtable.put(key, value) {
                Ok(()) => return Ok(()),
                Err(Error::MemtableFull { needed, remaining }) => {
                    if memtable.is_empty() {
                        return Err(Error::MemtableFull { needed, remaining });
                    }
                    drop(active);
                    self.flush_active()?;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Gets all values associated with the given key.
    /// The provided closure `f` is called for each value. This allows
    /// processing values without returning byte copy.
    ///
    /// Returns the minimum sequence number among the memtables searched.
    pub(crate) fn get_all<F>(&self, key: &[u8], mut f: F) -> Result<Option<u64>>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let snapshot = self.db_state.load();
        self.get_all_with_snapshot(snapshot, key, f)
    }

    /// Same as [`get_all`] but uses the provided `DbState` snapshot to ensure a consistent view.
    pub(crate) fn get_all_with_snapshot<F>(
        &self,
        snapshot: Arc<DbState>,
        key: &[u8],
        mut f: F,
    ) -> Result<Option<u64>>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut min_seq = u64::MAX;
        if let Some(active) = &snapshot.active {
            let active = active.lock().unwrap();
            let memtable = active.memtable.as_ref().expect("active memtable exists");
            min_seq = active.seq;
            for value in memtable.get_all(key) {
                f(value)?;
            }
            drop(active);
        }
        min_seq = min_seq.min(
            snapshot
                .immutables
                .front()
                .map(|m| m.seq)
                .unwrap_or(min_seq),
        );
        for immutable in snapshot.immutables.iter().rev() {
            for value in immutable.memtable.get_all(key) {
                f(value)?;
            }
        }
        Ok(Some(min_seq))
    }

    pub(crate) fn flush_active(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let mut guard = self.db_state.lock();
        guard = self.wait_for_write_stall_under_guard(guard);
        let snapshot = self.db_state.load();
        let mut to_flush = None;
        self.db_state
            .cas_mutate(snapshot.seq_id, |db_state, snapshot| {
                let active = snapshot.active.clone().expect("active memtable exists");
                let mut inner_active = active.lock().unwrap();
                let active_memtable = inner_active
                    .memtable
                    .take()
                    .expect("active memtable exists");
                let mut immutables = snapshot.immutables.clone();
                let new_immutable = ImmutableMemtable {
                    seq: inner_active.seq,
                    memtable: Arc::new(active_memtable),
                };
                to_flush = Some(new_immutable.clone());
                immutables.push_back(new_immutable);

                DbState::new(db_state, snapshot.lsm_version.clone(), None, immutables)
            });
        drop(guard);
        Self::make_active_buffer(&mut state, &self.db_state);
        state.in_flight += 1;
        drop(state);

        let Some(to_flush) = to_flush else {
            return Ok(());
        };
        self.spawn_flush(to_flush.seq, to_flush.memtable);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn drain_flush_results(&self) -> Vec<Result<MemtableFlushResult>> {
        let mut state = self.state.lock().unwrap();
        state.flush_results.drain(..).collect()
    }

    #[cfg(test)]
    pub(crate) fn wait_for_flushes(&self) -> Vec<Result<MemtableFlushResult>> {
        let mut state = self.state.lock().unwrap();
        while state.in_flight > 0 {
            state = self.buffer_ready.wait(state).unwrap();
        }
        state.flush_results.drain(..).collect()
    }

    pub(crate) fn close(&self) -> Result<()> {
        {
            let mut tx = self.flush_tx.lock().unwrap();
            tx.take();
        }
        let runtime = self.runtime.lock().unwrap().take();
        let Some(runtime) = runtime else {
            return Ok(());
        };
        runtime.shutdown_timeout(std::time::Duration::from_secs(5));
        Ok(())
    }

    fn spawn_flush(&self, seq: u64, memtable: Arc<HashMemtable>) {
        let sender = self.flush_tx.lock().unwrap();
        if let Some(sender) = sender.as_ref()
            && sender.send(FlushJob { seq, memtable }).is_err()
        {
            let mut state = self.state.lock().unwrap();
            Self::make_active_buffer(&mut state, &self.db_state);
            state.in_flight = state.in_flight.saturating_sub(1);
            #[cfg(test)]
            state
                .flush_results
                .push(Err(Error::IoError("Flush worker unavailable".to_string())));
            self.buffer_ready.notify_all();
        } else if sender.is_none() {
            let mut state = self.state.lock().unwrap();
            Self::make_active_buffer(&mut state, &self.db_state);
            state.in_flight = state.in_flight.saturating_sub(1);
            #[cfg(test)]
            state
                .flush_results
                .push(Err(Error::IoError("Memtable manager closed".to_string())));
            self.buffer_ready.notify_all();
        }
    }

    fn wait_for_write_stall_under_guard<'a>(
        &self,
        mut guard: std::sync::MutexGuard<'a, ()>,
    ) -> std::sync::MutexGuard<'a, ()> {
        while Self::should_write_stall_with_snapshot(&self.db_state.load(), self.write_stall_limit)
        {
            guard = self.db_state.wait_for_change(guard);
        }
        guard
    }

    fn should_write_stall_with_snapshot(snapshot: &DbState, write_stall_limit: usize) -> bool {
        let immutables = snapshot.immutables.len();
        let level0 = snapshot
            .lsm_version
            .levels
            .iter()
            .find(|level| level.ordinal == 0)
            .map(|level| level.files.len())
            .unwrap_or(0);
        immutables + level0 > write_stall_limit
    }
}

impl Drop for MemtableManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn flush_memtable(
    seq: u64,
    memtable: Arc<HashMemtable>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
) -> (Result<MemtableFlushResult>, Arc<HashMemtable>, u64) {
    let result = (|| {
        let (file_id, writer) = file_manager.create_data_file()?;
        let mut builder = (file_builder_factory)(Box::new(writer));
        let mut dedup_iter =
            DeduplicatingIterator::new(PrimedIterator::new(memtable.iter()), num_columns);
        dedup_iter.seek_to_first()?;
        while dedup_iter.valid() {
            if let Some((key, value)) = dedup_iter.current()? {
                builder.add(key.as_ref(), value.as_ref())?;
            }
            dedup_iter.next()?;
        }
        let (start_key, end_key, file_size) = builder.finish()?;
        let data_file = DataFile {
            file_type: DataFileType::SSTable,
            start_key,
            end_key,
            file_id,
            size: file_size,
            seq,
        };
        Ok(MemtableFlushResult {
            data_file: Arc::new(data_file),
            seq,
        })
    })();
    (result, memtable, seq)
}

struct PrimedIterator<I: KvIterator> {
    inner: I,
}

impl<I: KvIterator> PrimedIterator<I> {
    fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I: KvIterator> KvIterator for PrimedIterator<I> {
    fn seek(&mut self, target: &[u8]) -> Result<()> {
        self.inner.seek(target)?;
        let _ = self.inner.next()?;
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<()> {
        self.inner.seek_to_first()?;
        let _ = self.inner.next()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        self.inner.next()
    }

    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn key(&self) -> Result<Option<Bytes>> {
        self.inner.key()
    }

    fn value(&self) -> Result<Option<Bytes>> {
        self.inner.value()
    }
}

fn make_sst_builder_factory(options: SSTWriterOptions) -> FileBuilderFactory {
    Box::new(move |writer| {
        Box::new(SSTWriter::new(
            writer,
            SSTWriterOptions {
                block_size: options.block_size,
                buffer_size: options.buffer_size,
                num_columns: options.num_columns,
            },
        )) as Box<dyn FileBuilder>
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{FileManager, FileSystemRegistry};
    use crate::sst::row_codec::{decode_value, encode_value};
    use crate::sst::{SSTIterator, SSTIteratorOptions, SSTWriterOptions};
    use crate::r#type::{Column, Value, ValueType};

    fn cleanup_test_root() {
        let _ = std::fs::remove_dir_all("/tmp/memtable_manager_test");
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_flush_deduplicates() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let file_manager = Arc::new(FileManager::with_defaults(fs).unwrap());
        let lsm_tree = Arc::new(crate::lsm::LSMTree::default());
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions::default(),
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
            },
        )
        .unwrap();

        let num_columns = 1;
        let old = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"old".to_vec()))]),
            num_columns,
        );
        let new = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"new".to_vec()))]),
            num_columns,
        );
        let v1 = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v1".to_vec()))]),
            num_columns,
        );

        manager.put(b"a", &old).unwrap();
        manager.put(b"a", &new).unwrap();
        manager.put(b"b", &v1).unwrap();

        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].as_ref().unwrap().seq, 0);
        let data_file = results[0].as_ref().unwrap().data_file.clone();
        let level0_files = lsm_tree.level_files(0);
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].file_id, data_file.file_id);
        let reader = file_manager
            .open_data_file_reader(data_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::with_file_id(
            Box::new(reader),
            data_file.file_id,
            SSTIteratorOptions::default(),
        )
        .unwrap();
        iter.seek_to_first().unwrap();
        let mut entries = Vec::new();
        while iter.valid() {
            let (key, value) = iter.current().unwrap().unwrap();
            let decoded = decode_value(&value, num_columns).unwrap();
            let raw = decoded
                .columns()
                .get(0)
                .and_then(|col| col.as_ref())
                .map(|col| Bytes::copy_from_slice(col.data()))
                .unwrap_or_else(Bytes::new);
            entries.push((key, raw));
            iter.next().unwrap();
        }
        assert_eq!(
            entries,
            vec![
                (Bytes::from("a"), Bytes::from("new")),
                (Bytes::from("b"), Bytes::from("v1"))
            ]
        );
        cleanup_test_root();
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_memtable_reuses_buffer_after_flush() {
        cleanup_test_root();
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/memtable_manager_test".to_string())
            .unwrap();
        let file_manager = Arc::new(FileManager::with_defaults(fs).unwrap());
        let lsm_tree = Arc::new(crate::lsm::LSMTree::default());
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions::default(),
                file_builder_factory: None,
                num_columns: 1,
                write_stall_limit: 8,
            },
        )
        .unwrap();

        let num_columns = 1;
        let v1 = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v1".to_vec()))]),
            num_columns,
        );
        manager.put(b"k1", &v1).unwrap();
        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 1);
        {
            let state = manager.state.lock().unwrap();
            assert_eq!(state.free_buffers.len(), 1);
        }

        let v2 = encode_value(
            &Value::new(vec![Some(Column::new(ValueType::Put, b"v2".to_vec()))]),
            num_columns,
        );
        manager.put(b"k2", &v2).unwrap();
        manager.flush_active().unwrap();
        let results = manager.wait_for_flushes();
        assert_eq!(results.len(), 1);
        assert_eq!(lsm_tree.level_files(0).len(), 2);
        cleanup_test_root();
    }
}
