use bytes::Bytes;
use std::sync::{Arc, Condvar, Mutex};

use crate::data_file::{DataFile, DataFileType};
use crate::error::{Error, Result};
use crate::file::FileManager;
use crate::format::{FileBuilder, FileBuilderFactory};
use crate::iterator::{DeduplicatingIterator, KvIterator};
use crate::lsm::LSMTree;
use crate::memtable::Memtable;
use crate::memtable::hash::HashMemtable;
use crate::sst::{SSTWriter, SSTWriterOptions};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

pub(crate) struct MemtableFlushResult {
    pub(crate) data_file: Arc<DataFile>,
}

pub(crate) struct MemtableManagerOptions {
    pub(crate) memtable_capacity: usize,
    pub(crate) buffer_count: usize,
    pub(crate) sst_options: SSTWriterOptions,
    pub(crate) file_builder_factory: Option<Arc<FileBuilderFactory>>,
    pub(crate) num_columns: usize,
}

impl Default for MemtableManagerOptions {
    fn default() -> Self {
        Self {
            memtable_capacity: 1024 * 1024,
            buffer_count: 2,
            sst_options: SSTWriterOptions::default(),
            file_builder_factory: None,
            num_columns: 1,
        }
    }
}

pub(crate) struct MemtableManager {
    state: Arc<Mutex<MemtableManagerState>>,
    buffer_ready: Arc<Condvar>,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
    lsm_tree: Arc<Mutex<LSMTree>>,
    flush_tx: Mutex<Option<mpsc::UnboundedSender<FlushJob>>>,
    runtime: Mutex<Option<Runtime>>,
}

struct MemtableManagerState {
    active: Option<HashMemtable>,
    free_buffers: Vec<Vec<u8>>,
    in_flight: usize,
    #[cfg(test)]
    flush_results: Vec<Result<MemtableFlushResult>>,
}

struct FlushJob {
    memtable: HashMemtable,
}

impl MemtableManager {
    pub(crate) fn new(
        file_manager: Arc<FileManager>,
        lsm_tree: Arc<Mutex<LSMTree>>,
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
        let active_buffer = buffers.pop().expect("buffer_count > 0");
        let active = HashMemtable::with_buffer(active_buffer);
        let state = MemtableManagerState {
            active: Some(active),
            free_buffers: buffers,
            in_flight: 0,
            #[cfg(test)]
            flush_results: Vec::new(),
        };
        let state = Arc::new(Mutex::new(state));
        let buffer_ready = Arc::new(Condvar::new());
        let file_builder_factory = options
            .file_builder_factory
            .unwrap_or_else(|| Arc::new(make_sst_builder_factory(options.sst_options.clone())));
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
        let num_columns = options.num_columns;
        let lsm_tree_clone = Arc::clone(&lsm_tree);
        runtime.spawn(async move {
            while let Some(job) = flush_rx.recv().await {
                let file_manager = Arc::clone(&file_manager_clone);
                let file_builder_factory = Arc::clone(&file_builder_factory_clone);
                let (result, buffer) = tokio::task::spawn_blocking(move || {
                    flush_memtable(
                        job.memtable,
                        file_manager,
                        file_builder_factory,
                        num_columns,
                    )
                })
                .await
                .unwrap_or_else(|err| {
                    (
                        Err(Error::IoError(format!("Flush task failed: {}", err))),
                        Vec::new(),
                    )
                });
                let mut state = state_clone.lock().unwrap();
                if !buffer.is_empty() && state.active.is_none() {
                    state.active = Some(HashMemtable::with_buffer(buffer));
                } else if !buffer.is_empty() {
                    state.free_buffers.push(buffer);
                }
                state.in_flight = state.in_flight.saturating_sub(1);
                match result {
                    Ok(Some(res)) => {
                        if let Ok(mut tree) = lsm_tree_clone.lock() {
                            tree.add_level0_files(vec![Arc::clone(&res.data_file)]);
                        }
                        #[cfg(test)]
                        state.flush_results.push(Ok(res));
                    }
                    Ok(None) => {}
                    Err(err) => {
                        #[cfg(test)]
                        state.flush_results.push(Err(err));
                        #[cfg(not(test))]
                        {
                            let _ = err;
                        }
                    }
                }
                buffer_ready_clone.notify_all();
            }
        });
        Ok(Self {
            state,
            buffer_ready,
            file_manager,
            file_builder_factory,
            num_columns: options.num_columns,
            lsm_tree,
            flush_tx: Mutex::new(Some(flush_tx)),
            runtime: Mutex::new(Some(runtime)),
        })
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        loop {
            let mut state = self.state.lock().unwrap();
            while state.active.is_none() {
                state = self.buffer_ready.wait(state).unwrap();
            }
            let active = state.active.as_mut().expect("active memtable exists");
            match active.put(key, value) {
                Ok(()) => return Ok(()),
                Err(Error::MemtableFull { needed, remaining }) => {
                    if active.is_empty() {
                        return Err(Error::MemtableFull { needed, remaining });
                    }
                    let memtable = state.active.take().expect("active memtable exists");
                    state.active = state.free_buffers.pop().map(HashMemtable::with_buffer);
                    state.in_flight += 1;
                    drop(state);
                    self.spawn_flush(memtable);
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub(crate) fn flush_active(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        while state.active.is_none() {
            state = self.buffer_ready.wait(state).unwrap();
        }
        let should_flush = state
            .active
            .as_ref()
            .is_some_and(|memtable| !memtable.is_empty());
        if !should_flush {
            return Ok(());
        }
        let memtable = state.active.take().expect("active memtable exists");
        state.active = state.free_buffers.pop().map(HashMemtable::with_buffer);
        state.in_flight += 1;
        drop(state);
        self.spawn_flush(memtable);
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

    fn spawn_flush(&self, memtable: HashMemtable) {
        let sender = self.flush_tx.lock().unwrap();
        if let Some(sender) = sender.as_ref()
            && sender.send(FlushJob { memtable }).is_err()
        {
            let mut state = self.state.lock().unwrap();
            if state.active.is_none() && !state.free_buffers.is_empty() {
                let buffer = state.free_buffers.pop().expect("free buffer exists");
                state.active = Some(HashMemtable::with_buffer(buffer));
            }
            state.in_flight = state.in_flight.saturating_sub(1);
            #[cfg(test)]
            state
                .flush_results
                .push(Err(Error::IoError("Flush worker unavailable".to_string())));
            self.buffer_ready.notify_all();
        } else if sender.is_none() {
            let mut state = self.state.lock().unwrap();
            if state.active.is_none() && !state.free_buffers.is_empty() {
                let buffer = state.free_buffers.pop().expect("free buffer exists");
                state.active = Some(HashMemtable::with_buffer(buffer));
            }
            state.in_flight = state.in_flight.saturating_sub(1);
            #[cfg(test)]
            state
                .flush_results
                .push(Err(Error::IoError("Memtable manager closed".to_string())));
            self.buffer_ready.notify_all();
        }
    }
}

impl Drop for MemtableManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn flush_memtable(
    memtable: HashMemtable,
    file_manager: Arc<FileManager>,
    file_builder_factory: Arc<FileBuilderFactory>,
    num_columns: usize,
) -> (Result<Option<MemtableFlushResult>>, Vec<u8>) {
    let result = (|| {
        if memtable.is_empty() {
            return Ok(None);
        }
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
        };
        Ok(Some(MemtableFlushResult {
            data_file: Arc::new(data_file),
        }))
    })();
    let buffer = memtable.into_buffer();
    (result, buffer)
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
        let lsm_tree = Arc::new(Mutex::new(crate::lsm::LSMTree::default()));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions::default(),
                file_builder_factory: None,
                num_columns: 1,
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
        let data_file = results[0].as_ref().unwrap().data_file.clone();
        let level0_files = lsm_tree.lock().unwrap().level_files(0);
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].file_id, data_file.file_id);
        let reader = file_manager
            .open_data_file_reader(data_file.file_id)
            .unwrap();
        let mut iter = SSTIterator::new(Box::new(reader), SSTIteratorOptions::default()).unwrap();
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
        let lsm_tree = Arc::new(Mutex::new(crate::lsm::LSMTree::default()));
        let manager = MemtableManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&lsm_tree),
            MemtableManagerOptions {
                memtable_capacity: 256,
                buffer_count: 2,
                sst_options: SSTWriterOptions::default(),
                file_builder_factory: None,
                num_columns: 1,
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
        assert_eq!(lsm_tree.lock().unwrap().level_files(0).len(), 1);
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
        assert_eq!(lsm_tree.lock().unwrap().level_files(0).len(), 2);
        cleanup_test_root();
    }
}
