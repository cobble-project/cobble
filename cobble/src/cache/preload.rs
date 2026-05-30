use super::{BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock};
use crate::db_status::{DbLifecycle, DbLifecycleState};
use crate::file::{FileManager, RandomAccessFile};
use crate::sst::compression::decode_block_bytes;
use crate::sst::format::Block;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use log::warn;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct BlockCachePreload {
    pub(crate) key: BlockCacheKey,
    pub(crate) size: usize,
}

/// Loads compaction-produced blocks into the local block cache in the background.
///
/// The hot-block flow spans four files:
/// - `sst/iterator.rs` registers scan current/next input block keys and lets
///   compaction input iterators call `ScanHotBlockRegistry::observe_if_hot`.
/// - `sst/writer.rs` converts those observations into output `BlockCacheKey`s.
/// - `compaction/executor.rs` returns those keys in `CompactionResult`.
/// - local and remote completion paths submit the keys here after the new files
///   are registered locally, so remote compaction can warm cache entries too.
pub(crate) struct BlockCachePreloadWorker {
    runtime: Mutex<Option<tokio::runtime::Runtime>>,
    lifecycle: Arc<DbLifecycle>,
    cancelled: Arc<AtomicBool>,
    in_flight: Arc<DashMap<BlockCacheKey, ()>>,
    notifier: Arc<Condvar>,
    notifier_mutex: Arc<Mutex<()>>,
    watcher: Mutex<Option<JoinHandle<()>>>,
}

impl BlockCachePreloadWorker {
    /// Starts the dedicated preload runtime.
    ///
    /// Preload is deliberately kept off caller runtimes because it performs
    /// background file reads. The watcher uses `DbLifecycle` notifications to
    /// cancel queued/running work when the DB enters closing or error state; each
    /// task also checks the lifecycle between files and blocks.
    pub(crate) fn new(lifecycle: Arc<DbLifecycle>) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("cobble-block-cache-preload")
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|err| {
                warn!("failed to start block cache preload runtime: {}", err);
                err
            })
            .ok();
        let cancelled = Arc::new(AtomicBool::new(false));
        let in_flight = Arc::new(DashMap::new());
        let notifier = Arc::new(Condvar::new());
        let notifier_mutex = Arc::new(Mutex::new(()));
        lifecycle.register_error_notifier(&notifier);
        let watcher = {
            let lifecycle = Arc::clone(&lifecycle);
            let cancelled = Arc::clone(&cancelled);
            let notifier = Arc::clone(&notifier);
            let notifier_mutex = Arc::clone(&notifier_mutex);
            std::thread::Builder::new()
                .name("cobble-block-cache-preload-watch".to_string())
                .spawn(move || {
                    let mut guard = notifier_mutex.lock().unwrap();
                    while !cancelled.load(Ordering::Acquire)
                        && !matches!(
                            lifecycle.state(),
                            DbLifecycleState::Closing
                                | DbLifecycleState::Closed
                                | DbLifecycleState::Error
                        )
                    {
                        guard = notifier.wait(guard).unwrap();
                    }
                    cancelled.store(true, Ordering::Release);
                })
                .map_err(|err| {
                    warn!("failed to start block cache preload watcher: {}", err);
                    err
                })
                .ok()
        };
        Self {
            runtime: Mutex::new(runtime),
            lifecycle,
            cancelled,
            in_flight,
            notifier,
            notifier_mutex,
            watcher: Mutex::new(watcher),
        }
    }

    pub(crate) fn submit(
        &self,
        file_manager: Arc<FileManager>,
        block_cache: BlockCache,
        preloads: Vec<BlockCachePreload>,
    ) {
        if preloads.is_empty()
            || self.cancelled.load(Ordering::Acquire)
            || !self.lifecycle.is_open_fast()
        {
            return;
        }
        let handle = self
            .runtime
            .lock()
            .unwrap()
            .as_ref()
            .map(|runtime| runtime.handle().clone());
        let Some(handle) = handle else {
            return;
        };
        let preloads = reserve_block_cache_preloads(preloads, &self.in_flight);
        if preloads.is_empty() {
            return;
        }
        let reservation = BlockCachePreloadReservation::new(
            Arc::clone(&self.in_flight),
            preloads.iter().map(|preload| preload.key).collect(),
        );
        let lifecycle = Arc::clone(&self.lifecycle);
        let cancelled = Arc::clone(&self.cancelled);
        handle.spawn(async move {
            preload_block_cache_keys(
                file_manager,
                block_cache,
                preloads,
                lifecycle,
                cancelled,
                reservation,
            );
        });
    }

    pub(crate) fn shutdown(&self) {
        self.cancelled.store(true, Ordering::Release);
        let guard = self.notifier_mutex.lock().unwrap();
        self.notifier.notify_all();
        drop(guard);
        if let Some(watcher) = self.watcher.lock().unwrap().take() {
            let _ = watcher.join();
        }
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            runtime.shutdown_timeout(Duration::from_millis(0));
        }
    }
}

impl Drop for BlockCachePreloadWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct BlockCachePreloadReservation {
    in_flight: Arc<DashMap<BlockCacheKey, ()>>,
    keys: Vec<BlockCacheKey>,
}

impl BlockCachePreloadReservation {
    fn new(in_flight: Arc<DashMap<BlockCacheKey, ()>>, keys: Vec<BlockCacheKey>) -> Self {
        Self { in_flight, keys }
    }
}

impl Drop for BlockCachePreloadReservation {
    fn drop(&mut self) {
        for key in self.keys.drain(..) {
            self.in_flight.remove(&key);
        }
    }
}

fn reserve_block_cache_preloads(
    preloads: Vec<BlockCachePreload>,
    in_flight: &DashMap<BlockCacheKey, ()>,
) -> Vec<BlockCachePreload> {
    let mut reserved = Vec::new();
    for preload in preloads {
        if !matches!(preload.key.kind, BlockCacheKind::Data) {
            continue;
        }
        match in_flight.entry(preload.key) {
            Entry::Occupied(_) => {}
            Entry::Vacant(entry) => {
                entry.insert(());
                reserved.push(preload);
            }
        }
    }
    reserved
}

fn preload_block_cache_keys(
    file_manager: Arc<FileManager>,
    block_cache: BlockCache,
    preloads: Vec<BlockCachePreload>,
    lifecycle: Arc<DbLifecycle>,
    cancelled: Arc<AtomicBool>,
    _reservation: BlockCachePreloadReservation,
) {
    let mut by_file: std::collections::BTreeMap<u64, Vec<BlockCachePreload>> =
        std::collections::BTreeMap::new();
    for preload in preloads {
        if matches!(preload.key.kind, BlockCacheKind::Data) {
            by_file
                .entry(preload.key.file_id)
                .or_default()
                .push(preload);
        }
    }
    for (file_id, preloads) in by_file {
        if !should_continue_preload(&lifecycle, &cancelled) {
            return;
        }
        let reader = match file_manager.open_data_file_reader(file_id) {
            Ok(reader) => reader,
            Err(err) => {
                warn!(
                    "failed to open data file for block cache preload file_id={}: {}",
                    file_id, err
                );
                continue;
            }
        };
        for preload in preloads {
            if !should_continue_preload(&lifecycle, &cancelled) {
                return;
            }
            let key = preload.key;
            if block_cache.get(&key).is_some() {
                continue;
            }
            match reader
                .read_at(key.block_id as usize, preload.size)
                .and_then(decode_block_bytes)
                .and_then(Block::decode)
            {
                Ok(mut block) => {
                    if !should_continue_preload(&lifecycle, &cancelled) {
                        return;
                    }
                    block.set_block_id(key.block_id as u32);
                    block_cache.insert(key, CachedBlock::Block(Arc::new(block)));
                }
                Err(err) => {
                    warn!(
                        "failed to preload compaction block cache entry file_id={} block_id={}: {}",
                        key.file_id, key.block_id, err
                    );
                }
            }
        }
    }
}

fn should_continue_preload(lifecycle: &DbLifecycle, cancelled: &AtomicBool) -> bool {
    !cancelled.load(Ordering::Acquire) && lifecycle.is_open_fast()
}
