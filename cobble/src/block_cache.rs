use crate::Config;
use crate::cache::{CacheHandle, FoyerCache};
use crate::config::HybridCacheVolumePlan;
use crate::db_status::{DbLifecycle, DbLifecycleState};
use crate::error::{Error, Result};
use crate::file::{FileManager, RandomAccessFile};
use crate::sst::bloom::BloomFilter;
use crate::sst::compression::decode_block_bytes;
use crate::sst::format::Block;
use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use foyer::{Code, Error as FoyerError};
use log::warn;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{io::Read, io::Write, path::PathBuf};
use url::Url;

#[derive(Clone)]
pub enum CachedBlock {
    Block(Arc<Block>),
    BloomFilter(Arc<BloomFilter>),
    ParquetBlock(Bytes),
}

impl CachedBlock {
    pub fn size_in_bytes(&self) -> usize {
        match self {
            CachedBlock::Block(block) => block.size_in_bytes(),
            CachedBlock::BloomFilter(filter) => filter.size_in_bytes(),
            CachedBlock::ParquetBlock(bytes) => bytes.len(),
        }
    }
}

pub type BlockCache = Arc<dyn CacheHandle<BlockCacheKey, CachedBlock>>;

/// Tracks data blocks currently adjacent to cursor-driven scans.
///
/// Scan iterators keep only their current and next SST data block in this map.
/// Compaction iterators check the same block-cache keys when reading input; if
/// they touch one, the compaction writer can directly seed the output block cache
/// for the block that receives those hot keys. Values are reference counts so
/// overlapping scans do not clear each other's hot blocks when one iterator
/// advances or drops.
pub(crate) struct ScanHotBlockRegistry {
    hot_blocks: DashMap<BlockCacheKey, usize>,
    observed_hot_blocks: AtomicU64,
}

impl ScanHotBlockRegistry {
    pub(crate) fn new() -> Self {
        Self {
            hot_blocks: DashMap::new(),
            observed_hot_blocks: AtomicU64::new(0),
        }
    }

    pub(crate) fn handle(self: &Arc<Self>) -> ScanHotBlockHandle {
        ScanHotBlockHandle {
            registry: Arc::clone(self),
            blocks: Vec::new(),
        }
    }

    pub(crate) fn observe_if_hot(&self, key: BlockCacheKey) {
        if self.hot_blocks.contains_key(&key) {
            self.observed_hot_blocks.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(crate) fn observed_count(&self) -> u64 {
        self.observed_hot_blocks.load(Ordering::Acquire)
    }

    pub(crate) fn from_keys(keys: impl IntoIterator<Item = BlockCacheKey>) -> Arc<Self> {
        let registry = Arc::new(Self::new());
        for key in keys {
            registry.acquire(key);
        }
        registry
    }

    pub(crate) fn snapshot_keys(&self) -> Vec<BlockCacheKey> {
        self.hot_blocks.iter().map(|entry| *entry.key()).collect()
    }

    fn acquire(&self, key: BlockCacheKey) {
        self.hot_blocks
            .entry(key)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    fn release(&self, key: BlockCacheKey) {
        if let Some(mut count) = self.hot_blocks.get_mut(&key)
            && *count > 1
        {
            *count -= 1;
            return;
        }
        self.hot_blocks.remove(&key);
    }
}

pub(crate) struct ScanHotBlockHandle {
    registry: Arc<ScanHotBlockRegistry>,
    blocks: Vec<BlockCacheKey>,
}

impl ScanHotBlockHandle {
    pub(crate) fn replace(&mut self, keys: Vec<BlockCacheKey>) {
        for key in self.blocks.drain(..) {
            self.registry.release(key);
        }
        for key in &keys {
            self.registry.acquire(*key);
        }
        self.blocks = keys;
    }
}

impl Drop for ScanHotBlockHandle {
    fn drop(&mut self) {
        for key in self.blocks.drain(..) {
            self.registry.release(key);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum BlockCacheKind {
    Data,
    IndexPartition,
    IndexTop,
    FilterPartition,
    FilterIndex,
    ParquetData(u32),
}

impl Code for BlockCacheKind {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        match self {
            BlockCacheKind::Data => writer.write_all(&[0u8]).map_err(FoyerError::io_error),
            BlockCacheKind::IndexPartition => {
                writer.write_all(&[1u8]).map_err(FoyerError::io_error)
            }
            BlockCacheKind::IndexTop => writer.write_all(&[2u8]).map_err(FoyerError::io_error),
            BlockCacheKind::FilterPartition => {
                writer.write_all(&[3u8]).map_err(FoyerError::io_error)
            }
            BlockCacheKind::FilterIndex => writer.write_all(&[4u8]).map_err(FoyerError::io_error),
            BlockCacheKind::ParquetData(length) => {
                writer.write_all(&[5u8]).map_err(FoyerError::io_error)?;
                writer
                    .write_all(&length.to_le_bytes())
                    .map_err(FoyerError::io_error)
            }
        }
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut tag = [0u8; 1];
        reader.read_exact(&mut tag).map_err(FoyerError::io_error)?;
        match tag[0] {
            0 => Ok(BlockCacheKind::Data),
            1 => Ok(BlockCacheKind::IndexPartition),
            2 => Ok(BlockCacheKind::IndexTop),
            3 => Ok(BlockCacheKind::FilterPartition),
            4 => Ok(BlockCacheKind::FilterIndex),
            5 => {
                let mut length = [0u8; 4];
                reader
                    .read_exact(&mut length)
                    .map_err(FoyerError::io_error)?;
                Ok(BlockCacheKind::ParquetData(u32::from_le_bytes(length)))
            }
            kind => Err(FoyerError::io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown block cache kind tag: {}", kind),
            ))),
        }
    }

    fn estimated_size(&self) -> usize {
        match self {
            BlockCacheKind::ParquetData(_) => 1 + 4,
            _ => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct BlockCacheKey {
    pub namespace: u64,
    pub file_id: u64,
    pub block_id: u64,
    pub kind: BlockCacheKind,
}

pub(crate) fn bucket_scoped_cache_namespace(base_namespace: u64, bucket: u16) -> u64 {
    base_namespace ^ ((bucket as u64) << 48)
}

pub(crate) fn cache_namespace_for_db_id(db_id: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    db_id.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn data_block_cache_key(
    namespace: u64,
    file_id: u64,
    block_offset: u64,
) -> BlockCacheKey {
    BlockCacheKey {
        namespace,
        file_id,
        block_id: block_offset,
        kind: BlockCacheKind::Data,
    }
}

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
/// - local and remote completion paths call this function after the new files
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

impl Code for BlockCacheKey {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        writer
            .write_all(&self.namespace.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&self.file_id.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer
            .write_all(&self.block_id.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        self.kind.encode(writer)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut namespace = [0u8; 8];
        let mut file_id = [0u8; 8];
        let mut block_id = [0u8; 8];
        reader
            .read_exact(&mut namespace)
            .map_err(FoyerError::io_error)?;
        reader
            .read_exact(&mut file_id)
            .map_err(FoyerError::io_error)?;
        reader
            .read_exact(&mut block_id)
            .map_err(FoyerError::io_error)?;
        let kind = BlockCacheKind::decode(reader)?;
        Ok(Self {
            namespace: u64::from_le_bytes(namespace),
            file_id: u64::from_le_bytes(file_id),
            block_id: u64::from_le_bytes(block_id),
            kind,
        })
    }

    fn estimated_size(&self) -> usize {
        8 + 8 + 8 + self.kind.estimated_size()
    }
}

impl Code for CachedBlock {
    fn encode(&self, writer: &mut impl Write) -> foyer::Result<()> {
        let (tag, payload) = match self {
            CachedBlock::Block(block) => (0u8, block.encode()),
            CachedBlock::BloomFilter(filter) => (1u8, filter.encode()),
            CachedBlock::ParquetBlock(bytes) => (2u8, bytes.clone()),
        };
        writer.write_all(&[tag]).map_err(FoyerError::io_error)?;
        let len = payload.len() as u32;
        writer
            .write_all(&len.to_le_bytes())
            .map_err(FoyerError::io_error)?;
        writer.write_all(&payload).map_err(FoyerError::io_error)
    }

    fn decode(reader: &mut impl Read) -> foyer::Result<Self>
    where
        Self: Sized,
    {
        let mut tag = [0u8; 1];
        let mut len = [0u8; 4];
        reader.read_exact(&mut tag).map_err(FoyerError::io_error)?;
        reader.read_exact(&mut len).map_err(FoyerError::io_error)?;
        let len = u32::from_le_bytes(len) as usize;
        let mut data = vec![0u8; len];
        reader.read_exact(&mut data).map_err(FoyerError::io_error)?;
        let payload = Bytes::from(data);
        match tag[0] {
            0 => {
                let block = Block::decode(payload).map_err(|err| {
                    FoyerError::io_error(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))
                })?;
                Ok(CachedBlock::Block(Arc::new(block)))
            }
            1 => {
                let filter = BloomFilter::decode(payload).map_err(|err| {
                    FoyerError::io_error(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        err.to_string(),
                    ))
                })?;
                Ok(CachedBlock::BloomFilter(Arc::new(filter)))
            }
            2 => Ok(CachedBlock::ParquetBlock(payload)),
            kind => Err(FoyerError::io_error(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown cached block tag: {}", kind),
            ))),
        }
    }

    fn estimated_size(&self) -> usize {
        1 + 4 + self.size_in_bytes()
    }
}

pub fn new_block_cache(capacity: usize) -> BlockCache {
    Arc::new(FoyerCache::new(capacity, |_, v: &CachedBlock| {
        v.size_in_bytes()
    }))
}

pub(crate) fn new_block_cache_with_config(
    config: &Config,
    db_id: &str,
    memory_capacity: usize,
    selected_plan: Option<&HybridCacheVolumePlan>,
) -> Result<BlockCache> {
    if memory_capacity == 0 {
        return Err(Error::ConfigError(
            "block cache size must be greater than 0".to_string(),
        ));
    }
    let plan = if let Some(plan) = selected_plan {
        Some(plan.clone())
    } else {
        config.resolve_hybrid_cache_volume_plan(memory_capacity)?
    };
    let Some(plan) = plan else {
        return Ok(new_block_cache(memory_capacity));
    };
    let cache_dir = build_hybrid_cache_dir(&plan.base_dir, db_id)?;
    let hybrid = FoyerCache::new_hybrid(
        memory_capacity,
        plan.disk_capacity_bytes,
        cache_dir,
        |_, value: &CachedBlock| value.size_in_bytes(),
    )?;
    Ok(Arc::new(hybrid))
}

fn build_hybrid_cache_dir(base_dir: &str, db_id: &str) -> Result<PathBuf> {
    let url = Url::parse(base_dir).map_err(|err| {
        Error::ConfigError(format!("Invalid cache volume URL {}: {}", base_dir, err))
    })?;
    if !url.scheme().eq_ignore_ascii_case("file") {
        return Err(Error::ConfigError(format!(
            "Hybrid cache requires a local file:// volume, got {}",
            base_dir
        )));
    }
    let mut path = url.to_file_path().map_err(|_| {
        Error::ConfigError(format!("Invalid file URL for cache volume: {}", base_dir))
    })?;
    path.push(db_id);
    path.push("cache");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::block_cache::{
        BlockCache, BlockCacheKey, BlockCacheKind, CachedBlock, ScanHotBlockRegistry,
    };
    use crate::cache::MockCache;
    use crate::file::FileSystemRegistry;
    use crate::sst::iterator::{SSTIterator, SSTIteratorOptions};
    use crate::sst::writer::{SSTWriter, SSTWriterOptions};
    use bytes::Bytes;
    use foyer::Code;

    #[test]
    #[serial_test::serial(file)]
    fn test_block_cache_used_on_seek() {
        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/cache_it_test")
            .unwrap();

        {
            let writer_file = fs.open_write("cached.sst").unwrap();
            let mut writer = SSTWriter::new(
                writer_file,
                SSTWriterOptions {
                    metrics: None,
                    block_size: 64,
                    buffer_size: 8192,
                    num_columns: 1,
                    bloom_filter_enabled: true,
                    bloom_bits_per_key: 10,
                    partitioned_index: false,
                    data_block_restart_interval: 16,
                    compression: crate::SstCompressionAlgorithm::None,
                    value_has_ttl: true,
                },
            );

            for i in 0..10 {
                let key = format!("key{:03}", i);
                let value = format!("value{:03}_with_padding", i);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }
            writer.finish().unwrap();
        }

        let reader_file = fs.open_read("cached.sst").unwrap();
        let mock_cache = Arc::new(MockCache::<BlockCacheKey, CachedBlock>::default());
        let cache: BlockCache = mock_cache.clone();
        let hot_blocks = Arc::new(ScanHotBlockRegistry::new());
        let mut iter = SSTIterator::with_cache_test(
            reader_file,
            7,
            SSTIteratorOptions {
                metrics: None,
                block_cache_size: 0,
                num_columns: 1,
                bloom_filter_enabled: true,
                cache_namespace: 0,
                preload_next_data_block: true,
                hot_block_registry: Some(hot_blocks),
                ..SSTIteratorOptions::default()
            },
            cache,
        )
        .unwrap();

        iter.seek(b"key005").unwrap();
        assert!(mock_cache.get_count() > 0);
        assert!(mock_cache.insert_count() >= 3);
        assert!(iter.valid());

        iter.seek(b"key006").unwrap();
        assert!(mock_cache.get_count() > 0);

        let _ = std::fs::remove_dir_all("/tmp/cache_it_test");
    }

    #[test]
    fn test_parquet_cache_key_and_value_codec() {
        let key = BlockCacheKey {
            namespace: 0,
            file_id: 42,
            block_id: 1024,
            kind: BlockCacheKind::ParquetData(4096),
        };
        let block = CachedBlock::ParquetBlock(Bytes::from_static(b"parquet-page"));

        let mut key_buf = Vec::new();
        key.encode(&mut key_buf).unwrap();
        let mut key_read = key_buf.as_slice();
        let decoded_key = BlockCacheKey::decode(&mut key_read).unwrap();
        assert_eq!(decoded_key, key);

        let mut block_buf = Vec::new();
        block.encode(&mut block_buf).unwrap();
        let mut block_read = block_buf.as_slice();
        let decoded_block = CachedBlock::decode(&mut block_read).unwrap();
        match decoded_block {
            CachedBlock::ParquetBlock(bytes) => assert_eq!(bytes.as_ref(), b"parquet-page"),
            _ => panic!("expected parquet block"),
        }
    }
}
