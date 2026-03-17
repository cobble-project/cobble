use super::file_manager::FileId;
use crate::Error;
use crate::file::{DataVolume, FileManager, TrackedFile, TrackedWriter};
use dashmap::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VolumePressure {
    pub(crate) priority_rank: u8,
    pub(crate) used_bytes: u64,
    pub(crate) size_limit: Option<u64>,
}

pub(crate) trait PrimaryOffloadPolicy: Send + Sync {
    fn select_candidate(
        &self,
        candidates: &[(FileId, Arc<TrackedFile>)],
        source: &VolumePressure,
        target: &VolumePressure,
    ) -> Option<FileId>;
}

type OffloadJobFn = dyn Fn(FileId) + Send + Sync + 'static;

#[derive(Default)]
struct OffloadQueueState {
    queued_or_running: HashSet<FileId>,
}

pub(crate) struct OffloadRuntime {
    tx: Arc<Mutex<Option<mpsc::Sender<FileId>>>>,
    worker: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    state: Arc<Mutex<OffloadQueueState>>,
    done: Arc<Condvar>,
    runtime: Arc<Runtime>,
    primary_volume_by_rank: HashMap<u8, Vec<Arc<DataVolume>>>,
}

impl Default for OffloadRuntime {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl OffloadRuntime {
    pub(crate) fn new(data_volumes: &[Arc<DataVolume>]) -> Self {
        let worker_threads = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
            .min(8);
        let runtime = Builder::new_multi_thread()
            .worker_threads(worker_threads)
            .enable_all()
            .build()
            .expect("failed to build offload runtime");
        let mut primary_volume_by_rank = HashMap::new();
        for volume in data_volumes {
            if !volume.supports_primary_data {
                continue;
            }
            let rank = volume.priority.rank();
            primary_volume_by_rank
                .entry(rank)
                .or_insert_with(Vec::new)
                .push(Arc::clone(volume));
        }
        Self {
            tx: Arc::new(Mutex::new(None)),
            worker: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(OffloadQueueState::default())),
            done: Arc::new(Condvar::new()),
            runtime: Arc::new(runtime),
            primary_volume_by_rank,
        }
    }

    pub(crate) fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.primary_volume_by_rank
            .get(&rank)
            .and_then(|volumes| volumes.first())
            .map(Arc::clone)
    }

    pub(crate) fn select_lower_priority_primary_volume(
        &self,
        source_priority_rank: u8,
        base_file_size: u64,
    ) -> Option<Arc<DataVolume>> {
        if source_priority_rank <= 1 {
            return None;
        }
        for rank in (1..source_priority_rank).rev() {
            if let Some(volumes) = self.primary_volume_by_rank.get(&rank) {
                for volume in volumes {
                    if !volume.is_full(base_file_size) {
                        return Some(Arc::clone(volume));
                    }
                }
            }
        }
        (1..source_priority_rank).rev().find_map(|rank| {
            self.primary_volume_by_rank
                .get(&rank)
                .and_then(|volumes| volumes.first())
                .map(Arc::clone)
        })
    }

    fn complete_job(&self, file_id: FileId) {
        let mut state = self.state.lock().unwrap();
        state.queued_or_running.remove(&file_id);
        if state.queued_or_running.is_empty() {
            self.done.notify_all();
        }
    }

    fn start_worker(self: &Arc<Self>, handler: Arc<OffloadJobFn>) -> Result<(), String> {
        let mut tx_guard = self.tx.lock().unwrap();
        if tx_guard.is_some() {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel::<FileId>();
        let runtime = Arc::downgrade(self);
        let copy_runtime = Arc::clone(&self.runtime);
        let worker = std::thread::Builder::new()
            .name("cobble-offload".to_string())
            .spawn(move || {
                while let Ok(file_id) = rx.recv() {
                    let handler = Arc::clone(&handler);
                    copy_runtime.block_on(async move {
                        let _ = tokio::task::spawn_blocking(move || handler(file_id)).await;
                    });
                    let Some(runtime) = runtime.upgrade() else {
                        break;
                    };
                    runtime.complete_job(file_id);
                }
            })
            .map_err(|err| format!("Failed to start offload worker: {}", err))?;
        *tx_guard = Some(tx);
        let mut worker_guard = self.worker.lock().unwrap();
        *worker_guard = Some(worker);
        Ok(())
    }

    pub(crate) fn schedule(
        self: &Arc<Self>,
        file_id: FileId,
        handler: Arc<OffloadJobFn>,
    ) -> Result<bool, String> {
        self.start_worker(handler)?;
        {
            let mut state = self.state.lock().unwrap();
            if !state.queued_or_running.insert(file_id) {
                return Ok(false);
            }
        }
        let tx_guard = self.tx.lock().unwrap();
        let Some(tx) = tx_guard.as_ref() else {
            self.complete_job(file_id);
            return Err("Offload worker unavailable".to_string());
        };
        if tx.send(file_id).is_err() {
            self.complete_job(file_id);
            return Err("Offload worker unavailable".to_string());
        }
        Ok(true)
    }

    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        let guard = self.state.lock().unwrap();
        if guard.queued_or_running.is_empty() {
            return true;
        }
        let (guard, _) = self
            .done
            .wait_timeout_while(guard, timeout, |state| !state.queued_or_running.is_empty())
            .unwrap();
        guard.queued_or_running.is_empty()
    }

    pub(crate) fn stop(&self) {
        let mut tx_guard = self.tx.lock().unwrap();
        tx_guard.take();
        let worker = self.worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
    }
}

#[derive(Default)]
pub(crate) struct LargestFileOffloadPolicy;

impl PrimaryOffloadPolicy for LargestFileOffloadPolicy {
    fn select_candidate(
        &self,
        candidates: &[(FileId, Arc<TrackedFile>)],
        _source: &VolumePressure,
        _target: &VolumePressure,
    ) -> Option<FileId> {
        candidates
            .iter()
            .max_by(|left, right| {
                let left_size = left.1.size_bytes.load(std::sync::atomic::Ordering::SeqCst);
                let right_size = right.1.size_bytes.load(std::sync::atomic::Ordering::SeqCst);
                match left_size.cmp(&right_size) {
                    std::cmp::Ordering::Equal => right.0.cmp(&left.0),
                    ord => ord,
                }
            })
            .map(|candidate| candidate.0)
    }
}

impl FileManager {
    fn create_untracked_data_file_writer_on_volume(
        &self,
        volume: &Arc<DataVolume>,
    ) -> crate::Result<(TrackedWriter, Arc<TrackedFile>)> {
        let tracked = Arc::new(TrackedFile::new(
            self.data_file_path(0),
            Arc::clone(volume.fs()),
            Some(Arc::clone(volume)),
        ));
        let writer = volume.fs().open_write(tracked.path())?;
        Ok((TrackedWriter::new(writer, Arc::clone(&tracked)), tracked))
    }

    pub(crate) fn stop_offload_worker(&self) {
        self.offload_runtime.stop();
    }

    #[cfg(test)]
    fn wait_for_offload_idle(&self, timeout: Duration) -> bool {
        self.offload_runtime.wait_idle(timeout)
    }

    pub(crate) fn schedule_offload_move(self: &Arc<Self>, file_id: FileId) -> crate::Result<bool> {
        let manager = Arc::downgrade(self);
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                let Some(target_volume) =
                    manager.select_lower_priority_target_for_file(scheduled_file_id)
                else {
                    return;
                };
                let _ = manager
                    .offload_file_to_lower_priority_primary(scheduled_file_id, &target_volume);
            }
        });
        self.offload_runtime
            .schedule(file_id, handler)
            .map_err(Error::IoError)
    }

    pub(crate) fn select_offload_candidate(
        &self,
        source_volume: &Arc<DataVolume>,
        target_volume: &Arc<DataVolume>,
        policy: &dyn PrimaryOffloadPolicy,
    ) -> Option<FileId> {
        if !source_volume.supports_primary_data || !target_volume.supports_primary_data {
            return None;
        }
        let candidates: Vec<(FileId, Arc<TrackedFile>)> = self
            .data_files
            .iter()
            .filter_map(|entry| {
                let tracked = entry.value();
                let volume = tracked.volume.as_ref()?;
                if !volume.supports_primary_data || !Arc::ptr_eq(volume, source_volume) {
                    return None;
                }
                let size_bytes = tracked.size_bytes.load(std::sync::atomic::Ordering::SeqCst);
                if size_bytes == 0 {
                    return None;
                }
                Some((*entry.key(), Arc::clone(tracked)))
            })
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let source_pressure = VolumePressure {
            priority_rank: source_volume.priority.rank(),
            used_bytes: source_volume
                .used_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            size_limit: source_volume.size_limit,
        };
        let target_pressure = VolumePressure {
            priority_rank: target_volume.priority.rank(),
            used_bytes: target_volume
                .used_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            size_limit: target_volume.size_limit,
        };
        policy.select_candidate(&candidates, &source_pressure, &target_pressure)
    }

    #[cfg(test)]
    fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.offload_runtime.primary_volume_by_rank(rank)
    }

    fn select_lower_priority_target_for_file(&self, file_id: FileId) -> Option<Arc<DataVolume>> {
        let source_tracked = self.data_files.get(&file_id)?;
        let source_volume = source_tracked.volume.as_ref()?;
        if !source_volume.supports_primary_data {
            return None;
        }
        self.offload_runtime.select_lower_priority_primary_volume(
            source_volume.priority.rank(),
            self.options.base_file_size as u64,
        )
    }

    pub(crate) fn offload_file_to_lower_priority_primary(
        &self,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> crate::Result<bool> {
        let source_tracked = self
            .data_files
            .get(&file_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        let Some(source_volume) = &source_tracked.volume else {
            return Ok(false);
        };
        if !source_volume.supports_primary_data || !target_volume.supports_primary_data {
            return Ok(false);
        }
        if Arc::ptr_eq(source_volume, target_volume) {
            return Ok(false);
        }
        let source_reader = source_tracked.fs().open_read(source_tracked.path())?;
        let (mut writer, new_tracked) =
            self.create_untracked_data_file_writer_on_volume(target_volume)?;
        self.copy_reader_to_tracked_writer(source_reader.as_ref(), &mut writer)?;
        match self.data_files.entry(file_id) {
            Entry::Occupied(mut occupied) => {
                if !Arc::ptr_eq(occupied.get(), &source_tracked) {
                    return Ok(false);
                }
                occupied.insert(new_tracked);
            }
            Entry::Vacant(_) => return Ok(false),
        }
        if let Ok(mut cache) = self.reader_cache.lock() {
            cache.remove(&file_id);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::{
        File, FileSystemRegistry, RandomAccessFile, SequentialWriteFile, test_utils,
    };
    use crate::{Config, MetricsManager, VolumeUsageKind};

    fn pressure(rank: u8) -> VolumePressure {
        VolumePressure {
            priority_rank: rank,
            used_bytes: 1,
            size_limit: Some(2),
        }
    }

    #[test]
    fn largest_file_policy_picks_largest() {
        let policy = LargestFileOffloadPolicy;
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/offload-policy-largest".to_string())
            .unwrap();
        let candidates = vec![
            (
                7,
                Arc::new(TrackedFile::new("a".to_string(), Arc::clone(&fs), None)),
            ),
            (
                3,
                Arc::new(TrackedFile::new("b".to_string(), Arc::clone(&fs), None)),
            ),
        ];
        candidates[0].1.update_size_bytes(128);
        candidates[1].1.update_size_bytes(256);
        assert_eq!(
            policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
            Some(3)
        );
    }

    #[test]
    fn largest_file_policy_handles_empty_candidates() {
        let policy = LargestFileOffloadPolicy;
        assert!(
            policy
                .select_candidate(&[], &pressure(3), &pressure(2))
                .is_none()
        );
    }

    #[test]
    fn largest_file_policy_tie_breaks_by_file_id() {
        let policy = LargestFileOffloadPolicy;
        let registry = FileSystemRegistry::new();
        let fs = registry
            .get_or_register("file:///tmp/offload-policy-tie".to_string())
            .unwrap();
        let candidates = vec![
            (
                12,
                Arc::new(TrackedFile::new("b".to_string(), Arc::clone(&fs), None)),
            ),
            (
                6,
                Arc::new(TrackedFile::new("a".to_string(), Arc::clone(&fs), None)),
            ),
        ];
        candidates[0].1.update_size_bytes(64);
        candidates[1].1.update_size_bytes(64);
        assert_eq!(
            policy.select_candidate(&candidates, &pressure(3), &pressure(2)),
            Some(6)
        );
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_uses_policy() {
        let root = "/tmp/file_manager_offload_policy";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-policy"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (small_id, mut small_writer) = fm.create_data_file().unwrap();
        small_writer.write(&vec![b'a'; 32]).unwrap();
        small_writer.close().unwrap();
        let (large_id, mut large_writer) = fm.create_data_file().unwrap();
        large_writer.write(&vec![b'b'; 128]).unwrap();
        large_writer.close().unwrap();
        let policy = LargestFileOffloadPolicy;
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume, &policy);
        assert_eq!(selected, Some(large_id));
        assert_ne!(small_id, large_id);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_offload_move_is_async_and_keeps_reads_available() {
        let root = "/tmp/file_manager_offload_async";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry.get_or_register(high_url.clone()).unwrap();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    high_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    low_url,
                    vec![VolumeUsageKind::PrimaryDataPriorityLow],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-async"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let payload = vec![b'x'; 8 * 1024 * 1024];
        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&payload).unwrap();
        writer.close().unwrap();

        let old_path = fm.get_data_file_path(file_id).unwrap();
        assert!(high_fs.exists(&old_path).unwrap());
        let old_reader = fm.open_data_file_reader(file_id).unwrap();

        assert!(fm.schedule_offload_move(file_id).unwrap());
        assert!(!fm.schedule_offload_move(file_id).unwrap());
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

        assert_eq!(
            old_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
            &payload[payload.len() - 16..]
        );
        let new_path = fm.get_data_file_path(file_id).unwrap();
        assert_ne!(old_path, new_path);
        assert!(low_fs.exists(&new_path).unwrap());
        let new_reader = fm.open_data_file_reader(file_id).unwrap();
        assert_eq!(
            new_reader.read_at(payload.len() - 16, 16).unwrap().as_ref(),
            &payload[payload.len() - 16..]
        );
        drop(old_reader);
        test_utils::wait_for_file_deletion(&high_fs, &old_path);
        let _ = std::fs::remove_dir_all(root);
    }
}
