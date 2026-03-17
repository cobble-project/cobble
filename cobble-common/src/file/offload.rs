use super::file_manager::FileId;
use crate::Error;
use crate::file::{DataVolume, FileManager, TrackedFile, TrackedWriter};
use dashmap::{DashMap, Entry};
use log::warn;
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

#[derive(Clone)]
struct OffloadJobPlan {
    source_volume: Arc<DataVolume>,
    estimated_bytes: u64,
}

struct OffloadQueuedJob {
    file_id: FileId,
    handler: Arc<OffloadJobFn>,
}

pub(crate) struct OffloadRuntime {
    tx: Arc<Mutex<Option<mpsc::Sender<OffloadQueuedJob>>>>,
    worker: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    planned_jobs: Arc<DashMap<FileId, OffloadJobPlan>>,
    idle: Arc<Mutex<()>>,
    done: Arc<Condvar>,
    runtime: Arc<Runtime>,
    primary_volume_by_rank: HashMap<u8, Vec<Arc<DataVolume>>>,
    policy: Arc<dyn PrimaryOffloadPolicy>,
}

impl Default for OffloadRuntime {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl OffloadRuntime {
    pub(crate) fn new(data_volumes: &[Arc<DataVolume>]) -> Self {
        Self::new_with_policy(data_volumes, Arc::new(LargestFileOffloadPolicy))
    }

    fn new_with_policy(
        data_volumes: &[Arc<DataVolume>],
        policy: Arc<dyn PrimaryOffloadPolicy>,
    ) -> Self {
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
            planned_jobs: Arc::new(DashMap::new()),
            idle: Arc::new(Mutex::new(())),
            done: Arc::new(Condvar::new()),
            runtime: Arc::new(runtime),
            primary_volume_by_rank,
            policy,
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
        write_stop_watermark: f64,
    ) -> Option<Arc<DataVolume>> {
        if source_priority_rank <= 1 {
            return None;
        }
        for rank in (1..source_priority_rank).rev() {
            if let Some(volumes) = self.primary_volume_by_rank.get(&rank) {
                for volume in volumes {
                    if !volume.is_write_stopped(write_stop_watermark) {
                        return Some(Arc::clone(volume));
                    }
                }
            }
        }
        None
    }

    pub(crate) fn select_candidate(
        &self,
        candidates: &[(FileId, Arc<TrackedFile>)],
        source: &VolumePressure,
        target: &VolumePressure,
    ) -> Option<FileId> {
        self.policy.select_candidate(candidates, source, target)
    }

    fn complete_job(&self, file_id: FileId) {
        if let Some((_, plan)) = self.planned_jobs.remove(&file_id) {
            plan.source_volume
                .subtract_projected_offload_bytes(plan.estimated_bytes);
        }
        if self.planned_jobs.is_empty() {
            self.done.notify_all();
        }
    }

    fn is_queued_or_running(&self, file_id: FileId) -> bool {
        self.planned_jobs.contains_key(&file_id)
    }

    fn projected_source_offload_bytes(&self, source_volume: &Arc<DataVolume>) -> u64 {
        source_volume.projected_offload_bytes()
    }

    fn start_worker(self: &Arc<Self>) -> Result<(), String> {
        let mut tx_guard = self.tx.lock().unwrap();
        if tx_guard.is_some() {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel::<OffloadQueuedJob>();
        let runtime = Arc::downgrade(self);
        let copy_runtime = Arc::clone(&self.runtime);
        let worker = std::thread::Builder::new()
            .name("cobble-offload".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    copy_runtime.block_on(async move {
                        let file_id = job.file_id;
                        let handler = Arc::clone(&job.handler);
                        let _ = tokio::task::spawn_blocking(move || handler(file_id)).await;
                    });
                    let Some(runtime) = runtime.upgrade() else {
                        break;
                    };
                    runtime.complete_job(job.file_id);
                }
            })
            .map_err(|err| format!("Failed to start offload worker: {}", err))?;
        *tx_guard = Some(tx);
        let mut worker_guard = self.worker.lock().unwrap();
        *worker_guard = Some(worker);
        Ok(())
    }

    fn schedule(
        self: &Arc<Self>,
        file_id: FileId,
        plan: OffloadJobPlan,
        handler: Arc<OffloadJobFn>,
    ) -> Result<bool, String> {
        self.start_worker()?;
        match self.planned_jobs.entry(file_id) {
            Entry::Occupied(_) => {
                return Ok(false);
            }
            Entry::Vacant(vacant) => {
                plan.source_volume
                    .add_projected_offload_bytes(plan.estimated_bytes);
                vacant.insert(plan);
            }
        }
        let tx_guard = self.tx.lock().unwrap();
        let Some(tx) = tx_guard.as_ref() else {
            self.complete_job(file_id);
            return Err("Offload worker unavailable".to_string());
        };
        if tx.send(OffloadQueuedJob { file_id, handler }).is_err() {
            self.complete_job(file_id);
            return Err("Offload worker unavailable".to_string());
        }
        Ok(true)
    }

    pub(crate) fn wait_idle(&self, timeout: Duration) -> bool {
        if self.planned_jobs.is_empty() {
            return true;
        }
        let guard = self.idle.lock().unwrap();
        let (guard, _) = self
            .done
            .wait_timeout_while(guard, timeout, |_| !self.planned_jobs.is_empty())
            .unwrap();
        drop(guard);
        self.planned_jobs.is_empty()
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

    fn should_trigger_offload_on_volume(&self, volume: &Arc<DataVolume>) -> bool {
        volume
            .usage_ratio()
            .map(|ratio| ratio >= self.options.primary_volume_offload_trigger_watermark)
            .unwrap_or(false)
    }

    pub(crate) fn trigger_offload_if_needed(self: &Arc<Self>) -> crate::Result<usize> {
        let mut scheduled = 0usize;
        for source_volume in &self.data_volumes {
            if !source_volume.supports_primary_data {
                continue;
            }
            if !self.should_trigger_offload_on_volume(source_volume) {
                continue;
            }
            let Some(size_limit) = source_volume.size_limit else {
                continue;
            };
            let source_rank = source_volume.priority.rank();
            let trigger_used_bytes = (size_limit as f64
                * self.options.primary_volume_offload_trigger_watermark)
                .ceil() as u64;
            let already_planned = self
                .offload_runtime
                .projected_source_offload_bytes(source_volume);
            let mut projected_used = source_volume
                .used_bytes
                .load(std::sync::atomic::Ordering::SeqCst)
                .saturating_sub(already_planned);
            let mut attempted = HashSet::new();
            while projected_used > trigger_used_bytes {
                let Some(target_volume) =
                    self.offload_runtime.select_lower_priority_primary_volume(
                        source_rank,
                        self.options.primary_volume_write_stop_watermark,
                    )
                else {
                    break;
                };
                if Arc::ptr_eq(source_volume, &target_volume) {
                    break;
                }
                let Some(file_id) = self.select_offload_candidate_with_exclusions(
                    source_volume,
                    &target_volume,
                    &attempted,
                ) else {
                    break;
                };
                attempted.insert(file_id);
                let estimated_bytes = self
                    .data_files
                    .get(&file_id)
                    .map(|entry| {
                        entry
                            .value()
                            .size_bytes
                            .load(std::sync::atomic::Ordering::SeqCst)
                    })
                    .unwrap_or(0);
                if estimated_bytes == 0 {
                    break;
                }
                if self.schedule_offload_move(file_id, &target_volume)? {
                    scheduled += 1;
                    projected_used = projected_used.saturating_sub(estimated_bytes);
                    continue;
                }
                continue;
            }
        }
        Ok(scheduled)
    }

    pub(crate) fn create_data_file_with_offload(
        self: &Arc<Self>,
    ) -> crate::Result<(FileId, TrackedWriter)> {
        let _ = self.trigger_offload_if_needed()?;
        self.create_data_file()
    }

    pub(crate) fn schedule_offload_move(
        self: &Arc<Self>,
        file_id: FileId,
        target_volume: &Arc<DataVolume>,
    ) -> crate::Result<bool> {
        let target_volume = Arc::clone(target_volume);
        let source_volume = self
            .data_files
            .get(&file_id)
            .and_then(|entry| entry.value().volume.as_ref().map(Arc::clone))
            .ok_or_else(|| Error::IoError(format!("Data file {} is not tracked", file_id)))?;
        let estimated_bytes = self
            .data_files
            .get(&file_id)
            .map(|entry| {
                entry
                    .value()
                    .size_bytes
                    .load(std::sync::atomic::Ordering::SeqCst)
            })
            .unwrap_or(0);
        if estimated_bytes == 0 {
            return Ok(false);
        }
        let manager = Arc::downgrade(self);
        let handler = Arc::new(move |scheduled_file_id| {
            if let Some(manager) = manager.upgrade() {
                match manager
                    .offload_file_to_lower_priority_primary(scheduled_file_id, &target_volume)
                {
                    Ok(true) => {}
                    Ok(false) => manager.record_offload_noop(),
                    Err(err) => {
                        manager.record_offload_failed();
                        warn!(
                            "offload move failed for file_id={} target_rank={}: {}",
                            scheduled_file_id,
                            target_volume.priority.rank(),
                            err
                        );
                    }
                }
            }
        });
        let scheduled = self
            .offload_runtime
            .schedule(
                file_id,
                OffloadJobPlan {
                    source_volume,
                    estimated_bytes,
                },
                handler,
            )
            .map_err(Error::IoError)?;
        if scheduled {
            self.record_offload_scheduled();
        }
        Ok(scheduled)
    }

    pub(crate) fn select_offload_candidate(
        &self,
        source_volume: &Arc<DataVolume>,
        target_volume: &Arc<DataVolume>,
    ) -> Option<FileId> {
        let excluded = HashSet::new();
        self.select_offload_candidate_with_exclusions(source_volume, target_volume, &excluded)
    }

    fn select_offload_candidate_with_exclusions(
        &self,
        source_volume: &Arc<DataVolume>,
        target_volume: &Arc<DataVolume>,
        excluded_file_ids: &HashSet<FileId>,
    ) -> Option<FileId> {
        if !source_volume.supports_primary_data || !target_volume.supports_primary_data {
            return None;
        }
        let candidates: Vec<(FileId, Arc<TrackedFile>)> = self
            .data_files
            .iter()
            .filter_map(|entry| {
                if excluded_file_ids.contains(entry.key()) {
                    return None;
                }
                let tracked = entry.value();
                let volume = tracked.volume.as_ref()?;
                if !volume.supports_primary_data || !Arc::ptr_eq(volume, source_volume) {
                    return None;
                }
                let explicit_refs = tracked
                    .explicit_refs
                    .load(std::sync::atomic::Ordering::SeqCst);
                if explicit_refs != 0 && !tracked.is_marked_for_deletion() {
                    return None;
                }
                if self.offload_runtime.is_queued_or_running(*entry.key()) {
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
        self.offload_runtime
            .select_candidate(&candidates, &source_pressure, &target_pressure)
    }

    #[cfg(test)]
    fn primary_volume_by_rank(&self, rank: u8) -> Option<Arc<DataVolume>> {
        self.offload_runtime.primary_volume_by_rank(rank)
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
        if let Some(snapshot_replica_file_id) = self.snapshot_replica_hint_file_id(file_id)
            && let Some(snapshot_tracked) = self
                .data_files
                .get(&snapshot_replica_file_id)
                .map(|entry| Arc::clone(entry.value()))
            && let Some(snapshot_volume) = &snapshot_tracked.volume
            && snapshot_volume.supports_primary_data
            && Arc::ptr_eq(snapshot_volume, target_volume)
        {
            match self.data_files.entry(file_id) {
                Entry::Occupied(mut occupied) => {
                    if !Arc::ptr_eq(occupied.get(), &source_tracked) {
                        return Ok(false);
                    }
                    occupied.insert(snapshot_tracked);
                }
                Entry::Vacant(_) => return Ok(false),
            }
            if let Ok(mut cache) = self.reader_cache.lock() {
                cache.remove(&file_id);
            }
            self.record_offload_completed_promotion();
            return Ok(true);
        }
        let copied_bytes = source_tracked
            .size_bytes
            .load(std::sync::atomic::Ordering::SeqCst);
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
        self.record_offload_completed_copy(copied_bytes);
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
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(large_id));
        assert_ne!(small_id, large_id);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_select_offload_candidate_skips_snapshot_replica_files() {
        let root = "/tmp/file_manager_offload_skip_snapshot_replica";
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
        let metrics_manager = Arc::new(MetricsManager::new(
            "file-manager-offload-skip-snapshot-replica",
        ));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();
        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(&vec![b's'; 64]).unwrap();
        source_writer.close().unwrap();
        let (snapshot_replica_id, mut snapshot_writer) = fm.create_data_file().unwrap();
        snapshot_writer.write(&vec![b'r'; 1024]).unwrap();
        snapshot_writer.close().unwrap();
        fm.make_data_file_readonly(snapshot_replica_id).unwrap();
        let snapshot_ref = fm.data_file_ref(snapshot_replica_id).unwrap();
        fm.register_snapshot_replica_hint(source_file_id, snapshot_replica_id);
        let source_volume = fm.primary_volume_by_rank(3).unwrap();
        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let selected = fm.select_offload_candidate(&source_volume, &target_volume);
        assert_eq!(selected, Some(source_file_id));
        drop(snapshot_ref);
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
        let target_volume = fm.primary_volume_by_rank(1).unwrap();

        assert!(fm.schedule_offload_move(file_id, &target_volume).unwrap());
        assert!(!fm.schedule_offload_move(file_id, &target_volume).unwrap());
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

    #[test]
    #[serial_test::serial(file)]
    fn test_offload_promotes_existing_snapshot_replica_on_target_volume() {
        let root = "/tmp/file_manager_offload_promote_snapshot_replica";
        let _ = std::fs::remove_dir_all(root);
        let config = Config {
            volumes: vec![
                crate::VolumeDescriptor::new(
                    format!("file://{}/high", root),
                    vec![VolumeUsageKind::PrimaryDataPriorityHigh],
                ),
                crate::VolumeDescriptor::new(
                    format!("file://{}/low", root),
                    vec![
                        VolumeUsageKind::PrimaryDataPriorityLow,
                        VolumeUsageKind::Snapshot,
                    ],
                ),
            ],
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new(
            "file-manager-offload-promote-snapshot-replica",
        ));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (source_file_id, mut source_writer) = fm.create_data_file().unwrap();
        source_writer.write(&vec![b'x'; 512]).unwrap();
        source_writer.close().unwrap();
        let source_path = fm.get_data_file_path(source_file_id).unwrap();

        let (snapshot_replica_file_id, copied) = fm
            .copy_data_file_to_snapshot_volume_with_result(source_file_id, None)
            .unwrap();
        assert!(copied);
        fm.register_snapshot_replica_hint(source_file_id, snapshot_replica_file_id);
        let snapshot_replica_path = fm.get_data_file_path(snapshot_replica_file_id).unwrap();

        let target_volume = fm.primary_volume_by_rank(1).unwrap();
        let promoted = fm
            .offload_file_to_lower_priority_primary(source_file_id, &target_volume)
            .unwrap();
        assert!(promoted);
        assert_eq!(
            fm.get_data_file_path(source_file_id).unwrap(),
            snapshot_replica_path
        );
        assert_ne!(source_path, snapshot_replica_path);
        assert_eq!(
            fm.snapshot_replica_hint_file_id(source_file_id),
            Some(snapshot_replica_file_id)
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_create_data_file_with_offload_triggers_background_offload() {
        let root = "/tmp/file_manager_offload_trigger_watermark";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let high_fs = registry.get_or_register(high_url.clone()).unwrap();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(1024);
        let low =
            crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
        let config = Config {
            volumes: vec![high, low],
            base_file_size: 64,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.5,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-watermark"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());

        let (file_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 600]).unwrap();
        writer.close().unwrap();
        let old_path = fm.get_data_file_path(file_id).unwrap();
        assert!(high_fs.exists(&old_path).unwrap());

        let (_new_id, mut new_writer) = fm.create_data_file_with_offload().unwrap();
        new_writer.write(b"small").unwrap();
        new_writer.close().unwrap();

        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
        let new_path = fm.get_data_file_path(file_id).unwrap();
        assert_ne!(new_path, old_path);
        assert!(low_fs.exists(&new_path).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_trigger_offload_loops_until_projected_watermark_recovers() {
        let root = "/tmp/file_manager_offload_loop_trigger";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let low_url = format!("file://{}/low", root);
        let registry = FileSystemRegistry::new();
        let low_fs = registry.get_or_register(low_url.clone()).unwrap();
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(1200);
        let low =
            crate::VolumeDescriptor::new(low_url, vec![VolumeUsageKind::PrimaryDataPriorityLow]);
        let config = Config {
            volumes: vec![high, low],
            base_file_size: 64,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.4,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-loop-trigger"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        let mut file_ids = Vec::new();
        for _ in 0..3 {
            let (file_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; 300]).unwrap();
            writer.close().unwrap();
            file_ids.push(file_id);
        }

        let scheduled = fm.trigger_offload_if_needed().unwrap();
        assert_eq!(scheduled, 2);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));

        let moved_to_low = file_ids
            .iter()
            .filter(|file_id| {
                let path = fm.get_data_file_path(**file_id).unwrap();
                low_fs.exists(&path).unwrap_or(false)
            })
            .count();
        assert!(moved_to_low >= 2);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_trigger_offload_uses_planned_bytes_to_avoid_overscheduling() {
        let root = "/tmp/file_manager_offload_planned_backpressure";
        let _ = std::fs::remove_dir_all(root);
        let mut high = crate::VolumeDescriptor::new(
            format!("file://{}/high", root),
            vec![VolumeUsageKind::PrimaryDataPriorityHigh],
        );
        high.size_limit = Some(1024);
        let low = crate::VolumeDescriptor::new(
            format!("file://{}/low", root),
            vec![VolumeUsageKind::PrimaryDataPriorityLow],
        );
        let config = Config {
            volumes: vec![high, low],
            base_file_size: 64,
            primary_volume_write_stop_watermark: 0.95,
            primary_volume_offload_trigger_watermark: 0.8,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-offload-backpressure"));
        let fm = Arc::new(FileManager::from_config(&config, "db", metrics_manager).unwrap());
        for _ in 0..3 {
            let (_id, mut writer) = fm.create_data_file().unwrap();
            writer.write(&vec![b'x'; 300]).unwrap();
            writer.close().unwrap();
        }
        let first = fm.trigger_offload_if_needed().unwrap();
        let second = fm.trigger_offload_if_needed().unwrap();
        assert_eq!(first, 1);
        assert_eq!(second, 0);
        assert!(fm.wait_for_offload_idle(Duration::from_secs(20)));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[serial_test::serial(file)]
    fn test_write_stop_watermark_blocks_new_writes() {
        let root = "/tmp/file_manager_write_stop_watermark";
        let _ = std::fs::remove_dir_all(root);
        let high_url = format!("file://{}/high", root);
        let mut high =
            crate::VolumeDescriptor::new(high_url, vec![VolumeUsageKind::PrimaryDataPriorityHigh]);
        high.size_limit = Some(1024);
        let config = Config {
            volumes: vec![high],
            base_file_size: 64,
            primary_volume_write_stop_watermark: 0.5,
            primary_volume_offload_trigger_watermark: 0.4,
            ..Config::default()
        };
        let metrics_manager = Arc::new(MetricsManager::new("file-manager-write-stop-watermark"));
        let fm = FileManager::from_config(&config, "db", metrics_manager).unwrap();

        let (_id, mut writer) = fm.create_data_file().unwrap();
        writer.write(&vec![b'x'; 600]).unwrap();
        writer.close().unwrap();

        let err = match fm.create_data_file() {
            Ok(_) => panic!("writes should stop after crossing write-stop watermark"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("All primary data volumes are full")
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
