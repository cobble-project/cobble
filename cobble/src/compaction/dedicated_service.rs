//! Multi-DB dedicated compaction service.
//!
//! One scanner thread discovers and validates DB shard directories, while a bounded worker pool
//! executes at most one compaction step per shard. Local paths and storage URLs are scanned with
//! bounded recursion and may identify either one DB or a parent prefix containing many shards.

use super::dedicated_compactor::{
    DedicatedCompactionExecution, DedicatedCompactionExecutor, DedicatedCompactionPlan,
    DedicatedCompactionPlanStatus, DedicatedCompactionPlanner, DedicatedCompactionPlanning,
};
use crate::config::{Config, VolumeDescriptor, VolumeUsageKind};
use crate::error::{Error, Result};
use crate::file::FileSystemRegistry;
use crate::properties::DB_PROPERTIES_NAME;
use crate::schema::SchemaTransformRegistry;
use crate::util::normalize_storage_path_to_url;
use bytes::Bytes;
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TrySendError, channel, sync_channel,
};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use url::Url;

const WORKER_RECV_TIMEOUT: Duration = Duration::from_millis(100);
// Covers checkpoint-root/job-id/shared/operator/volume/db while keeping recursive discovery
// bounded for accidentally broad storage prefixes.
const MAX_DISCOVERY_DEPTH: usize = 6;
const MAX_DISCOVERED_ENTRIES: usize = 10_000;

#[derive(Clone)]
enum CompactionScanPath {
    Local(PathBuf),
    Storage(String),
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ShardLocation {
    Local(PathBuf),
    Storage(String),
}

impl fmt::Display for ShardLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(path) => write!(formatter, "{}", path.display()),
            Self::Storage(url) => formatter.write_str(url),
        }
    }
}

/// A process-wide dedicated compaction service for multiple DB shard directories.
pub struct DedicatedCompactionService {
    monitor: Mutex<DedicatedCompactionMonitor>,
    executor_config: Arc<Config>,
    resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
    worker_count: usize,
    scan_interval: Duration,
    stop: Arc<AtomicBool>,
}

enum MonitorInput {
    Recursive(Vec<CompactionScanPath>),
    Exact(Vec<CompactionScanPath>),
}

/// Discovers Cobble DBs and produces portable compaction plans without executing them.
///
/// Use [`scan`](Self::scan) to recursively monitor one directory/prefix, or
/// [`watch_databases`](Self::watch_databases) when the caller already has a set of exact DB
/// directories. An emitted plan remains outstanding until its durable result appears, its source
/// observation becomes stale, or [`complete`](Self::complete) is called after an execution error.
/// This prevents one monitor instance from enqueueing the same DB repeatedly.
pub struct DedicatedCompactionMonitor {
    config: Config,
    input: MonitorInput,
    file_system_registry: FileSystemRegistry,
    resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
    planners: HashMap<ShardLocation, DedicatedCompactionPlanner>,
    outstanding: HashMap<String, OutstandingPlan>,
}

struct OutstandingPlan {
    location: ShardLocation,
    plan: DedicatedCompactionPlan,
}

impl DedicatedCompactionMonitor {
    pub fn scan(config: Config, root: impl Into<String>) -> Result<Self> {
        Self::new(config, vec![root.into()], true, None)
    }

    pub fn scan_with_resolver(
        config: Config,
        root: impl Into<String>,
        resolver: Arc<dyn crate::MergeOperatorResolver>,
    ) -> Result<Self> {
        Self::new(config, vec![root.into()], true, Some(resolver))
    }

    pub fn watch_databases(config: Config, paths: Vec<String>) -> Result<Self> {
        Self::new(config, paths, false, None)
    }

    pub fn watch_databases_with_resolver(
        config: Config,
        paths: Vec<String>,
        resolver: Arc<dyn crate::MergeOperatorResolver>,
    ) -> Result<Self> {
        Self::new(config, paths, false, Some(resolver))
    }

    fn new(
        config: Config,
        paths: Vec<String>,
        recursive: bool,
        resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(Error::ConfigError(
                "dedicated compaction monitor requires at least one path".to_string(),
            ));
        }
        let paths = paths
            .into_iter()
            .map(parse_scan_path)
            .collect::<Result<Vec<_>>>()?;
        Self::from_paths(config, paths, recursive, resolver)
    }

    fn from_paths(
        config: Config,
        paths: Vec<CompactionScanPath>,
        recursive: bool,
        resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    ) -> Result<Self> {
        config.validate_dedicated_compactor()?;
        if paths.is_empty() {
            return Err(Error::ConfigError(
                "dedicated compaction monitor requires at least one path".to_string(),
            ));
        }
        Ok(Self {
            config,
            input: if recursive {
                MonitorInput::Recursive(paths)
            } else {
                MonitorInput::Exact(paths)
            },
            file_system_registry: FileSystemRegistry::new(),
            resolver,
            transforms: Arc::new(SchemaTransformRegistry::default()),
            planners: HashMap::new(),
            outstanding: HashMap::new(),
        })
    }

    /// Register a single-column schema transform under its stable persisted ID.
    pub fn register_schema_transform<F>(
        &self,
        transform_id: impl Into<String>,
        transform: F,
    ) -> Result<()>
    where
        F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_id, transform)
    }

    /// Discovers current DBs and returns at most one plan per DB.
    pub fn poll(&mut self) -> Result<Vec<DedicatedCompactionPlan>> {
        let discovered = match &self.input {
            MonitorInput::Recursive(paths) => {
                discover_shards(&self.config, paths, &self.file_system_registry)
            }
            MonitorInput::Exact(paths) => {
                discover_exact_shards(&self.config, paths, &self.file_system_registry)
            }
        };
        let present: HashSet<ShardLocation> = discovered
            .iter()
            .map(|shard| shard.location.clone())
            .collect();
        self.reconcile_outstanding();
        let outstanding_locations: HashSet<ShardLocation> = self
            .outstanding
            .values()
            .map(|outstanding| outstanding.location.clone())
            .collect();
        self.planners.retain(|location, _| {
            present.contains(location) || outstanding_locations.contains(location)
        });

        let mut plans = Vec::new();
        for shard in discovered {
            if outstanding_locations.contains(&shard.location) {
                continue;
            }
            if !self.planners.contains_key(&shard.location) {
                match DedicatedCompactionPlanner::open_with_runtime_wiring(
                    shard.config,
                    &shard.db_id,
                    self.resolver.clone(),
                    Arc::clone(&self.transforms),
                ) {
                    Ok(planner) => {
                        info!(
                            "discovered dedicated compaction DB path={} db_id={}",
                            shard.location, shard.db_id
                        );
                        self.planners.insert(shard.location.clone(), planner);
                    }
                    Err(err) => {
                        debug!(
                            "dedicated compaction DB {} is not ready: {}",
                            shard.location, err
                        );
                        continue;
                    }
                }
            }
            let Some(planner) = self.planners.get(&shard.location) else {
                continue;
            };
            match planner.plan() {
                Ok(DedicatedCompactionPlanning::Plan(plan)) => {
                    self.outstanding.insert(
                        plan.job_id().to_string(),
                        OutstandingPlan {
                            location: shard.location.clone(),
                            plan: plan.clone(),
                        },
                    );
                    plans.push(plan);
                }
                Ok(
                    DedicatedCompactionPlanning::WaitingForObservation
                    | DedicatedCompactionPlanning::WaitingForResult
                    | DedicatedCompactionPlanning::NoPlan,
                ) => {}
                Err(err) => {
                    warn!(
                        "failed to plan dedicated compaction for {}: {}",
                        shard.location, err
                    );
                }
            }
        }
        Ok(plans)
    }

    /// Releases an outstanding plan after its execution attempt has completed.
    pub fn complete(&mut self, job_id: &str) {
        self.outstanding.remove(job_id);
    }

    fn reconcile_outstanding(&mut self) {
        self.outstanding.retain(|job_id, outstanding| {
            let Some(planner) = self.planners.get(&outstanding.location) else {
                return true;
            };
            match planner.status(&outstanding.plan) {
                Ok(DedicatedCompactionPlanStatus::Pending) => true,
                Ok(
                    DedicatedCompactionPlanStatus::ResultPublished
                    | DedicatedCompactionPlanStatus::Stale,
                ) => false,
                Err(err) => {
                    warn!(
                        "failed to refresh dedicated compaction plan {} for {}: {}",
                        job_id, outstanding.location, err
                    );
                    true
                }
            }
        });
    }
}

#[derive(Clone)]
struct DiscoveredShard {
    location: ShardLocation,
    db_id: String,
    config: Config,
}

struct ShardTask {
    plan: DedicatedCompactionPlan,
}

struct ShardCompletion {
    job_id: String,
    result: std::result::Result<DedicatedCompactionExecution, String>,
}

impl DedicatedCompactionService {
    /// Creates a service that scans `paths` and runs at most `worker_count` compactions at once.
    ///
    /// Every path must use the local filesystem. A path is treated as a DB directory when it
    /// contains DB metadata; otherwise it is scanned recursively with bounded depth and entry
    /// count. The user never supplies a DB id: it is derived from the DB directory name.
    pub fn open(
        config: Config,
        paths: Vec<PathBuf>,
        worker_count: usize,
        scan_interval: Duration,
    ) -> Result<Self> {
        Self::from_monitor(
            DedicatedCompactionMonitor::from_paths(
                config,
                paths.into_iter().map(CompactionScanPath::Local).collect(),
                true,
                None,
            )?,
            worker_count,
            scan_interval,
        )
    }

    /// Creates a service that can scan local paths or storage URLs such as S3 and OSS prefixes.
    pub fn open_storage_paths(
        config: Config,
        paths: Vec<String>,
        worker_count: usize,
        scan_interval: Duration,
    ) -> Result<Self> {
        Self::from_monitor(
            DedicatedCompactionMonitor::new(config, paths, true, None)?,
            worker_count,
            scan_interval,
        )
    }

    /// Creates the convenience service around an independently configured monitor.
    pub fn from_monitor(
        monitor: DedicatedCompactionMonitor,
        worker_count: usize,
        scan_interval: Duration,
    ) -> Result<Self> {
        if worker_count == 0 {
            return Err(Error::ConfigError(
                "dedicated compaction service worker count must be greater than zero".to_string(),
            ));
        }
        if scan_interval.is_zero() {
            return Err(Error::ConfigError(
                "dedicated compaction service scan interval must be greater than zero".to_string(),
            ));
        }
        let executor_config = Arc::new(monitor.config.clone());
        let resolver = monitor.resolver.clone();
        let transforms = Arc::clone(&monitor.transforms);
        Ok(Self {
            monitor: Mutex::new(monitor),
            executor_config,
            resolver,
            transforms,
            worker_count,
            scan_interval,
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Register a single-column schema transform under its stable persisted ID.
    pub fn register_schema_transform<F>(
        &self,
        transform_id: impl Into<String>,
        transform: F,
    ) -> Result<()>
    where
        F: Fn(Option<Bytes>) -> Result<Option<Bytes>> + Send + Sync + 'static,
    {
        self.transforms.register(transform_id, transform)
    }

    /// Signals the scanner and workers to stop. Running compactions finish their current step.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Runs the scanner loop and worker pool until [`stop`](Self::stop) is called.
    pub fn run(&self) -> Result<()> {
        let queue_capacity = self.worker_count.saturating_mul(2).max(1);
        let (task_tx, task_rx) = sync_channel::<ShardTask>(queue_capacity);
        let task_rx = Arc::new(Mutex::new(task_rx));
        let (completion_tx, completion_rx) = channel::<ShardCompletion>();
        let mut workers = Vec::with_capacity(self.worker_count);
        for worker_idx in 0..self.worker_count {
            workers.push(spawn_worker(
                worker_idx,
                Arc::clone(&task_rx),
                completion_tx.clone(),
                Arc::clone(&self.stop),
                Arc::clone(&self.executor_config),
                self.resolver.clone(),
                Arc::clone(&self.transforms),
            )?);
        }
        drop(completion_tx);

        info!(
            "dedicated compaction service started workers={}",
            self.worker_count
        );
        let result = self.run_scanner(&task_tx, &completion_rx);
        self.stop.store(true, Ordering::Release);
        drop(task_tx);
        for worker in workers {
            let _ = worker.join();
        }
        info!("dedicated compaction service stopped");
        result
    }

    fn run_scanner(
        &self,
        task_tx: &SyncSender<ShardTask>,
        completion_rx: &Receiver<ShardCompletion>,
    ) -> Result<()> {
        let mut next_scan = std::time::Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            self.drain_completions(completion_rx);
            let now = std::time::Instant::now();
            if now < next_scan {
                thread::sleep(
                    next_scan
                        .saturating_duration_since(now)
                        .min(WORKER_RECV_TIMEOUT),
                );
                continue;
            }

            let plans = self.monitor.lock().unwrap().poll()?;
            for plan in plans {
                let job_id = plan.job_id().to_string();
                match task_tx.try_send(ShardTask { plan }) {
                    Ok(()) => {}
                    Err(TrySendError::Full(task)) => {
                        self.monitor.lock().unwrap().complete(task.plan.job_id());
                        continue;
                    }
                    Err(TrySendError::Disconnected(task)) => {
                        self.monitor.lock().unwrap().complete(task.plan.job_id());
                        return Err(Error::InvalidState(
                            "dedicated compaction worker queue disconnected".to_string(),
                        ));
                    }
                }
                debug!("queued dedicated compaction plan job={job_id}");
            }
            next_scan = std::time::Instant::now() + self.scan_interval;
        }
        Ok(())
    }

    fn drain_completions(&self, completion_rx: &Receiver<ShardCompletion>) {
        while let Ok(completion) = completion_rx.try_recv() {
            self.monitor.lock().unwrap().complete(&completion.job_id);
            match completion.result {
                Ok(outcome) => {
                    debug!(
                        "dedicated compaction job={} outcome={:?}",
                        completion.job_id, outcome
                    );
                }
                Err(err) => {
                    warn!(
                        "dedicated compaction job={} failed: {}",
                        completion.job_id, err
                    );
                }
            }
        }
    }
}

fn spawn_worker(
    worker_idx: usize,
    task_rx: Arc<Mutex<Receiver<ShardTask>>>,
    completion_tx: std::sync::mpsc::Sender<ShardCompletion>,
    stop: Arc<AtomicBool>,
    executor_config: Arc<Config>,
    resolver: Option<Arc<dyn crate::MergeOperatorResolver>>,
    transforms: Arc<SchemaTransformRegistry>,
) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("dedicated-worker-{worker_idx}"))
        .spawn(move || {
            let executor = match DedicatedCompactionExecutor::open_with_runtime_wiring(
                executor_config.as_ref().clone(),
                resolver,
                transforms,
            ) {
                Ok(executor) => executor,
                Err(err) => {
                    warn!("failed to open dedicated compaction worker {worker_idx}: {err}");
                    return;
                }
            };
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let task = {
                    let receiver = task_rx.lock().unwrap();
                    match receiver.recv_timeout(WORKER_RECV_TIMEOUT) {
                        Ok(task) => task,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                };
                let job_id = task.plan.job_id().to_string();
                let result = executor.execute(&task.plan).map_err(|err| err.to_string());
                let _ = completion_tx.send(ShardCompletion { job_id, result });
            }
        })
        .map_err(|err| {
            Error::IoError(format!(
                "failed to start dedicated compaction worker {worker_idx}: {err}"
            ))
        })
}

fn discover_shards(
    base_config: &Config,
    scan_paths: &[CompactionScanPath],
    file_system_registry: &FileSystemRegistry,
) -> Vec<DiscoveredShard> {
    let mut discovered = BTreeMap::<ShardLocation, DiscoveredShard>::new();
    for scan_path in scan_paths {
        if let CompactionScanPath::Storage(scan_path) = scan_path {
            discover_storage_shards(
                base_config,
                scan_path,
                file_system_registry,
                &mut discovered,
            );
            continue;
        }
        let CompactionScanPath::Local(scan_path) = scan_path else {
            unreachable!();
        };
        let lexical_path = if scan_path.is_absolute() {
            scan_path.clone()
        } else {
            match std::env::current_dir() {
                Ok(current_dir) => current_dir.join(scan_path),
                Err(err) => {
                    warn!(
                        "failed to resolve relative dedicated compaction scan path {}: {}",
                        scan_path.display(),
                        err
                    );
                    continue;
                }
            }
        };
        let canonical = match std::fs::canonicalize(&lexical_path) {
            Ok(path) => path,
            Err(err) => {
                debug!(
                    "dedicated compaction scan path {} is not available yet: {}",
                    lexical_path.display(),
                    err
                );
                continue;
            }
        };
        let mut pending = vec![(lexical_path, canonical, 0usize)];
        let mut visited = 0usize;
        while let Some((path, canonical_path, depth)) = pending.pop() {
            if visited >= MAX_DISCOVERED_ENTRIES {
                warn!(
                    "dedicated compaction discovery at {} reached the {} entry limit",
                    scan_path.display(),
                    MAX_DISCOVERED_ENTRIES
                );
                break;
            }
            visited += 1;
            if is_db_directory(&path) {
                add_discovered_shard(base_config, path, canonical_path, &mut discovered);
                continue;
            }
            if depth >= MAX_DISCOVERY_DEPTH {
                continue;
            }
            let children = match std::fs::read_dir(&path) {
                Ok(children) => children,
                Err(err) => {
                    debug!(
                        "failed to scan dedicated compaction directory {}: {}",
                        path.display(),
                        err
                    );
                    continue;
                }
            };
            for child in children.flatten() {
                let child_path = child.path();
                if child_path.is_dir() {
                    let canonical_child =
                        std::fs::canonicalize(&child_path).unwrap_or_else(|_| child_path.clone());
                    pending.push((child_path, canonical_child, depth + 1));
                }
            }
        }
    }
    discovered.into_values().collect()
}

fn discover_exact_shards(
    base_config: &Config,
    paths: &[CompactionScanPath],
    file_system_registry: &FileSystemRegistry,
) -> Vec<DiscoveredShard> {
    let mut discovered = BTreeMap::<ShardLocation, DiscoveredShard>::new();
    for path in paths {
        match path {
            CompactionScanPath::Local(path) => {
                let path = if path.is_absolute() {
                    path.clone()
                } else {
                    match std::env::current_dir() {
                        Ok(current_dir) => current_dir.join(path),
                        Err(err) => {
                            debug!(
                                "failed to resolve dedicated compaction DB path {}: {}",
                                path.display(),
                                err
                            );
                            continue;
                        }
                    }
                };
                if !is_db_directory(&path) {
                    continue;
                }
                let canonical = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                add_discovered_shard(base_config, path, canonical, &mut discovered);
            }
            CompactionScanPath::Storage(path) => {
                let Ok(db_url) = normalized_url(path) else {
                    warn!("invalid dedicated compaction DB path {}", path);
                    continue;
                };
                let Some((volume, volume_url)) = matching_metadata_volume(base_config, &db_url)
                else {
                    warn!(
                        "dedicated compaction DB path {} is not under a configured metadata volume",
                        path
                    );
                    continue;
                };
                let Some(db_prefix) = relative_url_path(&volume_url, &db_url) else {
                    continue;
                };
                let fs = match file_system_registry.get_or_register_volume(volume) {
                    Ok(fs) => fs,
                    Err(err) => {
                        debug!(
                            "failed to open dedicated compaction DB path {}: {}",
                            path, err
                        );
                        continue;
                    }
                };
                let properties = join_relative_path(&db_prefix, DB_PROPERTIES_NAME);
                match fs.exists(&properties) {
                    Ok(true) => {
                        if let Some(shard) =
                            storage_discovered_shard(base_config, &volume_url, &db_prefix)
                        {
                            discovered.entry(shard.location.clone()).or_insert(shard);
                        }
                    }
                    Ok(false) => {}
                    Err(err) => debug!("failed to probe storage path {}: {}", properties, err),
                }
            }
        }
    }
    discovered.into_values().collect()
}

fn discover_storage_shards(
    base_config: &Config,
    scan_path: &str,
    file_system_registry: &FileSystemRegistry,
    discovered: &mut BTreeMap<ShardLocation, DiscoveredShard>,
) {
    let Ok(scan_url) = normalized_url(scan_path) else {
        warn!("invalid dedicated compaction storage path {}", scan_path);
        return;
    };
    let Some((volume, volume_url)) = matching_metadata_volume(base_config, &scan_url) else {
        warn!(
            "dedicated compaction storage path {} is not under a configured metadata volume",
            scan_path
        );
        return;
    };
    let Some(root_prefix) = relative_url_path(&volume_url, &scan_url) else {
        return;
    };
    let fs = match file_system_registry.get_or_register_volume(volume) {
        Ok(fs) => fs,
        Err(err) => {
            warn!(
                "failed to open dedicated compaction storage path {}: {}",
                scan_path, err
            );
            return;
        }
    };

    let mut pending = vec![(root_prefix, 0usize)];
    let mut visited = 0usize;
    while let Some((prefix, depth)) = pending.pop() {
        if visited >= MAX_DISCOVERED_ENTRIES {
            warn!(
                "dedicated compaction discovery at {} reached the {} entry limit",
                scan_path, MAX_DISCOVERED_ENTRIES
            );
            break;
        }
        visited += 1;
        let properties = join_relative_path(&prefix, DB_PROPERTIES_NAME);
        match fs.exists(&properties) {
            Ok(true) => {
                if let Some(shard) = storage_discovered_shard(base_config, &volume_url, &prefix) {
                    discovered.entry(shard.location.clone()).or_insert(shard);
                }
                continue;
            }
            Ok(false) => {}
            Err(err) => {
                debug!("failed to probe storage path {}: {}", properties, err);
                continue;
            }
        }
        if depth >= MAX_DISCOVERY_DEPTH {
            continue;
        }
        match fs.list(&prefix) {
            Ok(children) => {
                for child in children.into_iter().rev() {
                    pending.push((join_relative_path(&prefix, &child), depth + 1));
                }
            }
            Err(err) => debug!("failed to list storage path {}: {}", prefix, err),
        }
    }
}

fn storage_discovered_shard(
    base_config: &Config,
    volume_url: &Url,
    db_prefix: &str,
) -> Option<DiscoveredShard> {
    let db_id = db_prefix
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|value| !value.is_empty())?
        .to_string();
    let parent_prefix = db_prefix
        .trim_end_matches('/')
        .rsplit_once('/')
        .map(|(parent, _)| parent)
        .unwrap_or("");
    let mut config = base_config.clone();
    rebase_volume_roots(&mut config, parent_prefix);
    let location = ShardLocation::Storage(joined_storage_url(volume_url, db_prefix));
    Some(DiscoveredShard {
        location,
        db_id,
        config,
    })
}

fn matching_metadata_volume<'a>(
    config: &'a Config,
    scan_url: &Url,
) -> Option<(&'a VolumeDescriptor, Url)> {
    config
        .volumes
        .iter()
        .enumerate()
        .filter(|(_, volume)| volume.supports(VolumeUsageKind::Meta))
        .filter_map(|(index, volume)| {
            let url = normalized_url(&volume.base_dir).ok()?;
            relative_url_path(&url, scan_url).map(|relative| (relative.len(), index, volume, url))
        })
        .min_by_key(|(relative_len, _, _, _)| *relative_len)
        .map(|(_, _, volume, url)| (volume, url))
}

fn normalized_url(path: &str) -> Result<Url> {
    Url::parse(&normalize_storage_path_to_url(path)?).map_err(Error::from)
}

fn relative_url_path(base: &Url, target: &Url) -> Option<String> {
    if base.scheme() != target.scheme()
        || base.host_str() != target.host_str()
        || base.port_or_known_default() != target.port_or_known_default()
    {
        return None;
    }
    let base_path = base.path().trim_end_matches('/');
    let target_path = target.path().trim_end_matches('/');
    if target_path == base_path {
        return Some(String::new());
    }
    target_path
        .strip_prefix(base_path)
        .and_then(|suffix| suffix.strip_prefix('/'))
        .map(ToString::to_string)
}

fn joined_storage_url(base: &Url, relative: &str) -> String {
    let mut joined = base.clone();
    let path = join_relative_path(base.path().trim_end_matches('/'), relative);
    joined.set_path(&path);
    joined.to_string()
}

fn join_relative_path(parent: &str, child: &str) -> String {
    match (parent.trim_matches('/'), child.trim_matches('/')) {
        ("", child) => child.to_string(),
        (parent, "") => parent.to_string(),
        (parent, child) => format!("{parent}/{child}"),
    }
}

fn rebase_volume_roots(config: &mut Config, relative_parent: &str) {
    if relative_parent.is_empty() {
        return;
    }
    for volume in &mut config.volumes {
        volume.base_dir = append_relative_path(&volume.base_dir, relative_parent);
    }
}

fn append_relative_path(base: &str, relative: &str) -> String {
    if relative.is_empty() {
        return base.to_string();
    }
    if Path::new(base).is_absolute() {
        return Path::new(base)
            .join(relative)
            .to_string_lossy()
            .into_owned();
    }
    match Url::parse(base) {
        Ok(url) => joined_storage_url(&url, relative),
        Err(_) => Path::new(base)
            .join(relative)
            .to_string_lossy()
            .into_owned(),
    }
}

fn add_discovered_shard(
    base_config: &Config,
    db_dir: PathBuf,
    canonical_dir: PathBuf,
    discovered: &mut BTreeMap<ShardLocation, DiscoveredShard>,
) {
    match config_for_db_directory(base_config, &db_dir) {
        Ok((config, db_id)) => {
            let location = ShardLocation::Local(canonical_dir);
            discovered
                .entry(location.clone())
                .or_insert(DiscoveredShard {
                    location,
                    db_id,
                    config,
                });
        }
        Err(err) => warn!(
            "ignoring dedicated compaction DB directory {}: {}",
            db_dir.display(),
            err
        ),
    }
}

fn is_db_directory(path: &Path) -> bool {
    if path.join(DB_PROPERTIES_NAME).is_file() {
        return true;
    }
    let runtime_dir = path.join("runtime");
    if runtime_dir.is_dir() {
        return true;
    }
    let snapshot_dir = path.join("snapshot");
    let Ok(entries) = std::fs::read_dir(snapshot_dir) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with("SNAPSHOT-"))
    })
}

fn config_for_db_directory(base: &Config, db_dir: &Path) -> Result<(Config, String)> {
    let db_id = db_dir
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            Error::ConfigError(format!(
                "DB directory {} has no valid final path component",
                db_dir.display()
            ))
        })?
        .to_string();
    let parent = db_dir.parent().ok_or_else(|| {
        Error::ConfigError(format!(
            "DB directory {} has no parent directory",
            db_dir.display()
        ))
    })?;
    let mut config = base.clone();
    let canonical_parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    let matching_volume = config
        .volumes
        .iter()
        .enumerate()
        .filter(|(_, volume)| volume.supports(VolumeUsageKind::Meta))
        .filter_map(|(index, volume)| {
            let path = local_volume_path(&volume.base_dir)?;
            let canonical = std::fs::canonicalize(&path).unwrap_or(path);
            canonical_parent.starts_with(&canonical).then(|| {
                (
                    canonical.components().count(),
                    index,
                    canonical_parent
                        .strip_prefix(&canonical)
                        .expect("prefix checked above")
                        .to_string_lossy()
                        .into_owned(),
                )
            })
        })
        .max_by_key(|(depth, _, _)| *depth);
    if let Some((_, _, relative_parent)) = matching_volume {
        rebase_volume_roots(&mut config, &relative_parent);
    } else if config.volumes.len() == 1 {
        config.volumes[0].base_dir = parent.to_string_lossy().into_owned();
    } else {
        return Err(Error::ConfigError(format!(
            "DB directory {} is not under a configured local metadata volume",
            db_dir.display()
        )));
    }
    Ok((config, db_id))
}

fn parse_scan_path(path: String) -> Result<CompactionScanPath> {
    if path.trim().is_empty() {
        return Err(Error::ConfigError(
            "dedicated compaction scan paths must not be blank".to_string(),
        ));
    }
    // Check native absolute paths before URL parsing so Windows drive-letter paths are not
    // mistaken for custom storage schemes.
    if Path::new(&path).is_absolute() {
        return Ok(CompactionScanPath::Local(PathBuf::from(path)));
    }
    match Url::parse(&path) {
        Ok(url) if url.scheme() == "file" => url
            .to_file_path()
            .map(CompactionScanPath::Local)
            .map_err(|_| Error::ConfigError(format!("invalid file URL scan path: {path}"))),
        Ok(_) => Ok(CompactionScanPath::Storage(path)),
        Err(_) if path.contains("://") => Err(Error::ConfigError(format!(
            "invalid storage URL scan path: {path}"
        ))),
        Err(_) => Ok(CompactionScanPath::Local(PathBuf::from(path))),
    }
}

fn local_volume_path(base_dir: &str) -> Option<PathBuf> {
    match Url::parse(base_dir) {
        Ok(url) if url.scheme() == "file" => url.to_file_path().ok(),
        Ok(_) => None,
        Err(_) => Some(PathBuf::from(base_dir)),
    }
}

#[cfg(test)]
#[path = "../../tests/unit/compaction/dedicated_service.rs"]
mod tests;
