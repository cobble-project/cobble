//! Multi-DB dedicated compaction service.
//!
//! One scanner thread discovers and validates DB shard directories, while a bounded worker pool
//! executes at most one compaction step per shard. Paths may identify either a DB directory or
//! an immediate parent whose child directories are DB shards.

use super::dedicated_compactor::{
    DedicatedCompactionStep, DedicatedCompactor, DedicatedCompactorProbe,
};
use crate::config::{Config, VolumeUsageKind};
use crate::error::{Error, Result};
use log::{debug, info, warn};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TrySendError, channel, sync_channel,
};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use url::Url;

const WORKER_RECV_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(60);

/// A process-wide dedicated compaction service for multiple DB shard directories.
pub struct DedicatedCompactionService {
    config: Config,
    scan_paths: Vec<PathBuf>,
    worker_count: usize,
    scan_interval: Duration,
    stop: Arc<AtomicBool>,
}

#[derive(Clone)]
struct DiscoveredShard {
    canonical_dir: PathBuf,
    db_id: String,
    config: Config,
}

struct ShardEntry {
    compactor: Arc<DedicatedCompactor>,
    queued_or_running: bool,
    next_probe: Instant,
    consecutive_failures: u32,
}

struct ShardTask {
    key: PathBuf,
    compactor: Arc<DedicatedCompactor>,
}

struct ShardCompletion {
    key: PathBuf,
    result: std::result::Result<DedicatedCompactionStep, String>,
}

impl DedicatedCompactionService {
    /// Creates a service that scans `paths` and runs at most `worker_count` compactions at once.
    ///
    /// Every path must use the local filesystem. A path is treated as a DB directory when it
    /// contains a runtime or snapshot manifest directory; otherwise its immediate children are
    /// scanned for DB directories. The user never supplies a DB id: it is derived from the DB
    /// directory name.
    pub fn open(
        config: Config,
        paths: Vec<PathBuf>,
        worker_count: usize,
        scan_interval: Duration,
    ) -> Result<Self> {
        config.validate_dedicated_compactor()?;
        if paths.is_empty() {
            return Err(Error::ConfigError(
                "dedicated compaction service requires at least one directory".to_string(),
            ));
        }
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
        Ok(Self {
            config,
            scan_paths: paths,
            worker_count,
            scan_interval,
            stop: Arc::new(AtomicBool::new(false)),
        })
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
            )?);
        }
        drop(completion_tx);

        info!(
            "dedicated compaction service started paths={} workers={}",
            self.scan_paths.len(),
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
        let mut registry = HashMap::<PathBuf, ShardEntry>::new();
        let mut next_scan = Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            self.drain_completions(&mut registry, completion_rx);
            let now = Instant::now();
            if now < next_scan {
                thread::sleep(
                    next_scan
                        .saturating_duration_since(now)
                        .min(WORKER_RECV_TIMEOUT),
                );
                continue;
            }

            let discovered = discover_shards(&self.config, &self.scan_paths);
            let present: HashSet<PathBuf> = discovered
                .iter()
                .map(|shard| shard.canonical_dir.clone())
                .collect();
            registry.retain(|key, entry| present.contains(key) || entry.queued_or_running);

            for shard in discovered {
                if !registry.contains_key(&shard.canonical_dir) {
                    match DedicatedCompactor::open(shard.config, &shard.db_id) {
                        Ok(compactor) => {
                            info!(
                                "discovered dedicated compaction shard path={} db_id={}",
                                shard.canonical_dir.display(),
                                shard.db_id
                            );
                            registry.insert(
                                shard.canonical_dir.clone(),
                                ShardEntry {
                                    compactor: Arc::new(compactor),
                                    queued_or_running: false,
                                    next_probe: now,
                                    consecutive_failures: 0,
                                },
                            );
                        }
                        Err(err) => {
                            warn!(
                                "failed to open discovered DB shard {}: {}",
                                shard.canonical_dir.display(),
                                err
                            );
                        }
                    }
                }
            }

            let mut keys: Vec<PathBuf> = registry.keys().cloned().collect();
            keys.sort();
            for key in keys {
                let Some(entry) = registry.get_mut(&key) else {
                    continue;
                };
                if entry.queued_or_running || entry.next_probe > now {
                    continue;
                }
                match entry.compactor.probe() {
                    Ok(DedicatedCompactorProbe::Ready) => {
                        let task = ShardTask {
                            key: key.clone(),
                            compactor: Arc::clone(&entry.compactor),
                        };
                        match task_tx.try_send(task) {
                            Ok(()) => {
                                entry.queued_or_running = true;
                                entry.consecutive_failures = 0;
                            }
                            Err(TrySendError::Full(_)) => {
                                // Preserve fairness: leave the shard idle for the next scan.
                                break;
                            }
                            Err(TrySendError::Disconnected(_)) => {
                                return Err(Error::InvalidState(
                                    "dedicated compaction worker queue disconnected".to_string(),
                                ));
                            }
                        }
                    }
                    Ok(
                        DedicatedCompactorProbe::WaitingForObservation
                        | DedicatedCompactorProbe::WaitingForResult,
                    ) => {
                        entry.consecutive_failures = 0;
                        entry.next_probe = now + self.scan_interval;
                    }
                    Err(err) => {
                        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                        entry.next_probe =
                            now + retry_backoff(self.scan_interval, entry.consecutive_failures);
                        warn!("DB shard validation failed path={}: {}", key.display(), err);
                    }
                }
            }
            next_scan = Instant::now() + self.scan_interval;
        }
        Ok(())
    }

    fn drain_completions(
        &self,
        registry: &mut HashMap<PathBuf, ShardEntry>,
        completion_rx: &Receiver<ShardCompletion>,
    ) {
        while let Ok(completion) = completion_rx.try_recv() {
            let Some(entry) = registry.get_mut(&completion.key) else {
                continue;
            };
            entry.queued_or_running = false;
            match completion.result {
                Ok(step) => {
                    debug!(
                        "dedicated compaction shard step path={} outcome={:?}",
                        completion.key.display(),
                        step
                    );
                    entry.consecutive_failures = 0;
                    entry.next_probe = Instant::now() + self.scan_interval;
                }
                Err(err) => {
                    entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                    entry.next_probe = Instant::now()
                        + retry_backoff(self.scan_interval, entry.consecutive_failures);
                    warn!(
                        "dedicated compaction shard step failed path={}: {}",
                        completion.key.display(),
                        err
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
) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("dedicated-worker-{worker_idx}"))
        .spawn(move || {
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
                let result = task
                    .compactor
                    .run_once_step()
                    .map_err(|err| err.to_string());
                let _ = completion_tx.send(ShardCompletion {
                    key: task.key,
                    result,
                });
            }
        })
        .map_err(|err| {
            Error::IoError(format!(
                "failed to start dedicated compaction worker {worker_idx}: {err}"
            ))
        })
}

fn retry_backoff(base: Duration, failures: u32) -> Duration {
    let multiplier = 1u32 << failures.saturating_sub(1).min(6);
    base.saturating_mul(multiplier).min(MAX_RETRY_BACKOFF)
}

fn discover_shards(base_config: &Config, scan_paths: &[PathBuf]) -> Vec<DiscoveredShard> {
    let mut discovered = BTreeMap::<PathBuf, DiscoveredShard>::new();
    for scan_path in scan_paths {
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
        if is_db_directory(&lexical_path) {
            add_discovered_shard(base_config, lexical_path, canonical, &mut discovered);
            continue;
        }
        let children = match std::fs::read_dir(&lexical_path) {
            Ok(children) => children,
            Err(err) => {
                warn!(
                    "failed to scan dedicated compaction directory {}: {}",
                    lexical_path.display(),
                    err
                );
                continue;
            }
        };
        for child in children.flatten() {
            let path = child.path();
            if path.is_dir() && is_db_directory(&path) {
                let canonical_child = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
                add_discovered_shard(base_config, path, canonical_child, &mut discovered);
            }
        }
    }
    discovered.into_values().collect()
}

fn add_discovered_shard(
    base_config: &Config,
    db_dir: PathBuf,
    canonical_dir: PathBuf,
    discovered: &mut BTreeMap<PathBuf, DiscoveredShard>,
) {
    match config_for_db_directory(base_config, &db_dir) {
        Ok((config, db_id)) => {
            discovered
                .entry(canonical_dir.clone())
                .or_insert(DiscoveredShard {
                    canonical_dir,
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
    if config.volumes.len() == 1 {
        config.volumes[0].base_dir = parent.to_string_lossy().into_owned();
        return Ok((config, db_id));
    }

    let canonical_parent = std::fs::canonicalize(parent).unwrap_or_else(|_| parent.to_path_buf());
    let matches_meta_volume = config
        .volumes
        .iter()
        .filter(|volume| volume.supports(VolumeUsageKind::Meta))
        .filter_map(|volume| local_volume_path(&volume.base_dir))
        .any(|path| std::fs::canonicalize(&path).unwrap_or(path) == canonical_parent);
    if !matches_meta_volume {
        return Err(Error::ConfigError(format!(
            "DB directory {} is not directly under a configured metadata volume; \
             path-only discovery with multiple volumes requires that layout",
            db_dir.display()
        )));
    }
    Ok((config, db_id))
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
