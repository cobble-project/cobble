//! Snapshot manager and manifest encoding for LSM state.
use crate::db_state::DbState;
use crate::error::{Error, Result};
use crate::file::{BufferedWriter, FileManager, SequentialWriteFile};
use crate::lsm::Level;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;

/// Internal snapshot record tracked by the manager.
#[derive(Clone)]
pub(crate) struct DbSnapshot {
    pub id: u64,
    pub manifest_path: String,
    pub levels: Vec<Level>,
    pub finished: bool,
}

#[derive(Serialize)]
struct ManifestSnapshot {
    id: u64,
    levels: Vec<ManifestLevel>,
}

#[derive(Serialize)]
struct ManifestLevel {
    ordinal: u8,
    tiered: bool,
    files: Vec<ManifestFile>,
}

#[derive(Serialize)]
struct ManifestFile {
    file_id: u64,
    file_type: &'static str,
    seq: u64,
    size: usize,
    start_key: String,
    end_key: String,
    path: Option<String>,
}

impl DbSnapshot {
    pub(crate) fn new(id: u64, manifest_path: String) -> Self {
        Self {
            id,
            manifest_path,
            levels: vec![],
            finished: false,
        }
    }
}

pub(crate) struct SnapshotManager {
    file_manager: Arc<FileManager>,
    state: Arc<Mutex<SnapshotManagerState>>,
    /// Background worker for manifest materialization.
    materialize_tx: Arc<Mutex<Option<mpsc::Sender<u64>>>>,
    materialize_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Clone for SnapshotManager {
    fn clone(&self) -> Self {
        Self {
            file_manager: Arc::clone(&self.file_manager),
            state: Arc::clone(&self.state),
            materialize_tx: Arc::clone(&self.materialize_tx),
            materialize_worker: Arc::clone(&self.materialize_worker),
        }
    }
}

struct SnapshotManagerState {
    next_id: u64,
    snapshots: BTreeMap<u64, DbSnapshot>,
}

impl SnapshotManager {
    pub(crate) fn new(file_manager: Arc<FileManager>) -> Self {
        Self {
            file_manager,
            state: Arc::new(Mutex::new(SnapshotManagerState {
                next_id: 0,
                snapshots: BTreeMap::new(),
            })),
            materialize_tx: Arc::new(Mutex::new(None)),
            materialize_worker: Arc::new(Mutex::new(None)),
        }
    }

    /// Ensure the materializer worker is running.
    pub(crate) fn start_materializer(&self) -> Result<()> {
        let mut tx_guard = self.materialize_tx.lock().unwrap();
        if tx_guard.is_some() {
            return Ok(());
        }
        let (tx, rx) = mpsc::channel::<u64>();
        let manager = self.clone();
        let handle = std::thread::Builder::new()
            .name("cobble-snapshot".to_string())
            .spawn(move || {
                while let Ok(id) = rx.recv() {
                    let _ = manager.materialize(id);
                }
            })
            .map_err(|e| Error::IoError(format!("Failed to start snapshot worker: {}", e)))?;
        *tx_guard = Some(tx);
        let mut worker_guard = self.materialize_worker.lock().unwrap();
        *worker_guard = Some(handle);
        Ok(())
    }

    pub(crate) fn create_snapshot(&self) -> DbSnapshot {
        let mut state = self.state.lock().unwrap();
        let id = state.next_id;
        state.next_id += 1;
        let manifest_path = self.file_manager.metadata_path(&snapshot_manifest_name(id));
        let snapshot = DbSnapshot::new(id, manifest_path);
        state.snapshots.insert(id, snapshot.clone());
        snapshot
    }

    pub(crate) fn finish_snapshot(&self, id: u64, db_state: &Arc<DbState>) -> bool {
        let mut state = self.state.lock().unwrap();
        if let Some(snapshot) = state.snapshots.get_mut(&id) {
            snapshot.levels = db_state.lsm_version.levels.clone();
            return true;
        }
        false
    }

    /// Write the snapshot manifest for the given id.
    pub(crate) fn materialize(&self, id: u64) -> Result<()> {
        let mut snapshot = {
            let state = self.state.lock().unwrap();
            state
                .snapshots
                .get(&id)
                .cloned()
                .ok_or_else(|| Error::IoError(format!("Snapshot {} not found", id)))?
        };
        let writer = self
            .file_manager
            .create_metadata_file(&snapshot_manifest_name(id))?;
        let mut buffered = BufferedWriter::new(writer, 8192);
        encode_manifest(&mut buffered, &snapshot, &self.file_manager)?;
        buffered.close()?;
        snapshot.finished = true;
        Ok(())
    }

    /// Enqueue a manifest materialization job on the background worker.
    pub(crate) fn schedule_materialize(&self, id: u64) -> Result<()> {
        self.start_materializer()?;
        let tx_guard = self.materialize_tx.lock().unwrap();
        if let Some(tx) = tx_guard.as_ref() {
            tx.send(id)
                .map_err(|_| Error::IoError("Snapshot worker unavailable".to_string()))?;
        }
        Ok(())
    }

    /// Stop the background materializer worker.
    pub(crate) fn close(&self) -> Result<()> {
        let mut tx_guard = self.materialize_tx.lock().unwrap();
        tx_guard.take();
        let worker = self.materialize_worker.lock().unwrap().take();
        if let Some(worker) = worker {
            let _ = worker.join();
        }
        Ok(())
    }
}

/// Encode a snapshot manifest as JSON.
pub(crate) fn encode_manifest<W: SequentialWriteFile>(
    writer: &mut BufferedWriter<W>,
    snapshot: &DbSnapshot,
    file_manager: &FileManager,
) -> Result<()> {
    let manifest = ManifestSnapshot {
        id: snapshot.id,
        levels: snapshot
            .levels
            .iter()
            .map(|level| ManifestLevel {
                ordinal: level.ordinal,
                tiered: level.tiered,
                files: level
                    .files
                    .iter()
                    .map(|file| ManifestFile {
                        file_id: file.file_id,
                        file_type: file.file_type.as_str(),
                        seq: file.seq,
                        size: file.size,
                        start_key: to_hex(&file.start_key),
                        end_key: to_hex(&file.end_key),
                        path: file_manager.get_data_file_path(file.file_id),
                    })
                    .collect(),
            })
            .collect(),
    };
    let json = serde_json::to_vec(&manifest)
        .map_err(|err| Error::IoError(format!("Failed to encode manifest: {}", err)))?;
    writer.write(&json)?;
    Ok(())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{:02x}", b);
    }
    out
}

fn snapshot_manifest_name(id: u64) -> String {
    format!("SNAPSHOT-{}", id)
}
