//! Snapshot manager and manifest encoding for LSM state.
use crate::data_file::{DataFile, DataFileType};
use crate::db_state::DbState;
use crate::error::{Error, Result};
use crate::file::{
    BufferedWriter, File, FileManager, SequentialWriteFile, TrackedFile, TrackedFileId,
};
use crate::lsm::Level;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;

/// Internal snapshot record tracked by the manager.
#[derive(Clone)]
pub(crate) struct DbSnapshot {
    pub id: u64,
    pub manifest_path: String,
    pub levels: Vec<Level>,
    pub data_files: Vec<Arc<DataFile>>,
    pub tracked_files: Vec<Arc<TrackedFile>>,
    pub seq_id: u64,
    pub finished: bool,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ManifestSnapshot {
    pub(crate) id: u64,
    pub(crate) seq_id: u64,
    pub(crate) levels: Vec<ManifestLevel>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ManifestLevel {
    pub(crate) ordinal: u8,
    pub(crate) tiered: bool,
    pub(crate) files: Vec<ManifestFile>,
}

#[derive(Deserialize, Serialize)]
pub(crate) struct ManifestFile {
    pub(crate) file_id: u64,
    pub(crate) file_type: String,
    pub(crate) seq: u64,
    pub(crate) size: usize,
    pub(crate) start_key: String,
    pub(crate) end_key: String,
    pub(crate) path: Option<String>,
}

impl DbSnapshot {
    pub(crate) fn new(id: u64, manifest_path: String) -> Self {
        Self {
            id,
            manifest_path,
            levels: vec![],
            data_files: Vec::new(),
            tracked_files: Vec::new(),
            seq_id: 0,
            finished: false,
        }
    }
}

pub(crate) struct SnapshotManager {
    file_manager: Arc<FileManager>,
    state: Arc<Mutex<SnapshotManagerState>>,
    retention: Option<usize>,
    /// Background worker for manifest materialization.
    materialize_tx: Arc<Mutex<Option<mpsc::Sender<u64>>>>,
    materialize_worker: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Clone for SnapshotManager {
    fn clone(&self) -> Self {
        Self {
            file_manager: Arc::clone(&self.file_manager),
            state: Arc::clone(&self.state),
            retention: self.retention,
            materialize_tx: Arc::clone(&self.materialize_tx),
            materialize_worker: Arc::clone(&self.materialize_worker),
        }
    }
}

struct SnapshotManagerState {
    next_id: u64,
    snapshots: BTreeMap<u64, DbSnapshot>,
    completed: Vec<u64>,
    retained: HashSet<u64>,
}

impl SnapshotManager {
    pub(crate) fn new(file_manager: Arc<FileManager>, retention: Option<usize>) -> Self {
        Self {
            file_manager,
            state: Arc::new(Mutex::new(SnapshotManagerState {
                next_id: 0,
                snapshots: BTreeMap::new(),
                completed: Vec::new(),
                retained: HashSet::new(),
            })),
            retention,
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
        let Some(snapshot) = state.snapshots.get_mut(&id) else {
            return false;
        };
        snapshot.levels = db_state.lsm_version.levels.clone();
        snapshot.data_files = db_state
            .lsm_version
            .levels
            .iter()
            .flat_map(|level| level.files.iter().cloned())
            .collect();
        snapshot.seq_id = db_state.seq_id;
        snapshot.tracked_files = snapshot
            .data_files
            .iter()
            .filter_map(|file| self.file_manager.data_file_ref(file.file_id).ok())
            .collect();
        state.completed.push(id);
        true
    }

    pub(crate) fn retain_snapshot(&self, id: u64) -> bool {
        let mut state = self.state.lock().unwrap();
        if !state.snapshots.contains_key(&id) {
            return false;
        }
        state.retained.insert(id);
        true
    }

    pub(crate) fn process_retention(&self) -> Result<()> {
        let mut to_expire = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            let Some(retention) = self.retention else {
                return Ok(());
            };
            if state.completed.len() <= retention {
                return Ok(());
            }
            let keep_from = state.completed.len().saturating_sub(retention);
            to_expire.extend(state.completed.drain(..keep_from));
        }
        for id in to_expire {
            if !self.retain_snapshot(id) {
                let _ = self.expire_snapshot(id)?;
            }
        }
        Ok(())
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

    pub(crate) fn expire_snapshot(&self, id: u64) -> Result<bool> {
        let snapshot = {
            let mut state = self.state.lock().unwrap();
            state.completed.retain(|completed| *completed != id);
            state.retained.remove(&id);
            state.snapshots.remove(&id)
        };
        let Some(snapshot) = snapshot else {
            return Ok(false);
        };
        self.file_manager
            .remove_metadata_file(&snapshot_manifest_name(id))?;
        snapshot
            .tracked_files
            .iter()
            .for_each(|file| file.dereference());
        Ok(true)
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
        seq_id: snapshot.seq_id,
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
                        file_type: file.file_type.as_str().to_string(),
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

pub(crate) fn decode_manifest(bytes: &[u8]) -> Result<ManifestSnapshot> {
    serde_json::from_slice(bytes)
        .map_err(|err| Error::IoError(format!("Failed to decode manifest: {}", err)))
}

pub(crate) fn build_levels_from_manifest(
    file_manager: &Arc<FileManager>,
    manifest: ManifestSnapshot,
    read_only: bool,
) -> Result<Vec<Level>> {
    let mut levels = Vec::with_capacity(manifest.levels.len());
    for level in manifest.levels {
        let mut files = Vec::with_capacity(level.files.len());
        for file in level.files {
            let file_type = DataFileType::from_str(&file.file_type).map_err(Error::IoError)?;
            let start_key = from_hex(&file.start_key)?;
            let end_key = from_hex(&file.end_key)?;
            let path = file
                .path
                .unwrap_or_else(|| format!("data/{}.{}", file.file_id, file_type.as_str()));
            let tracked_id = if read_only {
                file_manager.register_data_file_readonly(file.file_id, path)?;
                TrackedFileId::detached(file.file_id)
            } else {
                file_manager.register_data_file(file.file_id, path)?;
                TrackedFileId::new(file_manager, file.file_id)
            };
            files.push(Arc::new(DataFile {
                file_type,
                start_key,
                end_key,
                file_id: file.file_id,
                tracked_id,
                seq: file.seq,
                size: file.size,
                meta_bytes: None,
            }));
        }
        levels.push(Level {
            ordinal: level.ordinal,
            tiered: level.tiered,
            files,
        });
    }
    Ok(levels)
}

pub(crate) fn from_hex(hex: &str) -> Result<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return Err(Error::IoError(format!(
            "Invalid hex string length: {}",
            hex.len()
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        let hi = hex_value(bytes[idx])?;
        let lo = hex_value(bytes[idx + 1])?;
        out.push((hi << 4) | lo);
        idx += 2;
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(10 + (byte - b'a')),
        b'A'..=b'F' => Ok(10 + (byte - b'A')),
        _ => Err(Error::IoError(format!(
            "Invalid hex character: {}",
            byte as char
        ))),
    }
}

pub(crate) fn snapshot_manifest_name(id: u64) -> String {
    format!("SNAPSHOT-{}", id)
}
