use crate::error::{invalid_state, map_error};
use cobble_binding::{GlobalSnapshotManifest, ShardSnapshotRef, SingleDb};
use pyo3::prelude::*;
use std::sync::{Arc, Mutex, mpsc};

#[pyclass(
    name = "BucketRange",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyBucketRange {
    #[pyo3(get)]
    pub(crate) start_inclusive: u16,
    #[pyo3(get)]
    pub(crate) end_inclusive: u16,
}

#[pyclass(
    name = "ColumnFamilyId",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyColumnFamilyId {
    #[pyo3(get)]
    pub(crate) name: String,
    #[pyo3(get)]
    pub(crate) id: u8,
}

#[pyclass(
    name = "ShardSnapshot",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyShardSnapshot {
    ranges: Vec<PyBucketRange>,
    column_families: Vec<PyColumnFamilyId>,
    #[pyo3(get)]
    pub(crate) db_id: String,
    #[pyo3(get)]
    pub(crate) snapshot_id: u64,
    #[pyo3(get)]
    pub(crate) manifest_path: String,
    #[pyo3(get)]
    pub(crate) timestamp_seconds: u32,
    #[pyo3(get)]
    pub(crate) data_size_bytes: u64,
    #[pyo3(get)]
    pub(crate) incremental_data_size_bytes: u64,
}

#[pymethods]
impl PyShardSnapshot {
    #[getter]
    fn ranges(&self) -> Vec<PyBucketRange> {
        self.ranges.clone()
    }

    #[getter]
    fn column_families(&self) -> Vec<PyColumnFamilyId> {
        self.column_families.clone()
    }
}

#[pyclass(
    name = "GlobalSnapshot",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyGlobalSnapshot {
    #[pyo3(get)]
    pub(crate) version: u32,
    #[pyo3(get)]
    pub(crate) id: u64,
    #[pyo3(get)]
    pub(crate) total_buckets: u32,
    column_families: Vec<PyColumnFamilyId>,
    shards: Vec<PyShardSnapshot>,
    #[pyo3(get)]
    pub(crate) watermark_seconds: u32,
}

#[pymethods]
impl PyGlobalSnapshot {
    #[getter]
    fn column_families(&self) -> Vec<PyColumnFamilyId> {
        self.column_families.clone()
    }

    #[getter]
    fn shards(&self) -> Vec<PyShardSnapshot> {
        self.shards.clone()
    }
}

fn family((name, id): (String, u8)) -> PyColumnFamilyId {
    PyColumnFamilyId { name, id }
}

fn shard(value: ShardSnapshotRef) -> PyShardSnapshot {
    PyShardSnapshot {
        ranges: value
            .ranges
            .into_iter()
            .map(|range| PyBucketRange {
                start_inclusive: *range.start(),
                end_inclusive: *range.end(),
            })
            .collect(),
        column_families: value.column_family_ids.into_iter().map(family).collect(),
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    }
}

pub(crate) fn snapshot(value: GlobalSnapshotManifest) -> PyGlobalSnapshot {
    PyGlobalSnapshot {
        version: value.version,
        id: value.id,
        total_buckets: value.total_buckets,
        column_families: value.column_family_ids.into_iter().map(family).collect(),
        shards: value.shard_snapshots.into_iter().map(shard).collect(),
        watermark_seconds: value.watermark_seconds,
    }
}

type SnapshotResult = cobble_binding::Result<GlobalSnapshotManifest>;

#[pyclass(name = "PendingSnapshot", module = "pycobble._native")]
pub(crate) struct PyPendingSnapshot {
    id: u64,
    receiver: Mutex<Option<mpsc::Receiver<SnapshotResult>>>,
}

impl PyPendingSnapshot {
    pub(crate) fn start(db: &Arc<SingleDb>) -> PyResult<Self> {
        let (sender, receiver) = mpsc::channel();
        let id = db
            .snapshot_with_callback(move |result| {
                let _ = sender.send(result);
            })
            .map_err(map_error)?;
        Ok(Self {
            id,
            receiver: Mutex::new(Some(receiver)),
        })
    }

    pub(crate) fn wait_result(&self, py: Python<'_>) -> PyResult<PyGlobalSnapshot> {
        let receiver = self
            .receiver
            .lock()
            .expect("pending snapshot receiver mutex poisoned")
            .take()
            .ok_or_else(|| invalid_state("pending snapshot was already waited"))?;
        py.detach(move || {
            receiver
                .recv()
                .map_err(|_| invalid_state("snapshot completion channel closed"))?
                .map(snapshot)
                .map_err(map_error)
        })
    }
}

#[pymethods]
impl PyPendingSnapshot {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    fn wait(&self, py: Python<'_>) -> PyResult<PyGlobalSnapshot> {
        self.wait_result(py)
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBucketRange>()?;
    module.add_class::<PyColumnFamilyId>()?;
    module.add_class::<PyShardSnapshot>()?;
    module.add_class::<PyGlobalSnapshot>()?;
    module.add_class::<PyPendingSnapshot>()
}
