use crate::error::input_error;
use crate::error::{invalid_state, map_error};
use cobble_binding::{Db, GlobalSnapshotManifest, ShardSnapshotInput, ShardSnapshotRef, SingleDb};
use pyo3::prelude::*;
use std::collections::BTreeMap;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex, mpsc};

#[pyclass(
    name = "BucketRange",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyBucketRange {
    #[pyo3(get)]
    pub(crate) start_inclusive: u16,
    #[pyo3(get)]
    pub(crate) end_inclusive: u16,
}

#[pymethods]
impl PyBucketRange {
    #[new]
    fn new(start_inclusive: u16, end_inclusive: u16) -> PyResult<Self> {
        if start_inclusive > end_inclusive {
            return Err(crate::error::input_error("bucket range is reversed"));
        }
        Ok(Self {
            start_inclusive,
            end_inclusive,
        })
    }
}

#[pyclass(
    name = "ColumnFamilyId",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyColumnFamilyId {
    #[pyo3(get)]
    pub(crate) name: String,
    #[pyo3(get)]
    pub(crate) id: u8,
}

#[pymethods]
impl PyColumnFamilyId {
    #[new]
    fn new(name: String, id: u8) -> PyResult<Self> {
        if name.is_empty() {
            return Err(input_error("column family name must not be empty"));
        }
        Ok(Self { name, id })
    }
}

#[pyclass(
    name = "ShardSnapshot",
    module = "pycobble._native",
    frozen,
    from_py_object
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
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (ranges, column_families, db_id, snapshot_id, manifest_path, timestamp_seconds, data_size_bytes, incremental_data_size_bytes))]
    fn new(
        ranges: Vec<PyBucketRange>,
        column_families: Vec<PyColumnFamilyId>,
        db_id: String,
        snapshot_id: u64,
        manifest_path: String,
        timestamp_seconds: u32,
        data_size_bytes: u64,
        incremental_data_size_bytes: u64,
    ) -> PyResult<Self> {
        let value = Self {
            ranges,
            column_families,
            db_id,
            snapshot_id,
            manifest_path,
            timestamp_seconds,
            data_size_bytes,
            incremental_data_size_bytes,
        };
        shard_snapshot_input(value.clone())?;
        Ok(value)
    }

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
    from_py_object
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

pub(crate) fn shard(value: ShardSnapshotRef) -> PyShardSnapshot {
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

pub(crate) fn shard_input(value: ShardSnapshotInput) -> PyShardSnapshot {
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

fn native_ranges(values: Vec<PyBucketRange>) -> PyResult<Vec<RangeInclusive<u16>>> {
    if values.is_empty() {
        return Err(input_error("shard snapshot ranges must not be empty"));
    }
    values
        .into_iter()
        .map(|value| {
            if value.start_inclusive > value.end_inclusive {
                return Err(input_error("shard snapshot range is reversed"));
            }
            Ok(value.start_inclusive..=value.end_inclusive)
        })
        .collect()
}

fn native_families(values: Vec<PyColumnFamilyId>) -> PyResult<BTreeMap<String, u8>> {
    let mut by_name = BTreeMap::new();
    let mut by_id = BTreeMap::new();
    for value in values {
        if value.name.is_empty() {
            return Err(input_error("column family name must not be empty"));
        }
        if by_name.insert(value.name.clone(), value.id).is_some() {
            return Err(input_error("duplicate column family name"));
        }
        if by_id.insert(value.id, value.name).is_some() {
            return Err(input_error("duplicate column family id"));
        }
    }
    Ok(by_name)
}

pub(crate) fn shard_snapshot_input(value: PyShardSnapshot) -> PyResult<ShardSnapshotInput> {
    if value.db_id.is_empty() || value.manifest_path.is_empty() {
        return Err(input_error(
            "shard snapshot db_id and manifest_path must not be empty",
        ));
    }
    Ok(ShardSnapshotInput {
        ranges: native_ranges(value.ranges)?,
        column_family_ids: native_families(value.column_families)?,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    })
}

pub(crate) fn shard_snapshot_reference(value: PyShardSnapshot) -> PyResult<ShardSnapshotRef> {
    let value = shard_snapshot_input(value)?;
    Ok(ShardSnapshotRef {
        ranges: value.ranges,
        column_family_ids: value.column_family_ids,
        db_id: value.db_id,
        snapshot_id: value.snapshot_id,
        manifest_path: value.manifest_path,
        timestamp_seconds: value.timestamp_seconds,
        data_size_bytes: value.data_size_bytes,
        incremental_data_size_bytes: value.incremental_data_size_bytes,
    })
}

pub(crate) fn global_snapshot(value: PyGlobalSnapshot) -> PyResult<GlobalSnapshotManifest> {
    let mut column_family_ids = BTreeMap::new();
    for family in value.column_families {
        if column_family_ids.insert(family.name, family.id).is_some() {
            return Err(input_error("duplicate global column family name"));
        }
    }
    Ok(GlobalSnapshotManifest {
        version: value.version,
        id: value.id,
        total_buckets: value.total_buckets,
        column_family_ids,
        shard_snapshots: value
            .shards
            .into_iter()
            .map(shard_snapshot_reference)
            .collect::<PyResult<_>>()?,
        watermark_seconds: value.watermark_seconds,
    })
}

type SnapshotResult = cobble_binding::Result<GlobalSnapshotManifest>;

#[pyclass(name = "PendingSnapshot", module = "pycobble._native")]
pub(crate) struct PyPendingSnapshot {
    id: u64,
    receiver: Mutex<Option<mpsc::Receiver<SnapshotResult>>>,
}

type ShardSnapshotResult = cobble_binding::Result<ShardSnapshotInput>;

#[pyclass(name = "PendingShardSnapshot", module = "pycobble._native")]
pub(crate) struct PyPendingShardSnapshot {
    id: u64,
    receiver: Mutex<Option<mpsc::Receiver<ShardSnapshotResult>>>,
}

impl PyPendingShardSnapshot {
    pub(crate) fn start(db: &Arc<Db>) -> PyResult<Self> {
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

    pub(crate) fn wait_result(&self, py: Python<'_>) -> PyResult<PyShardSnapshot> {
        let receiver = self
            .receiver
            .lock()
            .expect("pending shard snapshot receiver mutex poisoned")
            .take()
            .ok_or_else(|| invalid_state("pending shard snapshot was already waited"))?;
        py.detach(move || {
            receiver
                .recv()
                .map_err(|_| invalid_state("shard snapshot completion channel closed"))?
                .map(shard_input)
                .map_err(map_error)
        })
    }
}

#[pymethods]
impl PyPendingShardSnapshot {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    fn wait(&self, py: Python<'_>) -> PyResult<PyShardSnapshot> {
        self.wait_result(py)
    }
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
    module.add_class::<PyPendingSnapshot>()?;
    module.add_class::<PyPendingShardSnapshot>()
}
