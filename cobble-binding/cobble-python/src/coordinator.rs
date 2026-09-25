use crate::error::{input_error, map_error};
use crate::snapshot::{PyGlobalSnapshot, PyShardSnapshot, shard_snapshot_metadata, snapshot};
use cobble_binding::{Config, CoordinatorConfig, DbCoordinator, ShardSnapshotMetadata};
use pyo3::prelude::*;
use std::path::PathBuf;

#[pyclass(name = "DbCoordinator", module = "pycobble._native")]
pub(crate) struct PyDbCoordinator {
    coordinator: DbCoordinator,
}

impl PyDbCoordinator {
    fn open_config(config: Config) -> PyResult<Self> {
        opendal::install_default();
        DbCoordinator::open(CoordinatorConfig::from_config(&config))
            .map(|coordinator| Self { coordinator })
            .map_err(map_error)
    }

    fn validate_coverage(total_buckets: u32, shards: &[ShardSnapshotMetadata]) -> PyResult<()> {
        if total_buckets == 0 || total_buckets > u32::from(u16::MAX) + 1 {
            return Err(input_error("total_buckets must be in range 1..=65536"));
        }
        if shards.is_empty() {
            return Err(input_error("shard snapshots must not be empty"));
        }
        let mut covered = vec![false; total_buckets as usize];
        for shard in shards {
            for range in &shard.ranges {
                if u32::from(*range.end()) >= total_buckets {
                    return Err(input_error("shard range exceeds total_buckets"));
                }
                for bucket in range.clone() {
                    let slot = &mut covered[usize::from(bucket)];
                    if *slot {
                        return Err(input_error("shard ranges overlap"));
                    }
                    *slot = true;
                }
            }
        }
        if covered.iter().any(|covered| !covered) {
            return Err(input_error(
                "shard ranges must cover every bucket exactly once",
            ));
        }
        Ok(())
    }
}

#[pymethods]
impl PyDbCoordinator {
    #[staticmethod]
    fn open(py: Python<'_>, config_json: String) -> PyResult<Self> {
        py.detach(move || {
            Self::open_config(Config::from_json_str(&config_json).map_err(map_error)?)
        })
    }

    #[staticmethod]
    fn open_file(py: Python<'_>, config_path: PathBuf) -> PyResult<Self> {
        py.detach(move || Self::open_config(Config::from_path(config_path).map_err(map_error)?))
    }

    fn materialize_global_snapshot(
        &self,
        py: Python<'_>,
        total_buckets: u32,
        snapshot_id: u64,
        shards: Vec<PyShardSnapshot>,
    ) -> PyResult<PyGlobalSnapshot> {
        let shards = shards
            .into_iter()
            .map(shard_snapshot_metadata)
            .collect::<PyResult<Vec<_>>>()?;
        Self::validate_coverage(total_buckets, &shards)?;
        py.detach(|| {
            let global = self
                .coordinator
                .take_global_snapshot_with_id(total_buckets, shards, snapshot_id)
                .map_err(map_error)?;
            self.coordinator
                .materialize_global_snapshot(&global)
                .map_err(map_error)?;
            Ok(snapshot(global))
        })
    }

    fn get_global_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<PyGlobalSnapshot> {
        py.detach(|| {
            self.coordinator
                .load_global_snapshot(snapshot_id)
                .map(snapshot)
                .map_err(map_error)
        })
    }

    fn list_global_snapshots(&self, py: Python<'_>) -> PyResult<Vec<PyGlobalSnapshot>> {
        py.detach(|| {
            self.coordinator
                .list_global_snapshots()
                .map(|values| values.into_iter().map(snapshot).collect())
                .map_err(map_error)
        })
    }

    fn load_current_global_snapshot(&self, py: Python<'_>) -> PyResult<Option<PyGlobalSnapshot>> {
        py.detach(|| {
            self.coordinator
                .load_current_global_snapshot()
                .map(|value| value.map(snapshot))
                .map_err(map_error)
        })
    }

    fn retain_snapshot(&self, snapshot_id: u64) -> bool {
        self.coordinator.retain_snapshot(snapshot_id)
    }

    fn expire_snapshot(&self, py: Python<'_>, snapshot_id: u64) -> PyResult<bool> {
        py.detach(|| {
            self.coordinator
                .expire_snapshot(snapshot_id)
                .map_err(map_error)
        })
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDbCoordinator>()
}
