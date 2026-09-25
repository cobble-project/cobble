use super::database::PyStructuredScanCursor;
use super::types::PyStructuredScanOptions;
use crate::buffer::InputBytes;
use crate::error::{input_error, map_error};
use crate::snapshot::{PyGlobalSnapshot, PyShardSnapshot, global_snapshot, shard};
use cobble_binding::Config;
use cobble_binding::structured::{StructuredScanPlan, StructuredScanSplit};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::path::PathBuf;

#[pyclass(
    name = "StructuredScanSplitBoundary",
    module = "pycobble._native",
    frozen
)]
pub(crate) struct PyStructuredScanSplitBoundary {
    #[pyo3(get)]
    bucket: u16,
    key: Vec<u8>,
}

#[pymethods]
impl PyStructuredScanSplitBoundary {
    #[getter]
    fn key(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.key).unbind()
    }
}

#[pyclass(name = "StructuredScanSplit", module = "pycobble._native", frozen)]
pub(crate) struct PyStructuredScanSplit {
    split: StructuredScanSplit,
}

impl PyStructuredScanSplit {
    fn new(split: StructuredScanSplit) -> Self {
        Self { split }
    }

    fn open_scanner_config(
        &self,
        config: Config,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        let options = options.map_or_else(Default::default, |value| value.inner.clone());
        if options.as_cobble().should_stop_at_block_boundary() {
            return Err(input_error(
                "stop_at_block_boundary is not supported for split scanners",
            ));
        }
        opendal::install_default();
        self.split
            .create_scanner(config, &options)
            .map(PyStructuredScanCursor::new_split)
            .map_err(map_error)
    }
}

#[pymethods]
impl PyStructuredScanSplit {
    #[getter]
    fn shard(&self) -> PyShardSnapshot {
        shard(self.split.shard.clone())
    }

    #[getter]
    fn start_inclusive(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.split
            .start
            .as_ref()
            .map(|value| PyBytes::new(py, value).unbind())
    }

    #[getter]
    fn end_exclusive(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
        self.split
            .end
            .as_ref()
            .map(|value| PyBytes::new(py, value).unbind())
    }

    #[getter]
    fn start_after_exclusive(&self) -> PyResult<Option<PyStructuredScanSplitBoundary>> {
        match (
            self.split.start_bucket,
            self.split.start_key_exclusive.as_ref(),
        ) {
            (Some(bucket), Some(key)) => Ok(Some(PyStructuredScanSplitBoundary {
                bucket,
                key: key.clone(),
            })),
            (None, None) => Ok(None),
            _ => Err(input_error(
                "structured scan split start bucket and key must be set together",
            )),
        }
    }

    #[getter]
    fn end_at_inclusive(&self) -> PyResult<Option<PyStructuredScanSplitBoundary>> {
        match (self.split.end_bucket, self.split.end_key_inclusive.as_ref()) {
            (Some(bucket), Some(key)) => Ok(Some(PyStructuredScanSplitBoundary {
                bucket,
                key: key.clone(),
            })),
            (None, None) => Ok(None),
            _ => Err(input_error(
                "structured scan split end bucket and key must be set together",
            )),
        }
    }

    fn split_after(
        &self,
        bucket: u16,
        key_inclusive: &Bound<'_, PyAny>,
    ) -> PyResult<PyStructuredScanSplitPartition> {
        let key = InputBytes::extract(key_inclusive)?;
        self.split
            .split_after(bucket, key.as_ref().to_vec())
            .map(|partition| PyStructuredScanSplitPartition {
                before: PyStructuredScanSplit::new(partition.before),
                after: PyStructuredScanSplit::new(partition.after),
            })
            .map_err(map_error)
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.split).map_err(|error| {
            input_error(format!("cannot encode structured scan split JSON: {error}"))
        })
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        serde_json::from_str(json)
            .map(Self::new)
            .map_err(|error| input_error(format!("invalid structured scan split JSON: {error}")))
    }

    #[pyo3(signature = (config_json, options=None))]
    fn open_scanner(
        &self,
        config_json: &str,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        self.open_scanner_config(
            Config::from_json_str(config_json).map_err(map_error)?,
            options,
        )
    }

    #[pyo3(signature = (config_path, options=None))]
    fn open_scanner_file(
        &self,
        config_path: PathBuf,
        options: Option<PyRef<'_, PyStructuredScanOptions>>,
    ) -> PyResult<PyStructuredScanCursor> {
        self.open_scanner_config(Config::from_path(config_path).map_err(map_error)?, options)
    }
}

#[pyclass(
    name = "StructuredScanSplitPartition",
    module = "pycobble._native",
    frozen
)]
pub(crate) struct PyStructuredScanSplitPartition {
    before: PyStructuredScanSplit,
    after: PyStructuredScanSplit,
}

#[pymethods]
impl PyStructuredScanSplitPartition {
    #[getter]
    fn before(&self) -> PyStructuredScanSplit {
        PyStructuredScanSplit::new(self.before.split.clone())
    }

    #[getter]
    fn after(&self) -> PyStructuredScanSplit {
        PyStructuredScanSplit::new(self.after.split.clone())
    }
}

#[pyclass(name = "StructuredScanPlan", module = "pycobble._native")]
pub(crate) struct PyStructuredScanPlan {
    snapshot: cobble_binding::GlobalSnapshotManifest,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
}

#[pymethods]
impl PyStructuredScanPlan {
    #[staticmethod]
    fn from_global_snapshot(snapshot: PyGlobalSnapshot) -> PyResult<Self> {
        Ok(Self {
            snapshot: global_snapshot(snapshot)?,
            start: None,
            end: None,
        })
    }

    fn with_start<'py>(
        mut slf: PyRefMut<'py, Self>,
        start_inclusive: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.start = Some(InputBytes::extract(start_inclusive)?.as_ref().to_vec());
        Ok(slf)
    }

    fn with_end<'py>(
        mut slf: PyRefMut<'py, Self>,
        end_exclusive: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.end = Some(InputBytes::extract(end_exclusive)?.as_ref().to_vec());
        Ok(slf)
    }

    fn without_start(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.start = None;
        slf
    }

    fn without_end(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.end = None;
        slf
    }

    fn splits(&self) -> Vec<PyStructuredScanSplit> {
        let mut plan = StructuredScanPlan::new(self.snapshot.clone());
        if let Some(start) = &self.start {
            plan = plan.with_start(start.clone());
        }
        if let Some(end) = &self.end {
            plan = plan.with_end(end.clone());
        }
        plan.splits()
            .into_iter()
            .map(PyStructuredScanSplit::new)
            .collect()
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyStructuredScanSplitBoundary>()?;
    module.add_class::<PyStructuredScanSplit>()?;
    module.add_class::<PyStructuredScanSplitPartition>()?;
    module.add_class::<PyStructuredScanPlan>()
}
