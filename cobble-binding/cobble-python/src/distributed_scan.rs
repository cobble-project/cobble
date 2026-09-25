use crate::buffer::InputBytes;
use crate::error::{input_error, map_error};
use crate::options::PyScanOptions;
use crate::scan::PyScanCursor;
use crate::snapshot::{PyGlobalSnapshot, PyShardSnapshot, global_snapshot, shard, snapshot};
use crate::types::PickleReduction;
use cobble_binding::{Config, ScanPlan, ScanSplit};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::path::PathBuf;

type PickledPlan = (PyGlobalSnapshot, Option<Py<PyBytes>>, Option<Py<PyBytes>>);

#[pyclass(name = "ScanSplitBoundary", module = "pycobble._native", frozen)]
pub(crate) struct PyScanSplitBoundary {
    #[pyo3(get)]
    bucket: u16,
    key: Vec<u8>,
}

#[pymethods]
impl PyScanSplitBoundary {
    #[staticmethod]
    fn _restore(bucket: u16, key: Vec<u8>) -> Self {
        Self { bucket, key }
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(u16, Py<PyBytes>)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.bucket, PyBytes::new(py, &self.key).unbind()),
        ))
    }

    #[getter]
    fn key(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, &self.key).unbind()
    }
}

#[pyclass(name = "ScanSplit", module = "pycobble._native", frozen)]
pub(crate) struct PyScanSplit {
    split: ScanSplit,
}

impl PyScanSplit {
    fn new(split: ScanSplit) -> Self {
        Self { split }
    }
}

#[pymethods]
impl PyScanSplit {
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(String,)>> {
        Ok((
            py.get_type::<Self>().getattr("from_json")?.unbind(),
            (self.to_json()?,),
        ))
    }

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
    fn start_after_exclusive(&self) -> PyResult<Option<PyScanSplitBoundary>> {
        match (
            self.split.start_bucket,
            self.split.start_key_exclusive.as_ref(),
        ) {
            (Some(bucket), Some(key)) => Ok(Some(PyScanSplitBoundary {
                bucket,
                key: key.clone(),
            })),
            (None, None) => Ok(None),
            _ => Err(input_error(
                "scan split start bucket and key must be set together",
            )),
        }
    }

    #[getter]
    fn end_at_inclusive(&self) -> PyResult<Option<PyScanSplitBoundary>> {
        match (self.split.end_bucket, self.split.end_key_inclusive.as_ref()) {
            (Some(bucket), Some(key)) => Ok(Some(PyScanSplitBoundary {
                bucket,
                key: key.clone(),
            })),
            (None, None) => Ok(None),
            _ => Err(input_error(
                "scan split end bucket and key must be set together",
            )),
        }
    }

    fn split_after(
        &self,
        bucket: u16,
        key_inclusive: &Bound<'_, PyAny>,
    ) -> PyResult<PyScanSplitPartition> {
        let key = InputBytes::extract(key_inclusive)?;
        let partition = self
            .split
            .split_after(bucket, key.as_ref().to_vec())
            .map_err(map_error)?;
        Ok(PyScanSplitPartition {
            before: PyScanSplit::new(partition.before),
            after: PyScanSplit::new(partition.after),
        })
    }

    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.split)
            .map_err(|error| input_error(format!("cannot encode scan split JSON: {error}")))
    }

    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        serde_json::from_str(json)
            .map(Self::new)
            .map_err(|error| input_error(format!("invalid scan split JSON: {error}")))
    }

    #[pyo3(signature = (config_json, options=None))]
    fn open_scanner(
        &self,
        config_json: &str,
        options: Option<PyRef<'_, PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        let config = Config::from_json_str(config_json).map_err(map_error)?;
        self.open_scanner_config(config, options)
    }

    #[pyo3(signature = (config_path, options=None))]
    fn open_scanner_file(
        &self,
        config_path: PathBuf,
        options: Option<PyRef<'_, PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        let config = Config::from_path(config_path).map_err(map_error)?;
        self.open_scanner_config(config, options)
    }
}

impl PyScanSplit {
    fn open_scanner_config(
        &self,
        config: Config,
        options: Option<PyRef<'_, PyScanOptions>>,
    ) -> PyResult<PyScanCursor> {
        let options = options.map_or_else(Default::default, |options| options.inner.clone());
        if options.should_stop_at_block_boundary() {
            return Err(input_error(
                "stop_at_block_boundary is not supported for split scanners",
            ));
        }
        opendal::install_default();
        self.split
            .create_scanner(config, &options)
            .map(PyScanCursor::new_split)
            .map_err(map_error)
    }
}

#[pyclass(name = "ScanSplitPartition", module = "pycobble._native", frozen)]
pub(crate) struct PyScanSplitPartition {
    before: PyScanSplit,
    after: PyScanSplit,
}

#[pymethods]
impl PyScanSplitPartition {
    #[staticmethod]
    fn _restore(before: PyRef<'_, PyScanSplit>, after: PyRef<'_, PyScanSplit>) -> Self {
        Self {
            before: PyScanSplit::new(before.split.clone()),
            after: PyScanSplit::new(after.split.clone()),
        }
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(PyScanSplit, PyScanSplit)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.before(), self.after()),
        ))
    }

    #[getter]
    fn before(&self) -> PyScanSplit {
        PyScanSplit::new(self.before.split.clone())
    }

    #[getter]
    fn after(&self) -> PyScanSplit {
        PyScanSplit::new(self.after.split.clone())
    }
}

#[pyclass(name = "ScanPlan", module = "pycobble._native")]
pub(crate) struct PyScanPlan {
    snapshot: cobble_binding::GlobalSnapshotManifest,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
}

#[pymethods]
impl PyScanPlan {
    #[staticmethod]
    fn _restore(
        snapshot: PyGlobalSnapshot,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> PyResult<Self> {
        Ok(Self {
            snapshot: global_snapshot(snapshot)?,
            start,
            end,
        })
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledPlan>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (
                snapshot(self.snapshot.clone()),
                self.start
                    .as_ref()
                    .map(|value| PyBytes::new(py, value).unbind()),
                self.end
                    .as_ref()
                    .map(|value| PyBytes::new(py, value).unbind()),
            ),
        ))
    }

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

    fn splits(&self) -> Vec<PyScanSplit> {
        let mut plan = ScanPlan::new(self.snapshot.clone());
        if let Some(start) = &self.start {
            plan = plan.with_start(start.clone());
        }
        if let Some(end) = &self.end {
            plan = plan.with_end(end.clone());
        }
        plan.splits().into_iter().map(PyScanSplit::new).collect()
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyScanSplitBoundary>()?;
    module.add_class::<PyScanSplit>()?;
    module.add_class::<PyScanSplitPartition>()?;
    module.add_class::<PyScanPlan>()
}
