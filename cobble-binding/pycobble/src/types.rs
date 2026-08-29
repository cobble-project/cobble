use cobble_binding::RecoveryMode;
use pyo3::prelude::*;

#[pyclass(
    name = "RecoveryMode",
    module = "pycobble._native",
    eq,
    eq_int,
    from_py_object
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PyRecoveryMode {
    SnapshotOnly = 0,
    LatestWithWal = 1,
}

impl From<PyRecoveryMode> for RecoveryMode {
    fn from(value: PyRecoveryMode) -> Self {
        match value {
            PyRecoveryMode::SnapshotOnly => Self::SnapshotOnly,
            PyRecoveryMode::LatestWithWal => Self::LatestWithWal,
        }
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyRecoveryMode>()
}
