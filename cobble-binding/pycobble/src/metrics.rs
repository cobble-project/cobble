use cobble_binding::{MetricSample, MetricValue};
use pyo3::prelude::*;

#[pyclass(
    name = "MetricLabel",
    module = "pycobble._native",
    frozen,
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyMetricLabel {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    value: String,
}

#[pyclass(name = "CounterValue", module = "pycobble._native", frozen)]
pub(crate) struct PyCounterValue {
    #[pyo3(get)]
    value: u64,
}

#[pyclass(name = "GaugeValue", module = "pycobble._native", frozen)]
pub(crate) struct PyGaugeValue {
    #[pyo3(get)]
    value: f64,
}

#[pyclass(name = "HistogramValue", module = "pycobble._native", frozen)]
pub(crate) struct PyHistogramValue {
    #[pyo3(get)]
    count: u64,
    #[pyo3(get)]
    sum: f64,
    #[pyo3(get)]
    min: f64,
    #[pyo3(get)]
    max: f64,
}

#[derive(Clone, Copy)]
enum PyMetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

#[pyclass(name = "MetricSample", module = "pycobble._native", frozen)]
pub(crate) struct PyMetricSample {
    #[pyo3(get)]
    name: String,
    labels: Vec<PyMetricLabel>,
    value: PyMetricValue,
}

#[pymethods]
impl PyMetricSample {
    #[getter]
    fn labels(&self) -> Vec<PyMetricLabel> {
        self.labels.clone()
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.value {
            PyMetricValue::Counter(value) => Ok(Py::new(py, PyCounterValue { value })?.into_any()),
            PyMetricValue::Gauge(value) => Ok(Py::new(py, PyGaugeValue { value })?.into_any()),
            PyMetricValue::Histogram {
                count,
                sum,
                min,
                max,
            } => Ok(Py::new(
                py,
                PyHistogramValue {
                    count,
                    sum,
                    min,
                    max,
                },
            )?
            .into_any()),
        }
    }
}

pub(crate) fn metrics(samples: Vec<MetricSample>) -> Vec<PyMetricSample> {
    samples
        .into_iter()
        .map(|sample| PyMetricSample {
            name: sample.name,
            labels: sample
                .labels
                .into_iter()
                .map(|(key, value)| PyMetricLabel { key, value })
                .collect(),
            value: match sample.value {
                MetricValue::Counter(value) => PyMetricValue::Counter(value),
                MetricValue::Gauge(value) => PyMetricValue::Gauge(value),
                MetricValue::Histogram(value) => PyMetricValue::Histogram {
                    count: value.count,
                    sum: value.sum,
                    min: value.min,
                    max: value.max,
                },
            },
        })
        .collect()
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyMetricLabel>()?;
    module.add_class::<PyCounterValue>()?;
    module.add_class::<PyGaugeValue>()?;
    module.add_class::<PyHistogramValue>()?;
    module.add_class::<PyMetricSample>()
}
