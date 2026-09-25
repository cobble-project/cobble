use crate::types::PickleReduction;
use cobble_binding::{MetricSample, MetricValue};
use pyo3::prelude::*;

type PickledMetricSample = (String, Vec<PyMetricLabel>, Py<PyAny>);

#[pyclass(
    name = "MetricLabel",
    module = "pycobble._native",
    frozen,
    from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyMetricLabel {
    #[pyo3(get)]
    key: String,
    #[pyo3(get)]
    value: String,
}

#[pymethods]
impl PyMetricLabel {
    #[staticmethod]
    fn _restore(key: String, value: String) -> Self {
        Self { key, value }
    }
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(String, String)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.key.clone(), self.value.clone()),
        ))
    }
}

#[pyclass(name = "CounterValue", module = "pycobble._native", frozen)]
pub(crate) struct PyCounterValue {
    #[pyo3(get)]
    value: u64,
}

#[pymethods]
impl PyCounterValue {
    #[staticmethod]
    fn _restore(value: u64) -> Self {
        Self { value }
    }
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(u64,)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.value,),
        ))
    }
}

#[pyclass(name = "GaugeValue", module = "pycobble._native", frozen)]
pub(crate) struct PyGaugeValue {
    #[pyo3(get)]
    value: f64,
}

#[pymethods]
impl PyGaugeValue {
    #[staticmethod]
    fn _restore(value: f64) -> Self {
        Self { value }
    }
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(f64,)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.value,),
        ))
    }
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

#[pymethods]
impl PyHistogramValue {
    #[staticmethod]
    fn _restore(count: u64, sum: f64, min: f64, max: f64) -> Self {
        Self {
            count,
            sum,
            min,
            max,
        }
    }
    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<(u64, f64, f64, f64)>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.count, self.sum, self.min, self.max),
        ))
    }
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
    #[staticmethod]
    fn _restore(
        name: String,
        labels: Vec<PyMetricLabel>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let value = if let Ok(counter) = value.extract::<PyRef<'_, PyCounterValue>>() {
            PyMetricValue::Counter(counter.value)
        } else if let Ok(gauge) = value.extract::<PyRef<'_, PyGaugeValue>>() {
            PyMetricValue::Gauge(gauge.value)
        } else if let Ok(histogram) = value.extract::<PyRef<'_, PyHistogramValue>>() {
            PyMetricValue::Histogram {
                count: histogram.count,
                sum: histogram.sum,
                min: histogram.min,
                max: histogram.max,
            }
        } else {
            return Err(crate::error::input_error("invalid pickled metric value"));
        };
        Ok(Self {
            name,
            labels,
            value,
        })
    }

    fn __reduce__(&self, py: Python<'_>) -> PyResult<PickleReduction<PickledMetricSample>> {
        Ok((
            py.get_type::<Self>().getattr("_restore")?.unbind(),
            (self.name.clone(), self.labels.clone(), self.value(py)?),
        ))
    }

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
