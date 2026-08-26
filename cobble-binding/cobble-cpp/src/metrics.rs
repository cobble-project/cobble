use crate::{database::NativeDatabase, ffi};

pub(crate) fn native_database_metrics(db: &NativeDatabase) -> Vec<ffi::NativeMetric> {
    db.db
        .metrics()
        .into_iter()
        .map(|sample| {
            let labels = sample
                .labels
                .into_iter()
                .map(|(key, value)| ffi::NativeMetricLabel { key, value })
                .collect();
            match sample.value {
                cobble_binding::MetricValue::Counter(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 0,
                    counter: value,
                    gauge: 0.0,
                    count: 0,
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                },
                cobble_binding::MetricValue::Gauge(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 1,
                    counter: 0,
                    gauge: value,
                    count: 0,
                    sum: 0.0,
                    min: 0.0,
                    max: 0.0,
                },
                cobble_binding::MetricValue::Histogram(value) => ffi::NativeMetric {
                    name: sample.name,
                    labels,
                    kind: 2,
                    counter: 0,
                    gauge: 0.0,
                    count: value.count,
                    sum: value.sum,
                    min: value.min,
                    max: value.max,
                },
            }
        })
        .collect()
}
