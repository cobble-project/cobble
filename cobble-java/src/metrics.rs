use cobble::{MetricSample, MetricValue};
use serde_json::{Map, Value, json};

pub(crate) fn metrics_json(samples: Vec<MetricSample>) -> String {
    let samples: Vec<Value> = samples
        .into_iter()
        .map(|sample| {
            let labels: Map<String, Value> = sample
                .labels
                .into_iter()
                .map(|(key, value)| (key, Value::String(value)))
                .collect();
            match sample.value {
                MetricValue::Counter(value) => json!({
                    "name": sample.name,
                    "labels": labels,
                    "type": "counter",
                    "value": value,
                }),
                MetricValue::Gauge(value) => json!({
                    "name": sample.name,
                    "labels": labels,
                    "type": "gauge",
                    "value": value,
                }),
                MetricValue::Histogram(value) => json!({
                    "name": sample.name,
                    "labels": labels,
                    "type": "histogram",
                    "value": {
                        "count": value.count,
                        "sum": value.sum,
                        "min": value.min,
                        "max": value.max,
                    },
                }),
            }
        })
        .collect();
    serde_json::to_string(&samples).expect("Cobble metric samples are JSON serializable")
}

#[cfg(test)]
#[path = "../tests/unit/metrics.rs"]
mod tests;
