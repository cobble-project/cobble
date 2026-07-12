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
mod tests {
    use super::metrics_json;
    use cobble::{HistogramSnapshot, MetricSample, MetricValue};

    #[test]
    fn encodes_typed_metric_samples_for_java() {
        let json = metrics_json(vec![MetricSample {
            name: "ratio".to_string(),
            labels: vec![("compression".to_string(), "lz4".to_string())],
            value: MetricValue::Histogram(HistogramSnapshot {
                count: 2,
                sum: 3.0,
                min: 1.0,
                max: 2.0,
            }),
        }]);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value[0]["type"], "histogram");
        assert_eq!(value[0]["labels"]["compression"], "lz4");
        assert_eq!(value[0]["value"]["count"], 2);
    }
}
