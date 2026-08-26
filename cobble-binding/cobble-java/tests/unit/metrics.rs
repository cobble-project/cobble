use super::metrics_json;
use cobble_binding::{HistogramSnapshot, MetricSample, MetricValue};

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
