//! In-memory metrics registry implementation using the `metrics` crate.
use dashmap::DashMap;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricSample {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(HistogramSnapshot),
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

/// Thread-safe in-memory registry backing `Db::metrics()`.
#[derive(Default)]
struct MetricStore {
    entries: DashMap<Key, Arc<MetricEntry>>,
}

struct MetricEntry {
    inner: MetricInner,
}

enum MetricInner {
    /// Monotonic counter recorded with relaxed atomics.
    Counter(AtomicU64),
    /// Gauge stored under a mutex to support set/increment/decrement.
    Gauge(Mutex<f64>),
    /// Histogram snapshots with min/max/sum/count.
    Histogram(Mutex<HistogramState>),
}

#[derive(Clone, Copy)]
struct HistogramState {
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Default for HistogramState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: 0.0,
            max: 0.0,
        }
    }
}

impl HistogramState {
    /// Record a single observation and maintain min/max.
    fn record(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        if self.count == 1 {
            self.min = value;
            self.max = value;
        } else {
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
    }

    fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            count: self.count,
            sum: self.sum,
            min: self.min,
            max: self.max,
        }
    }
}

impl MetricEntry {
    fn counter() -> Self {
        Self {
            inner: MetricInner::Counter(AtomicU64::new(0)),
        }
    }

    fn gauge() -> Self {
        Self {
            inner: MetricInner::Gauge(Mutex::new(0.0)),
        }
    }

    fn histogram() -> Self {
        Self {
            inner: MetricInner::Histogram(Mutex::new(HistogramState::default())),
        }
    }

    fn increment_counter(&self, value: u64) {
        if let MetricInner::Counter(counter) = &self.inner {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }

    fn absolute_counter(&self, value: u64) {
        if let MetricInner::Counter(counter) = &self.inner {
            let mut current = counter.load(Ordering::Relaxed);
            while current < value {
                match counter.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    fn increment_gauge(&self, value: f64) {
        if let MetricInner::Gauge(gauge) = &self.inner {
            let mut guard = gauge.lock().unwrap();
            *guard += value;
        }
    }

    fn decrement_gauge(&self, value: f64) {
        if let MetricInner::Gauge(gauge) = &self.inner {
            let mut guard = gauge.lock().unwrap();
            *guard -= value;
        }
    }

    fn set_gauge(&self, value: f64) {
        if let MetricInner::Gauge(gauge) = &self.inner {
            let mut guard = gauge.lock().unwrap();
            *guard = value;
        }
    }

    fn record_histogram(&self, value: f64) {
        if let MetricInner::Histogram(histogram) = &self.inner {
            let mut guard = histogram.lock().unwrap();
            guard.record(value);
        }
    }

    fn snapshot(&self) -> MetricValue {
        match &self.inner {
            MetricInner::Counter(counter) => MetricValue::Counter(counter.load(Ordering::Relaxed)),
            MetricInner::Gauge(gauge) => MetricValue::Gauge(*gauge.lock().unwrap()),
            MetricInner::Histogram(histogram) => {
                MetricValue::Histogram(histogram.lock().unwrap().snapshot())
            }
        }
    }
}

impl MetricStore {
    fn register(&self, key: &Key, factory: fn() -> MetricEntry) -> Arc<MetricEntry> {
        self.entries
            .entry(key.clone())
            .or_insert_with(|| Arc::new(factory()))
            .clone()
    }
}

struct CounterHandle {
    entry: Arc<MetricEntry>,
}

impl CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        self.entry.increment_counter(value);
    }

    fn absolute(&self, value: u64) {
        self.entry.absolute_counter(value);
    }
}

struct GaugeHandle {
    entry: Arc<MetricEntry>,
}

impl GaugeFn for GaugeHandle {
    fn increment(&self, value: f64) {
        self.entry.increment_gauge(value);
    }

    fn decrement(&self, value: f64) {
        self.entry.decrement_gauge(value);
    }

    fn set(&self, value: f64) {
        self.entry.set_gauge(value);
    }
}

struct HistogramHandle {
    entry: Arc<MetricEntry>,
}

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.entry.record_histogram(value);
    }
}

struct MetricsRecorder {
    store: Arc<MetricStore>,
}

impl Recorder for MetricsRecorder {
    fn describe_counter(
        &self,
        _key_name: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }

    fn describe_gauge(&self, _key_name: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(
        &self,
        _key_name: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let entry = self.store.register(key, MetricEntry::counter);
        Counter::from_arc(Arc::new(CounterHandle { entry }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        let entry = self.store.register(key, MetricEntry::gauge);
        Gauge::from_arc(Arc::new(GaugeHandle { entry }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        let entry = self.store.register(key, MetricEntry::histogram);
        Histogram::from_arc(Arc::new(HistogramHandle { entry }))
    }
}

static METRICS_STORE: OnceLock<Arc<MetricStore>> = OnceLock::new();

/// Install the global recorder once and keep a handle to the registry.
pub(crate) fn init_metrics() {
    METRICS_STORE.get_or_init(|| {
        let store = Arc::new(MetricStore::default());
        let recorder = MetricsRecorder {
            store: Arc::clone(&store),
        };
        let _ = metrics::set_global_recorder(recorder);
        store
    });
}

/// Snapshot all recorded metrics, optionally filtering by db_id label.
pub(crate) fn snapshot_metrics(db_id: Option<&str>) -> Vec<MetricSample> {
    let Some(store) = METRICS_STORE.get() else {
        return Vec::new();
    };
    let mut samples = Vec::new();
    for entry in store.entries.iter() {
        let labels: Vec<(String, String)> = entry
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        if let Some(db_id) = db_id
            && !labels
                .iter()
                .any(|(key, value)| key == "db_id" && value == db_id)
        {
            continue;
        }
        samples.push(MetricSample {
            name: entry.key().name().to_string(),
            labels,
            value: entry.value().snapshot(),
        });
    }
    samples.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.labels.cmp(&right.labels))
    });
    samples
}
