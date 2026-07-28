---
title: Metrics
parent: Reference
nav_order: 4
---

# Metrics

Cobble records per-database metrics in an in-process registry. Applications can poll a snapshot and export it to their monitoring system of choice.

## Getting Metrics

The Rust APIs return `Vec<MetricSample>`:

```rust
use cobble::MetricValue;

for sample in db.metrics() {
    match sample.value {
        MetricValue::Counter(value) => println!("{}: {}", sample.name, value),
        MetricValue::Gauge(value) => println!("{}: {}", sample.name, value),
        MetricValue::Histogram(value) => println!(
            "{}: count={} sum={} min={} max={}",
            sample.name, value.count, value.sum, value.min, value.max
        ),
    }
}
```

Metrics snapshots are available from:

- `Db::metrics()`
- `ReadOnlyDb::metrics()`
- `cobble_data_structure::StructuredDb::metrics()`

The Java raw and structured `Db` classes expose the same snapshot as an immutable `List<MetricSample>`:

```java
List<MetricSample> samples = db.metrics();
```

Java metric values implement `MetricValue` and are represented by `CounterMetricValue`, `GaugeMetricValue`, or `HistogramMetricValue`.

Each call returns the samples associated with that database's `db_id`. Counters are cumulative within the current process, gauges report current values, and histograms contain `count`, `sum`, `min`, and `max` rather than calculated percentiles.

## Common Labels

Every metric includes `db_id`. Some metric families have additional bounded labels:

| Label | Values | Meaning |
|-------|--------|---------|
| `db_id` | Database identifier | Identifies the database that owns the metric. |
| `kind` | Metric-specific | Identifies the block or cache entry kind. |
| `file` | `sst` | Identifies the data-file implementation. Cache hit/miss metrics currently cover SST reads. |
| `compression` | `none`, `lz4` | SST data-block compression algorithm. |
| `volume` | `0`, `1`, ... | Zero-based position of the volume in `Config.volumes`. Paths and credentials are never used as labels. |

Consumers should tolerate a series being absent until its subsystem or code path has been initialized.

## Compaction

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `compactions_total` | Counter | None | Compactions completed successfully. Remote compaction is counted on the writer after the result is applied locally. |
| `compaction_read_bytes_total` | Counter | None | Cumulative input data-file bytes attributed to compaction tasks. |
| `compaction_write_bytes_total` | Counter | None | Cumulative output data-file bytes produced by compaction tasks. |

Remote failures and skipped attempts do not increment `compactions_total`. If remote compaction falls back to local execution, the completed local task is counted normally.

## Memtable And Write Path

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `memtable_flushes_total` | Counter | None | Successfully completed memtable flushes. |
| `memtable_flush_bytes_total` | Counter | None | Cumulative size of data files produced by memtable flushes. |
| `write_stall_waits_total` | Counter | None | Number of times a writer waited for write-stall pressure to clear. |

`write_stall_waits_total` counts waits, not unique stall periods or stall duration.

## Block Cache

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `block_cache_hits_total` | Counter | `file`, `kind` | Successful SST block-cache lookups. `kind` is `data`, `index`, or `filter`. |
| `block_cache_misses_total` | Counter | `file`, `kind` | SST block-cache lookups that required a storage read. `kind` is `data`, `index`, or `filter`. |
| `block_cache_usage_bytes` | Gauge | `kind` | Current in-memory block-cache weight. `kind` is `data`, `index`, `filter`, or `parquet_data`. |

For usage accounting, top-level and partitioned index entries are combined as `index`; filter index and partition entries are combined as `filter`.

## SST Writer

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `sst_block_compression_ratio` | Histogram | `compression` | Distribution of encoded SST data-block bytes divided by the uncompressed block bytes. Lower values indicate better compression. |

The encoded size includes block framing and an optional checksum trailer, so the ratio can be greater than `1.0`, particularly when `compression=none` or for small blocks.

## Files And Storage

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `data_files_tracked` | Gauge | None | Number of data files currently tracked by the file manager. |
| `metadata_files_tracked` | Gauge | None | Number of metadata files currently tracked by the file manager. |
| `storage_file_bytes` | Gauge | `volume` | Current data and metadata file bytes accounted to a configured storage volume. |

`storage_file_bytes` follows Cobble's file lifecycle accounting. It does not scan the underlying filesystem or include unrelated files created outside Cobble.

## Storage Offload

| Metric | Type | Additional labels | Description |
|--------|------|-------------------|-------------|
| `offload_jobs_scheduled_total` | Counter | None | Primary-tiering jobs accepted for background execution, including backfill. |
| `offload_jobs_completed_total` | Counter | None | Primary-tiering jobs completed by either copying a file or promoting an existing replica. |
| `offload_jobs_failed_total` | Counter | None | Primary-tiering jobs that failed during execution. |
| `offload_jobs_noop_total` | Counter | None | Scheduled jobs that completed without moving a file because it was no longer eligible or referenced. |
| `offload_bytes_moved_total` | Counter | None | Cumulative bytes physically copied by completed primary-tiering jobs. |
| `offload_promotions_total` | Counter | None | Completed tiering jobs that reused and promoted an existing snapshot replica instead of copying data. |

Promotion jobs increment `offload_jobs_completed_total` and `offload_promotions_total`, but not `offload_bytes_moved_total`.
