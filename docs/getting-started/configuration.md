---
title: Configuration
parent: Getting Started
nav_order: 2
---

# Configuration

Before opening any database, you must create a `Config` and define at least one storage volume.

## Storage Feature Flags

Cobble storage backends are provided by Apache OpenDAL and are gated by Cargo features.

- Local `file://` is always enabled (no feature required)
- Optional feature set: `storage-alluxio`, `storage-cos`, `storage-oss`, `storage-s3`, `storage-ftp`, `storage-hdfs`, `storage-sftp`
- Enable all optional backends: `storage-all`
- Crates that depend on `cobble` in this workspace also expose the same `storage-*` features and forward them to `cobble`, so you can enable backend features from those crates as well.

Example:

```toml
[dependencies]
cobble = { version = "0.1.0", default-features = false, features = ["storage-s3"] }
```

If a volume URL uses a scheme whose feature is not enabled, Cobble will fail to initialize that volume.

## Volume Categories

Cobble organizes storage into **volumes**, each tagged with one or more usage kinds:

| Kind | Purpose |
|------|---------|
| `PrimaryDataPriorityHigh` | Main data files (SST, Parquet, VLOG) with highest priority |
| `PrimaryDataPriorityMedium` | Medium-priority data files |
| `PrimaryDataPriorityLow` | Low-priority data files (candidates for offload) |
| `Meta` | Metadata — snapshot manifests, schema files, pointers |
| `Snapshot` | Snapshot materialization target storage |
| `Cache` | Disk tier for hybrid block cache |
| `Readonly` | Read-only source for historical data loading |

A `VolumeDescriptor` specifies a base directory URL and a bitmask of supported usage kinds. The only difference between the priority levels is how the file manager selects volumes for new files and when to offload files as volumes fill up.

For a deeper explanation of volume roles, tiered offload behavior, and snapshot volume strategies, see [Multi-Volume Storage](../architecture/multi-volume).

## Minimal Configuration

A single local volume with `PrimaryDataPriorityHigh + Meta` is sufficient to get started:

```rust
use cobble::Config;

let config = Config::default();
// Default volume: file:///tmp/cobble with PrimaryDataPriorityHigh + Meta
```

Or configure a custom path:

```rust
use cobble::{Config, VolumeDescriptor};

let mut config = Config::default();
config.volumes = VolumeDescriptor::single_volume("file:///data/cobble");
```

{: .warning }
> **Restore/resume requires access to ALL files referenced by the target snapshot.** This includes manifests, schema files, SST/Parquet data files, and VLOG files. Ensure that all volumes used when the snapshot was taken are still accessible during recovery.

## Multi-Volume Setup

For production deployments, you can separate data and metadata across different storage backends:

```rust
use cobble::{Config, VolumeDescriptor, VolumeUsageKind};

let mut config = Config::default();
config.volumes = vec![
    // Fast SSD for hot data and metadata
    VolumeDescriptor::new(
        "file:///ssd/cobble",
        vec![
            VolumeUsageKind::PrimaryDataPriorityHigh,
            VolumeUsageKind::Meta,
        ],
    ),
    // Larger HDD for cold data
    VolumeDescriptor::new(
        "file:///hdd/cobble",
        vec![VolumeUsageKind::PrimaryDataPriorityLow],
    ),
    // Snapshot materialization
    VolumeDescriptor::new(
        "file:///shared/snapshots",
        vec![VolumeUsageKind::Snapshot],
    ),
];
```

## Key Configuration Parameters

All capacity/size fields in `Config` (such as `memtable_capacity`, `l1_base_bytes`, `base_file_size`, `block_cache_size`, and `parquet_row_group_size_bytes`) use `Size`. In config files, you can write raw bytes (for example `67108864`) or unit strings (for example `"64MiB"`).

### Memtable

| Parameter | Default | Description |
|-----------|---------|-------------|
| `memtable_capacity` | 64 MB | Maximum bytes before flush to L0 |
| `memtable_buffer_count` | 2 | Number of memtable buffers (active + immutable) |
| `memtable_type` | `Hash` | Implementation: `Hash`, `Skiplist`, or `Vec` |

### LSM Tree

| Parameter | Default | Description |
|-----------|---------|-------------|
| `l0_file_limit` | 4 | L0 file count that triggers compaction |
| `write_stall_limit` | `None` | Max immutable + L0 files before write stall (auto-calculated if unset) |
| `l1_base_bytes` | 64 MB | Target size for level 1 |
| `level_size_multiplier` | 10 | Size multiplier per level (L2 = 640MB, L3 = 6.4GB, ...) |
| `max_level` | 6 | Maximum number of levels |

### SST / Parquet

| Parameter | Default | Description |
|-----------|---------|-------------|
| `data_file_type` | `SSTable` | Output format: `SSTable` or `Parquet` |
| `base_file_size` | 64 MB | Target SST/Parquet file size |
| `sst_bloom_filter_enabled` | `false` | Enable bloom filters for point lookups |
| `sst_bloom_bits_per_key` | 10 | Bits per key for bloom filters |
| `sst_partitioned_index` | `false` | Enable two-level index for large files |
| `sst_compression_by_level` | `[None, None, Lz4]` | Compression algorithm per level |
| `parquet_row_group_size_bytes` | 256 KB | Parquet row group size |

### Block Cache

| Parameter | Default | Description |
|-----------|---------|-------------|
| `block_cache_size` | 64 MB | In-memory block cache size (0 = disabled) |
| `block_cache_hybrid_enabled` | `false` | Enable memory + disk hybrid cache |
| `block_cache_hybrid_disk_size` | `None` | Disk cache capacity (defaults to `block_cache_size`) |

### Value Separation

| Parameter | Default | Description |
|-----------|---------|-------------|
| `value_separation_threshold` | `None` | Byte threshold to separate values into VLOG (`None` disables separation) |

### TTL

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ttl_enabled` | `false` | Enable TTL metadata processing |
| `default_ttl_seconds` | `None` | Default TTL for entries without explicit TTL |
| `time_provider` | `System` | Time source: `System` (wall clock) or `Manual` |

### Snapshots

| Parameter | Default | Description |
|-----------|---------|-------------|
| `snapshot_on_flush` | `false` | Automatically take snapshot after every memtable flush |
| `snapshot_retention` | `None` | Keep only the N most recent snapshots |
| `active_memtable_incremental_snapshot_ratio` | 0.0 | Ratio for incremental memtable snapshots (0 = disabled) |

### Schema

| Parameter | Default | Description |
|-----------|---------|-------------|
| `num_columns` | 1 | Number of value columns |
| `total_buckets` | 1 | Total bucket count for sharding (1–65536) |

### Volume Offload

| Parameter | Default | Description |
|-----------|---------|-------------|
| `primary_volume_write_stop_watermark` | 0.95 | Volume usage ratio to stop writes |
| `primary_volume_offload_trigger_watermark` | 0.85 | Volume usage ratio to trigger file offload |
| `primary_volume_offload_policy` | `Priority` | Offload policy: `LargestFile` or `Priority` |

### Logging

| Parameter | Default | Description |
|-----------|---------|-------------|
| `log_path` | `None` | File path for logs (local only) |
| `log_console` | `false` | Enable console log output |
| `log_level` | `Info` | Log level: `Trace`, `Debug`, `Info`, `Warn`, `Error`, `Off` |

## Loading Config from File

Cobble supports loading configuration from YAML, JSON, TOML, or INI files:

```rust
let config = Config::from_path("config.yaml")?;
```

The file format is detected by extension (`.yaml`/`.yml`, `.json`, `.toml`, `.ini`).
