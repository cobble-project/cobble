---
title: All Configuration
parent: Reference
nav_order: 1
---

# All Configuration

Complete reference for all Cobble configuration parameters.

## Config

The main `Config` struct. Can be created programmatically or loaded from a file (`Config::from_path("config.yaml")`).

Supported file formats: YAML (`.yaml`/`.yml`), JSON (`.json`), TOML (`.toml`), INI (`.ini`).

A useful template would be (take care of the code version): https://github.com/cobble-project/cobble/blob/main/template/config.yaml

### Storage Feature Flags (OpenDAL)

Cobble exposes a focused set of optional OpenDAL backend features.

- Local `file://` is always enabled (no feature required)
- Enable all optional backends: `storage-all`
- Workspace crates that depend on `cobble` (`cobble-cli`, `cobble-web-monitor`, `cobble-cluster`, `cobble-bench`, `cobble-data-structure`, `cobble-java`) re-expose and forward the same `storage-*` features.

Optional feature mapping:

| Cobble Feature | OpenDAL Service |
|---|---|
| `storage-alluxio` | `services-alluxio` |
| `storage-cos` | `services-cos` |
| `storage-ftp` | `services-ftp` |
| `storage-hdfs` | `services-hdfs` |
| `storage-oss` | `services-oss` |
| `storage-s3` | `services-s3` |
| `storage-sftp` | `services-sftp` |

> Windows note: `storage-hdfs` and `storage-sftp` are currently unsupported.

### Size Value Format

Size-related entries (cache size, memtable size, file size, thresholds, etc.) support:

- raw bytes (`67108864`)
- unit strings (`"64MB"`, `"64MiB"`)

Supported units: `B`, `KB`, `MB`, `GB`, `TB`, `PB`, `KiB`, `MiB`, `GiB`, `TiB`, `PiB`.

### Storage

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `volumes` | `Vec<VolumeDescriptor>` | Single local volume at `/tmp/cobble` | Storage volume descriptors |
| `data_file_type` | `DataFileType` | `SSTable` | Output format: `SSTable` or `Parquet` |

### Memtable

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `memtable_capacity` | `Size` | `64MiB` | Max memtable size before flush |
| `memtable_buffer_count` | `usize` | 2 | Number of memtable buffers (active + immutable) |
| `memtable_type` | `MemtableType` | `Hash` | Implementation: `Hash`, `Skiplist`, `Vec` |

### LSM Tree

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `l0_file_limit` | `usize` | 4 | L0 file count that triggers compaction |
| `write_stall_limit` | `Option<usize>` | `None` | Max immutable+L0 files before stall. Auto: `min(l0+4, l0×2)` |
| `l1_base_bytes` | `Size` | `64MiB` | Target size for level 1 |
| `level_size_multiplier` | `usize` | 10 | Size multiplier per level |
| `max_level` | `u8` | 6 | Maximum number of LSM levels |

### SST Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `base_file_size` | `Size` | `64MiB` | Target output file size |
| `sst_bloom_filter_enabled` | `bool` | `false` | Enable bloom filter per SST file |
| `sst_bloom_bits_per_key` | `u32` | 10 | Bits per key for bloom filter |
| `sst_partitioned_index` | `bool` | `false` | Enable two-level partitioned index |
| `sst_data_block_restart_interval` | `usize` | 16 | Restart interval in SST data blocks (`>1` enables prefix compression, `1` disables; range `1..=65535`) |
| `sst_compression_by_level` | `Vec<SstCompressionAlgorithm>` | `[None, None, Lz4]` | Compression per level |

### Parquet

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parquet_row_group_size_bytes` | `Size` | `256KiB` | Row group size |

### Block Cache

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `block_cache_size` | `Size` | `64MiB` | In-memory cache size (`0` also disables cache) |
| `block_cache_hybrid_enabled` | `bool` | `false` | Enable memory + disk hybrid cache |
| `block_cache_hybrid_disk_size` | `Option<Size>` | `None` | Disk tier capacity (defaults to memory size) |

### Compaction

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `compaction_policy` | `CompactionPolicyKind` | `RoundRobin` | Policy: `RoundRobin` or `MinOverlap` |
| `compaction_read_ahead_enabled` | `bool` | `true` | Buffered reads during compaction |
| `compaction_remote_addr` | `Option<String>` | `None` | Remote compaction server address (host:port) |
| `compaction_threads` | `usize` | 4 | Compaction thread pool size |
| `compaction_remote_timeout_ms` | `u64` | 300,000 | Remote compaction timeout (milliseconds) |
| `compaction_server_max_concurrent` | `usize` | 4 | Max concurrent tasks on remote server |
| `compaction_server_max_queued` | `usize` | 64 | Max queued tasks before rejecting |

### Value Separation

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value_separation_threshold` | `Option<Size>` | `None` | Byte threshold for VLOG separation (`None` = disabled) |

### TTL

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ttl_enabled` | `bool` | `false` | Enable TTL metadata processing |
| `default_ttl_seconds` | `Option<u32>` | `None` | Default TTL for entries (None = no expiration) |
| `time_provider` | `TimeProviderKind` | `System` | Time source: `System` or `Manual` |

### Snapshots

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `snapshot_on_flush` | `bool` | `false` | Auto-snapshot after each memtable flush |
| `snapshot_retention` | `Option<usize>` | `None` | Keep only N most recent snapshots |
| `active_memtable_incremental_snapshot_ratio` | `f64` | 0.0 | Ratio for incremental memtable snapshots (0 = disabled) |

### Schema

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `num_columns` | `usize` | 1 | Initial number of columns in the default column family when creating a new DB |
| `total_buckets` | `u32` | 1 | Total buckets for sharding (1–65536) |

Named column families are added later through schema evolution. Reopen, restore, read-only, and compaction paths use the persisted schema rather than reapplying `num_columns`.

### Volume Offload

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `primary_volume_write_stop_watermark` | `f64` | 0.95 | Usage ratio to stop writes |
| `primary_volume_offload_trigger_watermark` | `f64` | 0.85 | Usage ratio to trigger offload |
| `primary_volume_offload_policy` | `PrimaryVolumeOffloadPolicyKind` | `Priority` | Policy: `LargestFile` or `Priority` |

### LSM Splitting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `lsm_split_trigger_level` | `Option<u8>` | `None` | Level that triggers LSM tree splitting |

### Logging

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `log_path` | `Option<String>` | `None` | Log file path (must be local) |
| `log_max_file_size` | `Size` | `10MiB` | Maximum size of the active log file before rollover |
| `log_keep_files` | `usize` | `3` | Total number of log files retained, including the active file |
| `log_console` | `bool` | `false` | Enable console logging |
| `log_level` | `log::LevelFilter` | `Info` | Trace, Debug, Info, Warn, Error, Off |

### Java JNI Direct Buffer

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `jni_direct_buffer_size` | `Size` | `2KiB` | Capacity of each pooled direct `ByteBuffer` used by Java direct get/scan APIs and structured direct APIs (`Db.getDirect*`, `Db.scanDirect*`, `io.cobble.structured.Db.getDirect*`, `io.cobble.structured.Db.scanDirect*`) |
| `jni_direct_buffer_pool_size` | `usize` | `64` | Maximum number of pooled direct buffers kept per Java process for raw + structured Java direct APIs |

---

## CoordinatorConfig

Configuration for `DbCoordinator`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `volumes` | `Vec<VolumeDescriptor>` | Single local volume | Storage volumes for global manifests |
| `snapshot_retention` | `Option<usize>` | `None` | Auto-expire old global snapshots |

---

## VolumeDescriptor

Describes a storage volume.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_dir` | `String` | (required) | Base directory URL (`file://`, `s3://`, etc.); S3 supports URL-encoded endpoint/root hints (for example `s3://127.0.0.1:9000/bucket/prefix?endpoint_scheme=http&region=us-east-1`) |
| `access_id` | `Option<String>` | `None` | Access ID for remote storage |
| `secret_key` | `Option<String>` | `None` | Secret key for remote storage |
| `size_limit` | `Option<Size>` | `None` | Maximum volume size |
| `custom_options` | `Option<HashMap<String, String>>` | `None` | Backend-specific initialization options passed to OpenDAL |
| `kinds` | `u8` | 0 | Bitmask of `VolumeUsageKind` values |

If you want to inject some custom options to OpenDAL for specific backends, you can use the `custom_options` field. You can find the list of supported options for each backend in [the OpenDAL documentation](https://opendal.apache.org/docs/rust/opendal/services/struct.S3.html#configuration), take S3 for example, you can set `endpoint`, `region` and so on.

### VolumeUsageKind

| Kind | Value | Description |
|------|-------|-------------|
| `Meta` | 0 | Metadata files (manifests, schemas) |
| `PrimaryDataPriorityHigh` | 1 | High-priority data (SST, Parquet, VLOG) |
| `PrimaryDataPriorityMedium` | 2 | Medium-priority data |
| `PrimaryDataPriorityLow` | 3 | Low-priority data |
| `Snapshot` | 4 | Snapshot materialization |
| `Cache` | 5 | Block cache disk tier |
| `Readonly` | 6 | Read-only data source |

### Helper Methods

| Method | Description |
|--------|-------------|
| `VolumeDescriptor::single_volume(url)` | Create single volume with `PrimaryDataPriorityHigh + Meta` |
| `VolumeDescriptor::new(url, kinds)` | Create volume with specified usage kinds list (`Vec<VolumeUsageKind>`) |
| `set_usage(kind)` | Add a usage kind to the volume |
| `supports(kind)` | Check if volume supports a usage kind |

---

## ReadOptions

Options for point lookups.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `column_indices` | `Option<Vec<usize>>` | `None` | Column projection (None = all columns) |

---

## ScanOptions

Options for scan operations.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `read_ahead_bytes` | `Size` | `0` | Read-ahead buffer size (`0` disables read-ahead) |
| `column_indices` | `Option<Vec<usize>>` | `None` | Column projection |

---

## WriteOptions

Options for write operations.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ttl_seconds` | `Option<u32>` | `None` | TTL for this write (overrides default) |

---

## ReaderConfigEntry

Configuration for the read proxy.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `pin_partition_in_memory_count` | `usize` | 1 | Partition snapshots pinned in memory |
| `block_cache_size` | `Size` | `512MiB` | Reader block cache size |
| `reload_tolerance_seconds` | `u64` | 10 | Min interval between snapshot reload checks |

---

## Enums

### DataFileType

| Variant | Description |
|---------|-------------|
| `SSTable` | Row-based SST format (default) |
| `Parquet` | Apache Parquet columnar format |

### MemtableType

| Variant | Description |
|---------|-------------|
| `Hash` | Hash table (default) |
| `Skiplist` | Skip list |
| `Vec` | Vector-based |

### CompactionPolicyKind

| Variant | Description |
|---------|-------------|
| `RoundRobin` | Fair round-robin level selection (default) |
| `MinOverlap` | Minimize key range overlap |

### SstCompressionAlgorithm

| Variant | Description |
|---------|-------------|
| `None` | No compression (default) |
| `Lz4` | LZ4 compression |

### TimeProviderKind

| Variant | Description |
|---------|-------------|
| `System` | System wall clock (default) |
| `Manual` | Manual time control (starts at 0) |

### PrimaryVolumeOffloadPolicyKind

| Variant | Description |
|---------|-------------|
| `Priority` | Offload by file priority (default) |
| `LargestFile` | Offload largest file first |
