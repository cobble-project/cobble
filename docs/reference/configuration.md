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
| `memtable_type` | `MemtableType` | `Adaptive` | `Adaptive`, `Hash`, `Skiplist`, or `Vec` |

### LSM Tree

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `l0_file_limit` | `usize` | 4 | L0 file count that triggers compaction |
| `write_stall_limit` | `Option<usize>` | `None` | Max immutable+L0 files before stall. Auto: `max(l0+2, 32)` |
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
| `sst_read_metadata_cache_mode` | `SstReadMetadataCacheMode` | `Eager` | Cache decoded SST footer and index-partition metadata: `Eager`, `Lazy`, or `Off` |
| `sst_pinned_metadata_max_level` | `Option<u8>` | `Some(2)` | Pin immutable top-level SST index and bloom-filter metadata for L0 through this level. Pinned metadata is shared by point reads, scans, and compactions and does not count against the block-cache budget. |
| `sst_pinned_metadata_partitions_enabled` | `bool` | `false` | Also pin second-level index and filter partitions for partitioned SST files. |
| `sst_data_block_restart_interval` | `usize` | 16 | Restart interval in SST data blocks (`>1` enables prefix compression, `1` disables; range `1..=65535`) |
| `block_checksum_enabled` | `bool` | `true` | Record CRC32 checksums for new SST data blocks; SST reads verify existing checksums automatically |
| `sst_compression_by_level` | `Vec<SstCompressionAlgorithm>` | `[None, None, Lz4]` | Compression per level |

`sst_read_metadata_cache_mode` accepts `eager`, `lazy`, and `off`. `eager` attaches metadata when a new SST is written, `lazy` caches it on the first read, and `off` rebuilds it for each reader.

Set `sst_pinned_metadata_max_level` to `0` to pin L0 metadata, or `N` to pin L0 through LN. Set it to `-1` in JSON/JVM configuration, or `None` through the Rust API, to use the normal block-cache path for metadata.

By default, partitioned SST files pin only their top-level index and filter index. Set `sst_pinned_metadata_partitions_enabled` to `true` to pin the second-level index and filter partitions as well.

### Parquet

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `parquet_row_group_size_bytes` | `Size` | `256KiB` | Row group size |

Parquet page checksums are not currently supported. The upstream Rust Parquet writer does not yet
write the standard optional page CRC field, and `block_checksum_enabled` therefore has no effect on
Parquet output.

### Block Cache

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `block_cache_size` | `Size` | `64MiB` | In-memory cache size (`0` also disables cache) |
| `block_cache_hybrid_enabled` | `bool` | `false` | Enable memory + disk hybrid cache |
| `block_cache_hybrid_disk_size` | `Option<Size>` | `None` | Disk tier capacity (defaults to memory size) |

### Compaction

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `compaction_policy` | `CompactionPolicyKind` | `RoundRobin` | Policy: `RoundRobin`, `MinOverlap`, or `ScorePriority` |
| `compaction_read_ahead_enabled` | `bool` | `true` | Buffered reads during compaction |
| `compaction_remote_addr` | `Option<String>` | `None` | Remote compaction server address (host:port) |
| `compaction_threads` | `usize` | 4 | Compaction thread pool size |
| `compaction_remote_timeout_ms` | `u64` | 300,000 | Remote compaction timeout (milliseconds) |
| `compaction_remote_failure_mode` | `RemoteCompactionFailureMode` | `FallbackLocal` | Behavior for transient remote compaction failures |
| `compaction_server_max_concurrent` | `usize` | 4 | Max concurrent tasks on remote server |
| `compaction_server_max_queued` | `usize` | 64 | Max queued tasks before rejecting |
| `compaction_mode` | `CompactionMode` | `Embedded` | Run compaction in the writer (`Embedded`) or through a standalone shared-storage process (`Dedicated`) |
| `runtime_manifest_mode` | `RuntimeManifestMode` | `Auto` | Publish persisted-layout observations: `Auto`, `Enabled`, or `Disabled` |
| `compaction_dedicated_poll_interval_ms` | `u64` | 1,000 | Poll interval for dedicated compaction results |
| `compaction_orphan_min_age_ms` | `u64` | 300,000 | Minimum age before abandoned dedicated-compaction job files may be removed |

`compaction_policy` accepts:

- `round_robin` - rotate through files in oversized non-`L0` levels
- `min_overlap` - choose the next file with the smallest overlap in the next level
- `score_priority` - prefer the highest-scored level first, then pick files in a RocksDB-style min-overlap order with a per-level cursor and RocksDB-style trivial-move gating

`compaction_remote_failure_mode` accepts:

- `fallback_local` - run the failed remote compaction locally and keep the DB writable
- `skip` - skip the failed compaction attempt and retry remote on a later compaction trigger

Only transient remote failures use this setting. Permanent protocol, schema, and configuration errors are surfaced to the DB.

`runtime_manifest_mode=auto` enables runtime manifests for a dedicated writer and for
`cobble-cli compact`; embedded writers leave them disabled. Set it to `disabled` to use the
snapshot-driven dedicated-compaction path, or `enabled` to publish observations from an embedded
writer. Runtime manifests describe the latest persisted LSM layout; snapshots remain the recovery
point and the durability proof for an applied dedicated-compaction result.

### Value Separation

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value_separation_threshold` | `Option<Size>` | `None` | Byte threshold for VLOG separation (`None` = disabled) |
| `vlog_low_priority_primary_enabled` | `bool` | `false` | Place VLOG files newly created or copied into primary on the lowest-priority tier. Existing primary replicas are not rebalanced. These VLOG files are not promoted by low-to-high primary backfill; fail if that tier cannot accept writes. |

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
| `snapshot_only_track` | `bool` | `false` | Track snapshots only; disable DB-side retention expiration |
| `snapshot_disable_incremental_base_link` | `bool` | `false` | Disable incremental manifest base linking |
| `active_memtable_incremental_snapshot_ratio` | `f64` | 0.0 | Ratio for incremental memtable snapshots (0 = disabled) |

### Governance

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `governance_mode` | `GovernanceMode` | `Filesystem` | Writable DB governance mode: `Filesystem` uses the manifest-backed ownership registry, `Noop` disables Cobble-side registration |

`governance_mode` accepts:

- `filesystem` - default mode. Writable `Db` opens register their bucket ranges into the governance manifest stored in the `Meta` volume and reject overlaps with other registered shards.
- `noop` - skip governance registration and unregistration entirely. Choose this only when exclusive bucket ownership is already enforced by the embedding runtime or deployment orchestration.

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
| `primary_volume_backfill_trigger_watermark` | `f64` | 0.40 | Backfill trigger ratio (maximum 0.80 and kept below the offload watermark) |
| `file_transfer_concurrency` | `usize` | 4 | Maximum concurrent background file transfers per database |
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
| `column_family` | `Option<String>` | `None` | Column family name; omitted means the default family |
| `max_rows` | `Option<usize>` | `None` | Optional soft cap for rows returned by one scan batch |
| `preload_scan_cursor_block` | `bool` | `false` | Preload the next SST block while a scan cursor advances |
| `should_stop_at_block_boundary` | `bool` | `false` | Pause after crossing the next physical SST block or Parquet row group boundary |

`should_stop_at_block_boundary` is an opt-in batching control. When enabled, scan
iterators may stop after they have crossed the next physical storage boundary and
report that pause through `stopped_at_block_boundary()`. Callers can return the
rows already collected, call `clear_stop_at_block_boundary()`, and then continue
from the same logical scan position on the next poll.

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
| `Adaptive` | Automatically selects a memtable implementation for the workload (default) |
| `Hash` | Hash table |
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
