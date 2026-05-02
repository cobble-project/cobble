---
title: API Overview
parent: Reference
nav_order: 2
---

# API Overview

This page summarizes the public Rust API surface of the Cobble crates.

## cobble (Core Crate)

### Primary Types

| Type | Description |
|------|-------------|
| `SingleDb` | Single-machine embedded database (wraps Db + Coordinator) |
| `Db` | Shard database for distributed deployments |
| `ReadOnlyDb` | Read-only snapshot access |
| `Reader` | Snapshot-following read proxy (visibility advances by snapshot cadence) |
| `DbCoordinator` | Global snapshot coordinator |

### Scan Types

| Type | Description |
|------|-------------|
| `ScanPlan` | Distributed scan plan from a global snapshot |
| `ScanSplit` | Serializable unit of scan work (one per shard) |
| `ScanSplitScanner` | Iterator over key-value pairs within a split |

### Configuration Types

| Type | Description |
|------|-------------|
| `Config` | Main database configuration |
| `CoordinatorConfig` | Coordinator configuration |
| `VolumeDescriptor` | Storage volume descriptor |
| `VolumeUsageKind` | Volume usage kind enum |
| `ReadOptions` | Point lookup options |
| `ScanOptions` | Scan/iteration options |
| `WriteOptions` | Write operation options |

### Metadata & Schema Types

| Type | Description |
|------|-------------|
| `Schema` | Current raw schema with family-local metadata |
| `SchemaBuilder` | Schema evolution builder; column-family aware via optional family arguments |
| `ShardSnapshotInput` | Shard snapshot DTO used by the coordinator |
| `GlobalSnapshotManifest` | Materialized global snapshot manifest |

### Column Family Model

- The default family is `default`.
- Plain raw `put` / `merge` / `delete` / `get` / `scan` APIs use the default family.
- Raw Rust selects other families through `WriteOptions::with_column_family`, `ReadOptions::for_column_in_family` / `for_columns_in_family` / `with_column_family`, and `ScanOptions::with_column_family`.
- Named families are created through `SchemaBuilder`, not through config.
- Distributed routing stays bucket-only, but for `Reader` and `ScanPlan`, the column family should be selected through options. That means only one family is read or scan per operation.

### ColumnFamilyOptions and TTL Behavior

- `SchemaBuilder::set_column_family_options` allows setting `ColumnFamilyOptions` for each family, which currently only includes `value_has_ttl`. Setting this to `false` allows the cobble optimize storage for values without TTL.
- `value_has_ttl` controls whether write-time TTL input is effective in that family:
  - `true`: write-time TTL may set expiration
  - `false`: write-time TTL input is ignored

### Compaction Types

| Type | Description |
|------|-------------|
| `RemoteCompactionServer` | Remote compaction worker server |

### Key Operations

#### SingleDb

```rust
SingleDb::open(config) -> Result<SingleDb>
SingleDb::resume(config, global_snapshot_id) -> Result<SingleDb>
db.put(bucket, key, column, value) -> Result<()>
db.put_with_options(bucket, key, column, value, &WriteOptions::with_column_family("metrics")) -> Result<()>
db.merge(bucket, key, column, value) -> Result<()>
db.delete(bucket, key, column) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.scan_with_options(bucket, range, &ScanOptions::for_column(0).with_column_family("metrics")) -> Result<DbIterator<'_>>
db.snapshot() -> Result<u64>
db.snapshot_with_callback(callback) -> Result<u64>
```

#### Db

```rust
Db::open(config, bucket_ranges) -> Result<Db>
Db::open_from_snapshot(config, snapshot_id, db_id) -> Result<Db>
Db::open_new_with_snapshot(config, snapshot_id, source_db_id) -> Result<Db>
Db::open_new_with_manifest_path(config, manifest_path) -> Result<Db>
ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id) -> Result<ReadOnlyDb>
db.current_schema() -> Arc<Schema>
db.update_schema() -> SchemaBuilder
db.put(bucket, key, column, value) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.snapshot() -> Result<u64>
db.shard_snapshot_input(snapshot_id) -> Result<ShardSnapshotInput>
```

`open_from_snapshot` preserves the source db identity and snapshot directory. `open_new_with_snapshot`
restores from the source snapshot but assigns a fresh db id and starts a new snapshot chain.
`open_new_with_manifest_path` does the same thing when your checkpoint metadata already stores the
exact source manifest path.

#### Reader

```rust
Reader::open_current(reader_config) -> Result<Reader>
reader.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
reader.current_global_snapshot() -> &GlobalSnapshotManifest
reader.refresh() -> Result<()>
```

#### Scan

```rust
ScanPlan::new(manifest) -> ScanPlan // bucket-only
plan.splits() -> Vec<ScanSplit>
split.create_scanner(config, &scan_options) -> Result<ScanSplitScanner> // choose non-default family here via ScanOptions
for row in scanner { let (key, columns) = row?; }
```

---

## cobble-data-structure (Structured Wrappers)

### Types

| Type | Description |
|------|-------------|
| `StructuredSingleDb` | Structured SingleDb wrapper |
| `StructuredDb` | Structured Db wrapper |
| `StructuredReadOnlyDb` | Structured ReadOnlyDb wrapper |
| `StructuredReader` | Structured Reader wrapper |
| `StructuredScanPlan` | Structured scan plan |
| `StructuredScanSplit` | Structured scan split |
| `StructuredScanSplitScanner` | Structured scan scanner |
| `StructuredRemoteCompactionServer` | Remote compaction with structured merge ops |

Structured values are represented with `StructuredColumnValue` and configured by `StructuredSchema` (`Bytes` / `List` column types).

`StructuredSchema` is also family-aware: `column_families()` returns per-family typed columns keyed by family name and always includes `default`, while `StructuredSchemaBuilder` methods accept `Option<String>` family arguments. Structured wrappers use `StructuredWriteOptions` / `StructuredReadOptions` / `StructuredScanOptions` for family selection.

---

## cobble-web-monitor

### Types

| Type | Description |
|------|-------------|
| `MonitorConfig` | Web monitor configuration |
| `MonitorServer` | HTTP server for monitoring dashboard |
| `MonitorServerHandle` | Handle to control the running server |

---

## cobble-java

The Java API mirrors the Rust API. See [Java Bindings](../ffi-bindings/java) for usage details.
On the Java side, restore flows are exposed as `Db.restore(..., boolean newDbId)` and
`Db.restoreWithManifest(...)`.

### Java Classes

| Class | Rust Equivalent |
|-------|-----------------|
| `io.cobble.SingleDb` | `SingleDb` |
| `io.cobble.Db` | `Db` |
| `io.cobble.ReadOnlyDb` | `ReadOnlyDb` |
| `io.cobble.Reader` | `Reader` |
| `io.cobble.Config` | `Config` |
| `io.cobble.ReadOptions` / `ScanOptions` / `WriteOptions` | Raw family-aware options |
| `io.cobble.Schema` / `SchemaBuilder` | Raw schema view and evolution builder |
| `io.cobble.ShardSnapshot` / `GlobalSnapshot` | Snapshot DTOs that preserve named family mapping |
| `io.cobble.ScanPlan` | `ScanPlan` |
| `io.cobble.ScanSplit` | `ScanSplit` |
| `io.cobble.ScanCursor` | Raw scan iterator cursor |
| `io.cobble.DirectColumns` | Zero-copy raw direct read view |
| `io.cobble.DirectEncodedRow` | Raw encoded direct row view with InputStream-based column decoder |
| `io.cobble.DirectScanCursor` / `DirectScanBatch` / `DirectScanEntry` | Raw direct scan cursor and batch/row views |
| `io.cobble.structured.SingleDb` | Structured `SingleDb` |
| `io.cobble.structured.Db` | Structured `Db` |
| `io.cobble.structured.Schema` / `StructuredSchemaBuilder` | Structured family-aware schema API |
| `io.cobble.structured.DirectListValueBuilder` | Reusable direct builder for Cobble core list payloads |
| `io.cobble.structured.StructuredScanSplit` | Structured distributed split |
| `io.cobble.structured.ScanCursor` | Structured scan iterator cursor |
| `io.cobble.structured.DirectRow` | Structured zero-copy direct read view, including direct list-element accessors |
| `io.cobble.structured.DirectEncodedRow` | Structured encoded direct row view with InputStream-based BYTES/LIST decoders |
| `io.cobble.structured.DirectScanCursor` / `DirectScanBatch` / `DirectEncodedScanBatch` / `DirectScanRow` / `DirectEncodedScanRow` | Structured direct scan cursor and batch/row views |
