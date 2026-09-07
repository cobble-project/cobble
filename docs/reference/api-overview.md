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
| `DbBuilder` | Configure a writer before opening or restoring it, including schema transform registration |
| `ReadOnlyDb` | Read-only snapshot access |
| `Reader` | Snapshot-following read proxy (visibility advances by snapshot cadence) |
| `DbCoordinator` | Global snapshot coordinator |

### Scan Types

| Type | Description |
|------|-------------|
| `ScanPlan` | Distributed scan plan from a global snapshot |
| `ScanSplit` | Serializable unit of scan work (one per shard) |
| `ScanSplitPartition` | Pair of `before` / `after` splits around one bucket/key boundary |
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

### Filesystem Extension Types

| Type / Function | Description |
|------|-------------|
| `ProcessFileSystemRequest` | Request context passed to process-level custom filesystem resolution (original and normalized base dir, parsed URL, credentials, custom options). |
| `ProcessFileSystemRegistry` | Trait for host-side custom filesystem resolution (`try_init`). |
| `register_process_custom_file_system_registry(...)` | Registers one process-level custom filesystem registry used as fallback when built-in resolution/access fails. |
| `clear_process_custom_file_system_registry()` | Clears the current process-level custom filesystem registry. |

Resolution order is built-in first, then process-level fallback. If a custom registry is configured,
Cobble also falls back when built-in filesystem initialization succeeds but the initial access probe
fails.

### Metadata & Schema Types

| Type | Description |
|------|-------------|
| `Schema` | Current raw schema with family-local metadata |
| `SchemaBuilder` | Schema evolution builder; column-family aware via optional family arguments |
| `ColumnEvolution` | Target column mapping: `Source` with optional transform ID, `Default`, or `Null` |
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
| `RemoteCompactionFailureMode` | Writer behavior for transient remote compaction failures (`FallbackLocal` or `Skip`) |
| `DedicatedCompactionMonitor` | Discover DBs and produce portable dedicated compaction plans |
| `DedicatedCompactionExecutor` | Revalidate and execute queued dedicated compaction plans |

### Key Operations

#### SingleDb

```rust
SingleDb::open(config) -> Result<SingleDb>
SingleDb::resume(config, global_snapshot_id) -> Result<SingleDb>
SingleDb::resume_with_recovery_mode(config, global_snapshot_id, recovery_mode) -> Result<SingleDb>
db.put(bucket, key, column, value) -> Result<()>
db.put_with_options(bucket, key, column, value, &WriteOptions::with_column_family("metrics")) -> Result<()>
db.merge(bucket, key, column, value) -> Result<()>
db.delete(bucket, key, column) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.scan_with_options(bucket, range, &ScanOptions::for_column(0).with_column_family("metrics")) -> Result<DbIterator<'_>>
db.snapshot() -> Result<u64>
db.snapshot_with_callback(callback) -> Result<u64>
db.switch_memtable_type(memtable_type, flush_current) -> Result<()>
db.load_readonly_files_to_primary() -> Result<usize>
```

#### Db

```rust
Db::open(config, bucket_ranges) -> Result<Db>
Db::resume(config, db_id) -> Result<Db>
Db::resume_from_snapshot(config, snapshot_id, db_id) -> Result<Db>
Db::resume_from_snapshot_with_recovery_mode(config, snapshot_id, db_id, recovery_mode) -> Result<Db>
Db::open_from_snapshot(config, snapshot_id, db_id) -> Result<Db>
Db::open_from_snapshot_with_recovery_mode(config, snapshot_id, db_id, recovery_mode) -> Result<Db>
Db::resume_with_recovery_mode(config, db_id, recovery_mode) -> Result<Db>
Db::open_new_with_snapshot(config, snapshot_id, source_db_id) -> Result<Db>
Db::open_new_with_manifest_path(config, manifest_path) -> Result<Db>
ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id) -> Result<ReadOnlyDb>
db.current_schema() -> Arc<Schema>
db.update_schema() -> SchemaBuilder
db.register_schema_transform(id, transform) -> Result<()>
db.put(bucket, key, column, value) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.scan(bucket, range) -> Result<DbIterator<'_>>
db.scan_bounds(bucket, start_key_inclusive, end_key_exclusive) -> Result<DbIterator<'_>>
db.scan_with_options(bucket, range, &scan_options) -> Result<DbIterator<'_>>
db.scan_with_options_bounds(bucket, start_key_inclusive, end_key_exclusive, &scan_options) -> Result<DbIterator<'_>>
read_only.scan(bucket, range) -> Result<DbIterator<'static>>
read_only.scan_bounds(bucket, start_key_inclusive, end_key_exclusive) -> Result<DbIterator<'static>>
read_only.scan_with_options(bucket, range, &scan_options) -> Result<DbIterator<'static>>
read_only.scan_with_options_bounds(bucket, start_key_inclusive, end_key_exclusive, &scan_options) -> Result<DbIterator<'static>>
db.snapshot() -> Result<u64>
db.snapshot_with_callback(callback) -> Result<u64>
db.switch_to_snapshot(snapshot_id) -> Result<()>
db.switch_memtable_type(memtable_type, flush_current) -> Result<()>
db.cancel_snapshot(snapshot_id) -> Result<bool>
db.expire_snapshot(snapshot_id) -> Result<bool>
db.retain_snapshot(snapshot_id) -> bool
db.shard_snapshot_input(snapshot_id) -> Result<ShardSnapshotInput>
db.expand_bucket_with_storage_mode(source_db_id, snapshot_id, ranges, storage_mode) -> Result<u64>
db.wait_for_expand_adoption(timeout) -> Result<()>
db.load_readonly_files_to_primary() -> Result<usize>
```

`open_from_snapshot` preserves the source db identity and snapshot directory. `open_new_with_snapshot`
restores from the source snapshot but assigns a fresh db id and starts a new snapshot chain.
`open_new_with_manifest_path` does the same thing when your checkpoint metadata already stores the
exact source manifest path.

Use `RecoveryMode::SnapshotOnly` for an exact snapshot restore or `RecoveryMode::LatestWithWal` to
replay the latest snapshot's durable WAL tail. See [Write-Ahead Log](../architecture/wal).

For custom column transforms, call
`DbBuilder::register_schema_transform(id, transform) -> Result<DbBuilder>` before `open()`,
`resume()`, `open_from_snapshot(...)`, or `resume_from_snapshot(...)`. Use the DB registration
method for subsequent runtime updates. See [Custom Column Transforms](../architecture/schema-evolution#custom-column-transforms)
for examples, recovery requirements, and current support limits.

`switch_to_snapshot` is runtime-only until a later snapshot is published. It deliberately keeps
the existing WAL tail so the latest state remains recoverable, but it does not create an isolated
WAL branch for writes based on the historical snapshot. See
[Active Snapshot Switch](../architecture/snapshot#active-snapshot-switch).

Snapshot lifecycle notes:

- `db.snapshot()` returns a snapshot id after the async materialization flow has been scheduled.
- `db.snapshot_with_callback(...)` delivers a `ShardSnapshotInput` once manifest publication finishes.
- `db.cancel_snapshot(snapshot_id)` only succeeds before manifest publication completes.
- `db.expire_snapshot(snapshot_id)` releases snapshot ownership and file references.
- `db.retain_snapshot(snapshot_id)` keeps a completed snapshot alive across retention passes.

`load_readonly_files_to_primary()` is also available on `StructuredDb` and
`StructuredSingleDb`. See [Loading Files from Readonly Volumes](../architecture/multi-volume#loading-files-from-readonly-volumes).

`switch_memtable_type()` accepts `Adaptive` or a concrete memtable type. See [Memtable](../architecture/memtable#choosing-a-memtable-type).

Bucket expansion supports asynchronous adoption, persistent references, and persistent references
with a local read cache. See [Rescale](../architecture/rescale).

#### Reader

```rust
Reader::open_current(reader_config) -> Result<Reader>
ReaderBuilder::new(reader_config).register_schema_transform(id, transform)?.open_current() -> Result<Reader>
ReadOnlyDbBuilder::new(config).db_id(db_id).register_schema_transform(id, transform)?.open(snapshot_id) -> Result<ReadOnlyDb>
reader.register_schema_transform(id, transform) -> Result<()>
read_only_db.register_schema_transform(id, transform) -> Result<()>
reader.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
reader.current_global_snapshot() -> &GlobalSnapshotManifest
reader.refresh() -> Result<()>
```

Both builders support registering transforms before opening; see [Schema Evolution](../architecture/schema-evolution#snapshot-readers).

Remote and dedicated compactor entrypoints also expose `register_schema_transform(id, transform)` for process-local callback registration; see [Standalone Compactors](../architecture/schema-evolution#standalone-compactors).

#### Scan

```rust
ScanPlan::new(manifest) -> ScanPlan // bucket-only
plan.splits() -> Vec<ScanSplit>
split.split_after(bucket, key) -> Result<ScanSplitPartition>
split.create_scanner(config, &scan_options) -> Result<ScanSplitScanner> // choose non-default family here via ScanOptions
for row in scanner { let (bucket, key, columns) = row?; }
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
| `StructuredScanSplit` | Structured scan split with optional resume/end boundary metadata |
| `StructuredScanSplitScanner` | Structured scan scanner yielding `(bucket, key, columns)` rows |
| `StructuredRemoteCompactionServer` | Remote compaction with structured merge ops |

Structured values are represented with `StructuredColumnValue` and configured by `StructuredSchema` (`Bytes` / `List` column types).

`StructuredSchema` is also family-aware: `column_families()` returns per-family typed columns keyed by family name and always includes `default`, while `StructuredSchemaBuilder` methods accept `Option<String>` family arguments. Structured wrappers use `StructuredWriteOptions` / `StructuredReadOptions` / `StructuredScanOptions` for family selection.

`StructuredDb` also mirrors the new bounds-scan entrypoints:
`scan_bounds(...)` and `scan_with_options_bounds(...)` use the same inclusive-start /
exclusive-end semantics as raw `Db`.

---

## cobble-table

| Type | Description |
|------|-------------|
| `TableWriterBuilder` | Open a writable shard and return a `Table` |
| `TableReaderBuilder` / `TableReader` | Open and own typed access to a fixed shard or global snapshot |
| `Table` | Typed reads, writes, and snapshots over a shared `Arc<Db>` |
| `ReadOnlyTable` | Typed access borrowing an application-managed `ReadOnlyDb` |
| `TableSchema` / `TableKey` | Logical row structure and reusable encoded primary keys |
| `TableProjection` | Reusable field selection for typed reads and scans |
| `SchemaChange` | Add, rename, or drop top-level fields by name while retaining stable field identities |
| `CatalogTable` | Loaded table definition with reader, writer, and coordinator factories sharing a stable storage namespace |
| `TableWriteBuilder` / `TableWritePlan` | Capture a table definition and storage routes for distributed shard writers; plans support Serde serialization |

Define a new schema by name; field IDs are assigned automatically:

```rust
let schema = TableSchema::builder()
    .field("id", LogicalType::int64())
    .field("name", LogicalType::string().nullable())
    .primary_key(["id"])
    .bucket_key(["id"])
    .build()?;
```

Use `LogicalType::struct_from_fields` for name-based nested fields. The schema builder
assigns IDs across the entire new schema, including nested fields. Explicit-ID
`DataField::new` and `TableSchema::new` remain available for advanced integrations.
When reopening existing tables, use their persisted schema; do not rebuild and renumber it.

`Table::create(Arc::clone(&db), name, schema)` and `Table::open(Arc::clone(&db), name)`
use an existing shared database. Catalog and writer-plan builders return the same `Table` type.
Dropping a table releases its reference without explicitly closing the shared database;
calling `Db::close()` affects all users of that database.

`Catalog::evolve_schema` accepts `SchemaChange` values. Added fields must be nullable;
renaming preserves the field ID, and deleted IDs are never reused. Publishing a catalog
schema does not refresh an already opened reader or writer automatically.

Standalone readers and writers open storage directly from configuration; a catalog is not required.
Catalog APIs are grouped under `cobble_table::catalog`. With a `FileCatalog`, use the loaded
table's `writer_builder(config)`, `reader_builder(config)`,
and `coordinator(config)` factories. Catalog configuration supplies Meta, Snapshot, and WAL volumes;
its Primary roles are ignored. Runtime configuration supplies all Primary volumes (High, Medium,
and Low), Cache, and READONLY, retaining their configured priorities. READONLY source paths remain
unchanged. Table directories follow stable IDs, so renaming a table does not move its files or
change its snapshot location.
For catalog-backed writers, `open()` creates a shard and `resume()` applies the loaded table
definition. Explicit snapshot restores and readers use the schema stored in that snapshot.
Selecting the current global snapshot captures its version when the reader opens; it does not
automatically follow later snapshots or schema changes.
`Table::snapshot()` starts an asynchronous snapshot; `snapshot_and_wait()` returns the
completed `ShardSnapshotInput` for opening a reader or submitting to a snapshot coordinator.

For distributed writing, build a plan once and serialize it for workers:

```rust
let plan = table.new_write_builder().total_buckets(256).build()?;
let payload = serde_json::to_vec(&plan)?;

// On a worker, after receiving the payload:
let plan: cobble_table::TableWritePlan = serde_json::from_slice(&payload)?;
let writer = plan.writer_builder(runtime_config)?
    .db_id("shard-0")
    .bucket_ranges(vec![0..=127])
    .open()?;
```

The plan fixes the schema, table identity, bucket count, and shared storage routes. A worker does
not reload the latest Catalog definition. Credentials are excluded from the serialized plan;
workers supply them through matching volume descriptors in their runtime config. Shared-volume
descriptors supply credentials only: the plan determines Meta/Snapshot/WAL locations, while runtime
config determines Primary/Cache/READONLY volumes. An in-process plan retains access to the original
Catalog credentials. Applications choose how to transport the serialized bytes.

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
On the Java side, raw and structured `Db` plus `SingleDb` accept `RecoveryMode` overloads. Restore
flows also expose `Db.restore(..., boolean newDbId)` and `Db.restoreWithManifest(...)`.
Raw and structured `Db` / `SingleDb` classes also expose `loadReadonlyFilesToPrimary()`. See
[Loading Files from Readonly Volumes](../architecture/multi-volume#loading-files-from-readonly-volumes).
Raw and structured `Db` expose `ExpandStorageMode` and `waitForExpandAdoption(...)`; see
[Rescale](../architecture/rescale).

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
| `io.cobble.DirectScanCursor` / `DirectScanEntry` | Raw direct scan cursor and row view |
| `io.cobble.ProcessFileSystems` | Process-level custom filesystem registration entrypoint |
| `io.cobble.ProcessFileSystemRequest` | Java DTO for fallback filesystem resolution context |
| `io.cobble.CustomFileSystemRegistry` | Java callback interface for resolving custom filesystems |
| `io.cobble.CustomFileSystem` | Java filesystem abstraction consumed by Cobble JNI |
| `io.cobble.CustomRandomAccessFile` | Random-read file abstraction; supports optional direct read path via `supportDirect()` / `readAtDirect(...)` |
| `io.cobble.CustomSequentialWriteFile` | Sequential-write file abstraction; supports optional direct write path via `supportDirect()` / `writeDirect(...)` |
| `io.cobble.structured.SingleDb` | Structured `SingleDb` |
| `io.cobble.structured.Db` | Structured `Db` |
| `io.cobble.structured.Schema` / `StructuredSchemaBuilder` | Structured family-aware schema API |
| `io.cobble.structured.DirectListValueBuilder` | Reusable direct builder for Cobble core list payloads |
| `io.cobble.structured.StructuredScanSplit` | Structured distributed split |
| `io.cobble.structured.ScanCursor` | Structured scan iterator cursor |
| `io.cobble.structured.DirectRow` | Structured zero-copy direct read view, including direct list-element accessors |
| `io.cobble.structured.DirectEncodedRow` | Structured encoded direct row view with InputStream-based BYTES/LIST decoders |
| `io.cobble.structured.DirectScanCursor` / `DirectScanRow` | Structured direct scan cursor and row view |
