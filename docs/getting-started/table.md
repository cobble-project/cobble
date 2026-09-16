---
title: Table
parent: Getting Started
nav_order: 9
---

# Table

The `cobble-table` crate adds logical field types, named schemas, and typed rows to Cobble.
Use it directly over a database shard, or use a Catalog to manage table definitions and
storage paths across distributed writers and readers.

`Table` is a writable shard, `ReadOnlyTable` reads one fixed shard snapshot, and
`TableReader` routes reads across the shards of a global snapshot. All use the core
storage engine; a Catalog is optional.

## Define a schema

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

## Open a writer or shard reader

`Table::create(Arc::clone(&db), name, schema)` and `Table::open(Arc::clone(&db), name)`
use an existing shared database. Catalog and writer-plan builders return the same `Table` type.
Dropping a table releases its reference without explicitly closing the shared database;
calling `Db::close()` affects all users of that database.

`ReadOnlyTable::open(Arc::clone(&db), name)` uses an existing read-only shard.

## Global readers and refresh

`TableReader::open(reader, name)` takes ownership of a core `Reader`. A reader opened from core
`CURRENT` checks for later committed global snapshots on its configured access interval; an
explicit core snapshot stays fixed. The proxy handles bucket routing;
`ReadOnlyTable` only accesses its shard. Both provide typed reads, projections, and scans.
Projections and scan cursors can outlive the table handle.

For a loaded `CatalogTable` named `table`, choose the global reader mode explicitly:

```rust
let latest = table.reader_builder(runtime_config.clone())?
    .current_global_snapshot()
    .open()?;
let fixed = table.reader_builder(runtime_config)?
    .global_snapshot(snapshot_id)
    .open()?;
```

- **Latest** checks the latest committed global snapshot on access, using
  `config.reader.reload_tolerance_seconds`. It refreshes the snapshot, schema, and layout
  together, opens shards lazily, and reuses unchanged shards still in its bounded cache.
  Idle readers do not poll; an interval of `0` checks on every fallible access.
- **Fixed snapshot** always reads the selected snapshot and schema, without automatic refresh.

`reader.refresh()?` on a current `TableReader` explicitly checks the latest committed global
snapshot and returns whether it changed; fixed readers return `false`. It derives the schema from
that snapshot, never from the latest catalog record. `reader.schema()` returns an
`Arc<TableSchema>` for the currently loaded view without I/O. Existing projections, scans,
and scan plans remain fixed views;
`ReadOnlyTable` is always fixed. Refresh does not retain snapshots against external expiration.

## Schema evolution

`Catalog::evolve_schema` accepts `SchemaChange` values. Added fields must be nullable;
renaming preserves the field ID, and deleted IDs are never reused. Publishing a catalog
schema alone does not refresh an already opened reader or writer. Latest readers follow
committed global snapshots rather than the catalog's newest schema.
`AlterFieldType` provides built-in lossless widening with automatic factory registration
on Table builders and CLI compactors. `TransformField` supports custom conversions using
a persisted `TransformSpec`; materialization applies intermediate catalog versions in order.
Register factories on writer and reader builders before opening.
See [Table field transforms](../architecture/schema-evolution#table-field-transforms).

To advance a live writable handle, load the intended catalog version and call
`loaded_table.refresh_writer(&mut writer)?`. It materializes that version's missing schema
steps and refreshes that handle's local layout, returning whether it changed. Refresh other live
handles and rebuild old projections after a local schema change. Already-created scans remain
fixed; current snapshot readers check for commits on later fallible accesses or explicit
`TableReader::refresh()`.
`Table::refresh_schema()` only reloads local database metadata and does not read a catalog.

## Catalog and storage configuration

Standalone readers and writers open storage directly from configuration; a catalog is not required.
Catalog APIs are grouped under `cobble_table::catalog`. With a `FileCatalog`, use the loaded
table's `writer_builder(config)`, `readonly_table_builder(config)` for a shard,
`reader_builder(config)` for a global read proxy, and `snapshot_committer(config, max_pending_commits)` factories.
The lower-level `coordinator(config)` remains available for direct snapshot management.
Catalog configuration supplies Meta, Snapshot, and WAL volumes;
its Primary roles are ignored. Runtime configuration supplies all Primary volumes (High, Medium,
and Low), Cache, and READONLY, retaining their configured priorities. READONLY source paths remain
unchanged. Table directories follow stable IDs, so renaming a table does not move its files or
change its snapshot location.
Every writer builder requires `.bucket(id)` and opens exactly one Db with identity `bucket-<id>`.
For catalog-backed writers, `open()` initializes or resumes the retained empty snapshot 0;
use it for a first write or overwrite. `resume_from_snapshot(committed_shard_snapshot_id)`
restores committed data for append and applies the captured catalog definition. Standalone
`create(schema)` also starts from the empty baseline; standalone snapshot resumes use the stored
schema. File and snapshot IDs do not rewind. Fixed snapshot readers always use the stored schema.
Writers require local/shared filesystem META storage with working file locks; keep automatic
snapshot pruning disabled so the empty baseline and historical snapshots remain available.

## Global snapshots

`Table::snapshot()` starts an asynchronous snapshot; `snapshot_and_wait()` returns the
completed `ShardSnapshotMetadata` for opening a reader or submitting to a snapshot coordinator.

`snapshot_committer(config, max_pending_commits)` returns the same `TableSnapshotCommitter`
available without a catalog. Use `submit(commit_id, shard_snapshot)` as shards arrive or
`commit_batch(commit_id, shard_snapshots)` for a complete checkpoint. Run one active committer
per table; pending state is in memory, so the application must replay incomplete checkpoints
after a restart.
The committer checks the reported table schemas and layouts before publishing, without reading
shard manifests. Submit the complete reports returned by the DB snapshot APIs.
Readers trust this committed consistency and load table metadata when opening or refreshing a view,
not on every lookup.
Applications publishing through the raw `DbCoordinator` must ensure table consistency themselves.

## Distributed writes

For distributed writing, build a plan once and serialize it for workers:

```rust
let plan = table.new_write_builder().total_buckets(256).build()?;
let payload = serde_json::to_vec(&plan)?;

// On a worker, after receiving the payload:
let plan: cobble_table::TableWritePlan = serde_json::from_slice(&payload)?;
let writer = plan.writer_builder(runtime_config)?
    .bucket(0)
    .open()?;
```

Open one writer for each bucket assigned to a worker. The plan fixes the schema, table identity,
bucket count, and shared storage routes. A worker does
not reload the latest Catalog definition. Credentials are excluded from the serialized plan;
workers supply them through matching volume descriptors in their runtime config. Shared-volume
descriptors supply credentials only: the plan determines Meta/Snapshot/WAL locations, while runtime
config determines Primary/Cache/READONLY volumes. An in-process plan retains access to the original
Catalog credentials. Applications choose how to transport the serialized bytes.

## Distributed scans

For distributed reading, capture a fixed scan plan from a `TableReader` and send each split to a worker:

```rust
let plan = reader.scan_plan()?;
for split in plan.splits()? {
    let payload = serde_json::to_vec(&split)?;
    // On a worker, after receiving the payload:
    let split: cobble_table::TableScanSplit = serde_json::from_slice(&payload)?;
    for row in split.create_scanner(runtime_config.clone())? {
        let row = row?;
        // Process the typed row.
    }
}
```

Workers need no Catalog. The plan fixes the snapshot, schema, bucket count, and source paths;
runtime configuration supplies local storage and credentials. Later CURRENT or Catalog schema
changes do not alter an existing plan. Applications must retain the referenced snapshots until
scanning finishes; serializing a plan does not retain them.

The plan carries the table definition from the reader view selected by `scan_plan()`.
Workers use that definition without rereading shard metadata for consistency checks. Execution delegates shard opening and
bucket traversal to the core `ScanSplit` / `ScanSplitScanner`, adding only typed row decoding.
