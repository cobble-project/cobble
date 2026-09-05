---
title: Schema Evolution
parent: Architecture
nav_order: 10
---

# Schema Evolution

Cobble supports online schema evolution for multi-column values. You can add or delete columns without rewriting all existing data files immediately. Instead, schema evolution is recorded as metadata, then applied lazily in the read and compaction paths.

## How to Evolve Schema

Use `Db::update_schema()` (or `SingleDb::update_schema()`) and commit:

```rust
use std::sync::Arc;
use cobble::{Db, U64CounterMergeOperator};

let mut builder = db.update_schema();
builder.set_column_operator(None, 2, Arc::new(U64CounterMergeOperator))?;
let _new_schema = builder.commit();
```

For adding/removing columns with explicit position control:

```rust
let mut builder = db.update_schema();
builder.add_column(1, None, None, None)?;     // insert new column at index 1
// or
builder.delete_column(None, 1)?;              // delete column 1 in the default family
let _new_schema = builder.commit();
```

Column evolution is family-local. Named families are created through the same builder:

```rust
let mut builder = db.update_schema();
builder.add_column(0, None, None, Some("metrics".to_string()))?;
builder.set_column_operator(
    Some("metrics".to_string()),
    0,
    Arc::new(U64CounterMergeOperator),
)?;
let _new_schema = builder.commit();
```

## Custom Column Transforms

Register a transform on `Db`, then reference its stable ID in `SchemaBuilder::remap_columns`:

```rust
use bytes::Bytes;
use cobble::{ColumnEvolution, DbBuilder};

fn append_suffix(value: Option<Bytes>) -> cobble::Result<Option<Bytes>> {
    Ok(value.map(|value| [value.as_ref(), b"-v2"].concat().into()))
}

db.register_schema_transform("append-suffix-v1", append_suffix)?;
let mut schema = db.update_schema();
schema.remap_columns(None, vec![
    ColumnEvolution::Source {
        source_index: 0,
        transform_id: Some("append-suffix-v1".into()),
    },
    ColumnEvolution::Default { value: Bytes::from_static(b"new-column") },
    ColumnEvolution::Null,
])?;
schema.commit();
```

Each entry defines one target column. `Source` refers to a column in the builder's current layout; omitted columns are removed. A transform receives only that column's `Option<Bytes>` and returns `Result<Option<Bytes>>`; it cannot read other columns. New writes must already use the new representation. A column can have one transform per schema transition; commit an intermediate schema before applying another.

Only the transform ID is persisted, so register the same implementation on every restart. Use the builder **before recovery begins**: restoring memtables or replaying WAL can trigger background compaction before the DB is returned.

```rust
let db = DbBuilder::new(config)
    .db_id("my-shard")
    .register_schema_transform("append-suffix-v1", append_suffix)?
    .resume()?;
```

Builder registration also works with `open()`, `open_from_snapshot(snapshot_id)`, and `resume_from_snapshot(snapshot_id)`, including their recovery-mode variants. Restore methods require `db_id` and use the snapshot's bucket ranges. Missing required IDs fail recovery; duplicate registrations return an error. Keep each ID's meaning stable. `switch_to_snapshot` preserves the current DB's registrations, and `Db::register_schema_transform` remains available for new runtime schema updates.

Custom transforms support `Db`, `ReadOnlyDb`, and `Reader` reads (`get`, multi-get, and scan), plus local, remote, and dedicated compaction. Scans merge older values with their original operators before applying transforms, then apply column selection and row limits. Higher-level binding registration is not yet available.

### Snapshot Readers

Register transforms before opening a shard snapshot or a global snapshot reader:

```rust
use cobble::{ReadOnlyDbBuilder, ReaderBuilder};

let snapshot = ReadOnlyDbBuilder::new(config)
    .db_id("my-shard")
    .register_schema_transform("append-suffix-v1", append_suffix)?
    .open(snapshot_id)?;

let reader = ReaderBuilder::new(reader_config)
    .register_schema_transform("append-suffix-v1", append_suffix)?
    .open_current()?; // Use open(global_snapshot_id) for a specific snapshot.
```

`ReadOnlyDbBuilder` requires the source `db_id` and validates required transform IDs during open. `Reader` opens shards lazily, so missing IDs are reported on the first access to the affected shard, not when opening the global snapshot. Both types expose `register_schema_transform`; a failed lazy shard open can be retried after registration. Reader registrations survive refreshes and shard cache eviction. Transform callbacks and registries are runtime-only and must be registered again after restart.

### Standalone Compactors

Register the same IDs and implementations in each compactor process before starting work:

```rust
let server = cobble::RemoteCompactionServer::new(server_config)?;
server.register_schema_transform("append-suffix-v1", append_suffix)?;
server.serve("0.0.0.0:9000")?;
```

Dedicated compaction exposes the same method on `DedicatedCompactor`, `DedicatedCompactionService`, `DedicatedCompactionMonitor`, `DedicatedCompactionPlanner`, and `DedicatedCompactionExecutor`. Register before `run`, `poll`, `plan`, or `execute`; separately deployed planners and executors need their own registrations. Service registrations are shared with its discovered shards and workers.

Remote requests carry the complete schema chain through the planned target, including intermediate versions with no remaining files. Dedicated compactors load the chain from shared storage. Only IDs and schema definitions travel between processes, never callback code; use an application-owned compactor executable to register custom Rust callbacks, not the unmodified CLI. Missing IDs fail planning/execution without publishing a compaction result. Registration must be repeated after restart.

## Add Column: What Actually Happens

When a column is added:

- New writes use the new schema immediately.
- Old SST/Parquet/VLOG-backed rows are still physically in old shape.
- During reads, Cobble evolves old rows to the current schema in memory, filling added columns with default/empty values (or `add_column` default value if configured).
- During [compaction](compaction), data is rewritten into new files under the selected target schema, so the conversion cost gradually disappears.

Operationally, this means you get forward-compatible reads immediately after commit, while storage catches up in background compaction.

## Delete Column: What Actually Happens

When a column is deleted:

- New writes no longer include that column.
- Old files may still physically carry it.
- During reads, deleted columns are dropped from the logical row view.
- During compaction, rewritten files no longer contain deleted column data.

This is why deletion is safe online: query behavior switches at schema-commit time, and physical reclamation is deferred to compaction.

## Impact on Read Path

For built-in column additions and deletions, read APIs (`get`, `scan`, `Reader`, `ReadOnlyDb`) return values in the schema selected by that reader or snapshot:

- Schema evolution is applied per-row when source files are older than current schema.
- Column projection (`ReadOptions.column_indices` / `ScanOptions.column_indices`) is interpreted against the current schema of the selected column family.
- Merge operator dispatch uses per-column operator mapping from the active schema (see [Merge Operators](merge-operator)).

In short: callers do not need to branch on file age or schema version.

## Impact on Compaction

Compaction is where logical evolution becomes physical:

- Input rows from mixed schema versions are normalized to target schema.
- Output files carry the target schema id selected when compaction was planned; this may be older than the latest schema.
- Repeated compaction reduces schema-conversion overhead and removes deleted-column payload from storage.

This is also why long snapshot retention can delay full reclamation after column deletion: old snapshots keep older files alive (see [Snapshot System](snapshot)).

## Schema Versions and Recovery

Each committed schema gets a version id and is persisted in metadata. Data files reference the schema id they were written with. On restore/resume, Cobble rebuilds schema history and can evolve rows correctly even if files span many schema versions.
