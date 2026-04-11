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
builder.set_column_operator(2, Arc::new(U64CounterMergeOperator))?;
let _new_schema = builder.commit();
```

For adding/removing columns with explicit position control:

```rust
let mut builder = db.update_schema();
builder.add_column(1, None, None, None)?;     // insert new column at index 1
// or
builder.delete_column(1)?;              // delete column at index 1
let _new_schema = builder.commit();
```

## Add Column: What Actually Happens

When a column is added:

- New writes use the new schema immediately.
- Old SST/Parquet/VLOG-backed rows are still physically in old shape.
- During reads, Cobble evolves old rows to the current schema in memory, filling added columns with default/empty values (or `add_column` default value if configured).
- During [compaction](compaction), data is rewritten into new files under the latest schema, so the conversion cost gradually disappears.

Operationally, this means you get forward-compatible reads immediately after commit, while storage catches up in background compaction.

## Delete Column: What Actually Happens

When a column is deleted:

- New writes no longer include that column.
- Old files may still physically carry it.
- During reads, deleted columns are dropped from the logical row view.
- During compaction, rewritten files no longer contain deleted column data.

This is why deletion is safe online: query behavior switches at schema-commit time, and physical reclamation is deferred to compaction.

## Impact on Read Path

Read APIs (`get`, `scan`, `Reader`, `ReadOnlyDb`) always return values in the current schema shape:

- Schema evolution is applied per-row when source files are older than current schema.
- Column projection (`ReadOptions.column_indices` / `ScanOptions.column_indices`) is interpreted against the current schema.
- Merge operator dispatch uses per-column operator mapping from the active schema (see [Merge Operators](merge-operator)).

In short: callers do not need to branch on file age or schema version.

## Impact on Compaction

Compaction is where logical evolution becomes physical:

- Input rows from mixed schema versions are normalized to target schema.
- Output files are written with latest schema id.
- Repeated compaction reduces schema-conversion overhead and removes deleted-column payload from storage.

This is also why long snapshot retention can delay full reclamation after column deletion: old snapshots keep older files alive (see [Snapshot System](snapshot)).

## Schema Versions and Recovery

Each committed schema gets a version id and is persisted in metadata. Data files reference the schema id they were written with. On restore/resume, Cobble rebuilds schema history and can evolve rows correctly even if files span many schema versions.
