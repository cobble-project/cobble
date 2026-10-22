---
title: Tools
parent: Reference
nav_order: 3
---

# Tools

This page documents Cobble maintenance tools that are used outside an active writer `Db`.

## Prune Shard Snapshot

`prune_shard_snapshot` removes one shard snapshot manifest and releases files that are only referenced by that snapshot.

Rust:

```rust
use cobble::{Config, prune_shard_snapshot};

let removed = prune_shard_snapshot(config, db_id, snapshot_id)?;
// removed == true: snapshot existed and was pruned
// removed == false: snapshot did not exist
```

### Notes

- This is a maintenance tool API, not a `Db` instance method.
- It is intended for out-of-band lifecycle managers (for example, a global committer).
- If files are still referenced by other snapshots, those files are retained.

### Recommended Writer DB Configuration (with external pruning)

When writer shards are managed by an external global lifecycle component, configure writer DBs as:

- `snapshot_only_track: true` — writer only tracks snapshots and does not run DB-side retention expiration.
- `snapshot_disable_incremental_base_link: true` — disable incremental manifest base linking for writer snapshots.
- `snapshot_retention: null` — do not enable local retention on writer shards.

This keeps snapshot cleanup ownership in the external pruning workflow.

## Snapshot Cleanup

Use `cleanup_shard_snapshot` or `cleanup_global_snapshot` to remove saved snapshots,
including standalone snapshots and exports, without opening a database.
Stop all readers, writers, compactors, and snapshot publication before cleanup.

```rust
use cobble::{SnapshotCleanupOptions, VolumeDescriptor, VolumeUsageKind,
    cleanup_global_snapshot};

let metadata = VolumeDescriptor::new(
    "file:///backups/run-001", vec![VolumeUsageKind::Snapshot]);
let result = cleanup_global_snapshot(
    &metadata, &[], snapshot_id, SnapshotCleanupOptions::default())?;
```

For one shard, call
`cleanup_shard_snapshot(&metadata, &other_volumes, db_id, snapshot_id, options)`.
The metadata base is the parent of `<db_id>/snapshot/` (an export's `shards/`
directory). For global cleanup, it contains `snapshot/SNAPSHOT-<global-id>`
(the export root). Supply additional volume descriptors with paths and credentials
when files are stored elsewhere.

Two options control removal:

- `preserve_dependencies` defaults to `true`: retain selected shard snapshots and
  files needed by surviving snapshots. Setting it to `false` can make surviving
  snapshots unrecoverable.
- `include_older_snapshots` defaults to `false`: when enabled, also select all
  snapshots with IDs below the requested ID. Global cleanup compares global IDs,
  not shard IDs.

The result reports deleted file and snapshot counts, plus `preserved_snapshots`.
Shard cleanup only accounts for that shard's snapshots; use global cleanup when
global references must be protected. References outside the cleanup scope remain
the caller's responsibility.

See the [Java](../ffi-bindings/java#snapshot-cleanup),
[C++](../ffi-bindings/cpp#snapshot-cleanup), and
[Python](../ffi-bindings/python#snapshot-cleanup) examples.
