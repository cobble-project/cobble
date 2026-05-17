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
