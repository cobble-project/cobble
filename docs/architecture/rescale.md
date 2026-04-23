---
title: Online Rescale
parent: Architecture
nav_order: 12
---

# Online Rescale

Cobble supports **runtime online rescale**: you can rebalance bucket ownership across shards without rebuilding the cluster, exporting/importing data manually, or running offline data cleanup jobs.

> Practical rules:
> 1. Start rescale after a fresh **global snapshot** is materialized (recommended for consistency baseline).
> 2. During the actual handoff window, **pause reads and writes** briefly.
> 3. The handoff is typically fast, and no manual data cleanup is required; obsolete data is cleaned gradually by normal background lifecycle work.

## Shard and Bucket Relationship

- `total_buckets` defines a fixed global bucket space (for example `0..=1023`).
- A **shard** (`Db` instance) owns one or more non-overlapping bucket ranges.
- Rescale changes only the **ownership mapping** (which shard owns which bucket ranges).
- Buckets are the migration unit; data for moved buckets is imported from snapshot state.

## Required Rescale Order

For correctness and predictable ownership transitions, use this order:

1. **Shrink all involved shards first**
2. **Then run expand on target shards**

`shrink_bucket` creates a pre-shrink snapshot and returns its snapshot id; this snapshot id is the source for later `expand_bucket`.

## Example: 2 Shards -> 3 Shards

Assume:

- Initial:
  - Shard A owns `0..=511`
  - Shard B owns `512..=1023`
- Target:
  - Shard A owns `0..=341`
  - Shard B owns `342..=683`
  - Shard C owns `684..=1023`

### Step 0: start new shard C (fresh restore from snapshot)

Use the latest global snapshot as a consistent baseline, and start shard C from shard B's snapshot
with a **fresh db id**:

```rust
// from latest global snapshot manifest
let shard_b_snapshot_id = /* manifest.shard_snapshots for db_b */ ;

// C is a new shard process, bootstrapped from snapshot with a fresh db id
let db_c = Db::open_new_with_snapshot(config_c, shard_b_snapshot_id, db_b.id())?;
```

Keep read/write traffic paused while this migration is running.

If your rescale metadata already captures the exact source manifest path, prefer
`Db::open_new_with_manifest_path(...)`.

### Phase 1: shrink (all shards first)

```rust
// A gives out 342..=511 (snapshot id is pre-shrink state)
let snap_a = db_a.shrink_bucket(vec![342..=511])?;

// B gives out 684..=1023
let snap_b = db_b.shrink_bucket(vec![684..=1023])?;

// C keeps only 684..=1023 after bootstrapping from B snapshot
let snap_c = db_c.shrink_bucket(vec![512..=683])?;
```

### Phase 2: expand (import to targets)

```rust
// B imports 342..=511 from A's pre-shrink snapshot
db_b.expand_bucket(db_a.id(), Some(snap_a), Some(vec![342..=511]))?;
```

After all expands are done, take new shard snapshots and materialize a new global snapshot with `DbCoordinator`.
