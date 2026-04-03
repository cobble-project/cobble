---
title: Reader & Distributed Scan
parent: Getting Started
nav_order: 6
---

# Reader & Distributed Scan

## Reader — Snapshot-Following Read Service

`Reader` is **not** a per-write real-time view. It reads from materialized global snapshots, so visibility advances only when:

- writers create and materialize a newer snapshot, and
- the reader refreshes (auto or manual) to that snapshot.

This makes `Reader` ideal for low-latency serving on a stable view, but with freshness bounded by snapshot cadence.

### Opening a Reader

```rust
use cobble::{Reader, ReaderConfig, VolumeDescriptor};

let read_config = ReaderConfig {
    volumes: VolumeDescriptor::single_volume("file:///tmp/my-db".to_string()),
    total_buckets: 1024,
    ..ReaderConfig::default()
};

let mut reader = Reader::open_current(read_config)?;
```

### Point Lookup

```rust
let value = reader.get(0, b"user:1")?;
```

### Refreshing and Visibility

Call `refresh()` to pick up a newer materialized snapshot:

```rust
reader.refresh()?;
```

In `open_current` mode, reads may auto-check pointer changes, throttled by `reader.reload_tolerance_seconds` (default: 10s). If writers snapshot every minute, reader-visible data can lag by up to about one snapshot interval plus refresh tolerance.

### Reader Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `reader.pin_partition_in_memory_count` | 1 | Number of partition snapshots pinned in memory |
| `reader.block_cache_size` | 512 MB | Block cache size for reader |
| `reader.reload_tolerance_seconds` | 10 | Minimum interval between snapshot reload checks |

## ReadOnlyDb — Snapshot-Based Read on one shard

`ReadOnlyDb` opens a specific snapshot for read-only access **on one shard** (corresponding to a single `Db`):

```rust
use cobble::{Config, ReadOnlyDb};

let read_only = ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id)?;
let value = read_only.get(0, b"key")?;
```

## Distributed Scan

Cobble supports distributed scan operations where work is split across multiple workers. This follows a **plan → split → scan** execution model.

### 1. Create a Scan Plan

A `ScanPlan` is generated from a global snapshot manifest:

```rust
use cobble::ScanPlan;

let plan = ScanPlan::new(global_manifest);
```

### 2. Generate Splits

Each split represents a unit of work (currently one shard = one split):

```rust
let splits = plan.splits();
// splits.len() == number of shards in the snapshot
```

### 3. Dispatch and Execute

Splits are serializable and can be sent to distributed workers:

```rust
use cobble::ScanOptions;

for split in splits {
    // Each worker opens a scanner from its split
    let scanner = split.create_scanner(config.clone(), &ScanOptions::default())?;

    for row in scanner {
        let (key, columns) = row?;
        // process...
    }
}
```

### Scan Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `read_ahead_bytes` | 0 | Read-ahead buffer size (0 = disabled) |
| `column_indices` | `None` | Column projection — only read specified columns |

### Column Projection

To read only specific columns, use `ScanOptions`:

```rust
let mut opts = ScanOptions::default();
opts.column_indices = Some(vec![0, 2]); // only columns 0 and 2
```

### Bounded Scan

Bounds are set on the plan and copied to each split:

```rust
use cobble::ScanPlan;

let plan = ScanPlan::new(global_manifest)
    .with_start(b"start_key".to_vec())   // inclusive
    .with_end(b"end_key".to_vec());      // exclusive
```
