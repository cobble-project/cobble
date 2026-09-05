---
title: Quick Start
parent: Getting Started
nav_order: 1
---

# Quick Start

Start by installing Cobble, then configure storage and choose the API flow that matches your application.

## Install

Add `cobble` to your `Cargo.toml`:

```toml
[dependencies]
cobble = "0.4.0"
```

Cobble uses Apache OpenDAL for volume backends.

The local `file://` backend is always enabled by default and does not require any Cargo feature.
Optional remote/storage-service features exposed by Cobble are:

- `storage-alluxio`
- `storage-cos`
- `storage-oss`
- `storage-s3`
- `storage-ftp`
- `storage-goosefs`
- `storage-hdfs`
- `storage-sftp`

> On Windows, `storage-hdfs` and `storage-sftp` are currently not supported.

```toml
[dependencies]
cobble = { version = "0.4.0", default-features = false, features = ["storage-s3"] }
```

- Enable all optional remote/storage-service backends: `storage-all`
- Workspace crates re-expose the same `storage-*` feature names and forward them to `cobble`; language bindings do so through the internal `cobble-binding` crate.

## 0) Config first: understand volumes

Before any API flow, define `Config` and volume layout.

Volume categories (`VolumeUsageKind`) and their roles:

- `PrimaryDataPriorityHigh/Medium/Low`: main data files (SST/parquet/VLOG) with priority-aware placement.
- `Meta`: metadata (manifests, pointers, schema files).
- `Snapshot`: snapshot materialization target when separated from primary.
- `Cache`: block cache disk tier (when hybrid cache is enabled).
- `Readonly`: read-only source volumes for loading historical files.

Minimal practical setup: one local path via `VolumeDescriptor::single_volume(...)`.
This is the simplest single-path deployment and is enough for local development.

```rust
use cobble::{Config, VolumeDescriptor};

let mut config = Config::default();
config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble");
```

> [!IMPORTANT]
> For **any restore/resume flow**, runtime must still be able to access **all files referenced by that snapshot** (snapshot manifests, schema files, and data/VLOG files). If any referenced file is missing or inaccessible, restore can fail.

See [Configuration](configuration.md) for volume setup and storage options.

## 1) Single-machine embedded DB (`SingleDb`)

This is the simplest mode. You run one embedded process with local write/read and single-node global snapshots.

Create + write:

```rust
use cobble::{Config, SingleDb, VolumeDescriptor};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::default();
    config.num_columns = 2;
    config.total_buckets = 1;
    config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-single");

    let db = SingleDb::open(config)?;
    db.put(0, b"user:1", 0, b"Alice")?;
    db.put(0, b"user:1", 1, b"premium")?;
    let global_snapshot_id = db.snapshot()?;
    println!("snapshot id = {}", global_snapshot_id);
    Ok(())
}
```

Recover/read flow:

- Resume directly via `SingleDb::resume(config, global_snapshot_id)`.
- Continue normal read/write on the resumed embedded instance.

See [Single-Machine Embedded DB](single-db.md) for more read, write, and recovery examples.

## 2) Distributed write/read with `N x Db` + `1 x DbCoordinator`

```rust
use cobble::{Config, CoordinatorConfig, Db, DbCoordinator};

// shard writers
let db1 = Db::open(config1, vec![0..=499])?;
let db2 = Db::open(config2, vec![500..=999])?;

// coordinator
let coord = DbCoordinator::open(CoordinatorConfig {
    volumes: coordinator_volumes,
    snapshot_retention: Some(5),
})?;

// write
db1.put(100, b"user:1", 0, b"Alice")?;
db2.put(700, b"order:9", 0, b"paid")?;

// global snapshot
let s1 = db1.snapshot()?;
let s2 = db2.snapshot()?;
let i1 = db1.shard_snapshot_input(s1)?;
let i2 = db2.shard_snapshot_input(s2)?;
let manifest = coord.take_global_snapshot(1000, vec![i1, i2])?;
coord.materialize_global_snapshot(&manifest)?;
```

Remote compaction example:

```rust
let mut config = Config::default();
config.compaction_remote_addr = Some("127.0.0.1:18888".to_string());
```

See [Distributed DB](distributed.md) for the full distributed setup and restore examples, and [Remote Compaction](remote-compaction.md) for dedicated compaction workers.

## 3) Snapshot-following read service (`Reader`)

`Reader` serves a stable materialized snapshot and advances to newer snapshots when refreshed.

```rust
use cobble::{ReadOptions, Reader, ReaderConfig, VolumeDescriptor};

let read_config = ReaderConfig {
    volumes: VolumeDescriptor::single_volume("file:///tmp/cobble"),
    total_buckets: 1024,
    ..ReaderConfig::default()
};
let mut reader = Reader::open_current(read_config)?;
let v = reader.get(0, b"user:1")?;
let metrics = reader.get_with_options(
    0,
    b"user:1",
    &ReadOptions::for_column_in_family("metrics", 0),
)?;
reader.refresh()?; // pull newer materialized snapshot
```

See [Reader & Distributed Scan](reader-and-scan.md#refreshing-and-visibility) for visibility and refresh semantics.

## 4) Distributed scan on one snapshot

```rust
use cobble::{ScanOptions, ScanPlan};

let plan = ScanPlan::new(global_manifest); // plan stays bucket-only
let scan_options = ScanOptions::for_column(0).with_column_family("metrics");

for split in plan.splits() {
    let scanner = split.create_scanner(config.clone(), &scan_options)?;
    for row in scanner {
        let (key, columns) = row?;
        // process row...
    }
}
```

If you want to scan a non-default family, pass it when creating the scanner. `ScanPlan` / `ScanSplit` do not bind a family by themselves.

`create_scanner(...)` keeps the full `ScanOptions` inside the worker-side scanner, so `column_family` still takes effect after a split is serialized and reopened elsewhere.

See [Reader & Distributed Scan](reader-and-scan.md#distributed-scan) for more scan examples.

## 5) Structured DB wrappers (typed columns)

`cobble-data-structure` provides typed wrappers for all flows above:

- Single-machine embedded: `StructuredSingleDb`
- Distributed write shards: `StructuredDb`
- Snapshot-following read: `StructuredReader`
- Snapshot pinned read: `StructuredReadOnlyDb`
- Distributed scan: `StructuredScanPlan` / `StructuredScanSplit`

All snapshot/read/scan patterns are the same as core `cobble`, but values are encoded/decoded as structured typed columns (`Bytes`/`List`).

Structured wrappers are column-family aware too: `StructuredSchema` keeps family-local typed columns, and you still select a family through the same raw `ReadOptions` / `ScanOptions` / `WriteOptions`.

```rust
use bytes::Bytes;
use cobble::{Config, VolumeDescriptor};
use cobble_data_structure::{ListConfig, ListRetainMode, StructuredColumnValue, StructuredSingleDb};

let mut config = Config::default();
config.num_columns = 2;
config.total_buckets = 1;
config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-structured");

let mut db = StructuredSingleDb::open(config)?;
db.update_schema()
  .add_list_column(None, 1, ListConfig {
      max_elements: Some(100),
      retain_mode: ListRetainMode::Last,
      preserve_element_ttl: false,
  })
  .commit()?;

db.put(0, b"k1", 0, StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))?;
```

See [Structured DB](structured-db.md) for more structured examples.
