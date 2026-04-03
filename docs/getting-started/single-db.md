---
title: Single-Machine Embedded DB
parent: Getting Started
nav_order: 4
---

# Single-Machine Embedded DB

`SingleDb` is a convenience wrapper that bundles a single-shard `Db` with a `DbCoordinator`. It is the simplest way to use Cobble as an embedded key-value store.

## Open a New Database

```rust
use cobble::{Config, SingleDb, VolumeDescriptor};

let mut config = Config::default();
config.volumes = VolumeDescriptor::single_volume("file:///tmp/my-db".to_string());

let db = SingleDb::open(config)?;
```

## Write Data

```rust
// Put a value (bucket 0, column 0)
db.put(0, b"user:1", 0, b"Alice")?;

// Merge a value
db.merge(0, b"counter", 0, b"\x01\x00\x00\x00")?;

// Delete a key
db.delete(0, b"user:2", 0)?;
```

## Read Data

```rust
use cobble::ReadOptions;

let value = db.get_with_options(0, b"user:1", &ReadOptions::default())?;
if let Some(v) = value {
    println!("Found: {:?}", v);
}
```

## Snapshot and Resume

Snapshots capture a consistent point-in-time view of the database:

```rust
// Take a snapshot — returns a global snapshot ID
let snapshot_id = db.snapshot()?;
println!("Snapshot ID: {}", snapshot_id);

// Drop the database (simulating process restart)
drop(db);

// Resume from the snapshot
let mut config = Config::default();
config.volumes = VolumeDescriptor::single_volume("file:///tmp/my-db".to_string());
let db = SingleDb::resume(config, snapshot_id)?;

// Data is intact
let value = db.get_with_options(0, b"user:1", &ReadOptions::default())?;
assert!(value.is_some());
```

{: .important }
> When resuming, you must use the same volume configuration that was active when the snapshot was taken. All referenced data files must be accessible.

## Snapshot with Callback

You can register a callback that fires after the snapshot manifest is persisted. The callback receives the manifest object:

```rust
db.snapshot_with_callback(|manifest| {
    if let Ok(manifest) = manifest {
        println!("Snapshot {} materialized", manifest.id);
    }
    // Use manifest for downstream processing
})?;
```

## Scan

```rust
use cobble::ScanOptions;

let scanner = db.scan_with_options(
    0,
    b"a".as_ref()..b"z".as_ref(), // [start, end)
    &ScanOptions::default(),
)?;

for row in scanner {
    let (key, columns) = row?;
    // process key-value pair
}
```

## Write with TTL

```rust
use cobble::WriteOptions;

let mut config = Config::default();
config.ttl_enabled = true;
config.default_ttl_seconds = Some(3600); // 1 hour default

let db = SingleDb::open(config)?;

// Write with explicit TTL
db.put_with_options(0, b"session:abc", 0, b"data", &WriteOptions::with_ttl(300))?;
```
