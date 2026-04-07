---
title: Structured DB
parent: Getting Started
nav_order: 8
---

# Structured DB

The `cobble-data-structure` crate provides typed wrappers over raw byte columns.

Current supported structured types are:

- `Bytes` (default for all columns)
- `List` (with list-specific merge semantics)

More types are planned for the future. It does **not** provide arbitrary POJO/struct auto-mapping. You configure column types with `StructuredSchema`, then read/write `StructuredColumnValue`.

`StructuredDb::open(...)` and `StructuredSingleDb::open(...)` open without schema arguments. They start with default schema (all columns `Bytes`), then evolve schema via `current_schema()` + `update_schema() ... commit()`.

## StructuredSingleDb

```rust
use bytes::Bytes;
use cobble::{Config, VolumeDescriptor};
use cobble_data_structure::{
    ListConfig, ListRetainMode, StructuredColumnValue, StructuredSingleDb,
};

let mut config = Config::default();
config.num_columns = 2;
config.total_buckets = 1;
config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-structured-single");
let mut db = StructuredSingleDb::open(config)?;
db.update_schema()
  .add_list_column(1, ListConfig {
      max_elements: Some(100),
      retain_mode: ListRetainMode::Last,
      preserve_element_ttl: false,
  })
  .commit()?;

// bytes column
db.put(0, b"k1", 0, StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))?;

// list column (append via merge)
db.merge(0, b"k1", 1, StructuredColumnValue::List(vec![
    Bytes::from_static(b"a"),
    Bytes::from_static(b"b"),
]))?;

let row = db.get(0, b"k1")?.unwrap();
```

## StructuredDb (Distributed shard writer)

```rust
use cobble::{Config, VolumeDescriptor};
use cobble_data_structure::{ListConfig, ListRetainMode, StructuredDb};

let mut config = Config::default();
config.num_columns = 2;
config.total_buckets = 1024;
config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-structured-dist");
let mut db = StructuredDb::open(config, vec![0u16..=1023u16])?;
db.update_schema()
  .add_list_column(1, ListConfig {
      max_elements: Some(100),
      retain_mode: ListRetainMode::Last,
      preserve_element_ttl: false,
  })
  .commit()?;
let snap = db.snapshot()?;
let shard_input = db.shard_snapshot_input(snap)?;
```

Snapshot/coordinator flow is the same as raw `Db`, but values are encoded/decoded with the structured schema.

## Schema Management APIs

Both `StructuredDb` and `StructuredSingleDb` expose:

- `current_schema()` — returns current structured schema
- `update_schema()` — returns a schema builder that directly holds and mutates inner `cobble::SchemaBuilder`
- builder `.commit()` — persists and applies schema changes
- `reload_schema()` — reloads from persisted underlying cobble schema

Builder methods are applied incrementally to the inner schema builder (matching core schema-builder semantics):

- `add_bytes_column(column)` — set column type back to `Bytes`
- `add_list_column(column, config)` — set column type to `List`
- `delete_column(column)` — remove structured typing for this column (fallback to `Bytes`)

## StructuredReader and StructuredReadOnlyDb

Corresponding structured wrappers are provided for `Reader` and `ReadOnlyDb`:

```rust
use cobble::{Config, ReaderConfig, VolumeDescriptor};
use cobble_data_structure::{StructuredReader, StructuredReadOnlyDb};

let read_config = ReaderConfig {
    volumes: VolumeDescriptor::single_volume("file:///tmp/my-db"),
    total_buckets: 1024,
    ..ReaderConfig::default()
};

let mut reader = StructuredReader::open_current(read_config)?;
let row = reader.get(0, b"k1")?;
reader.refresh()?;

let mut config = Config::default();
config.num_columns = 2;
config.total_buckets = 1024;
config.volumes = VolumeDescriptor::single_volume("file:///tmp/my-db");
let ro = StructuredReadOnlyDb::open(config, snapshot_id, db_id)?;
let row2 = ro.get(0, b"k1")?;
```

## Structured Distributed Scan

```rust
use cobble::ScanOptions;
use cobble_data_structure::StructuredScanPlan;

let plan = StructuredScanPlan::new(global_manifest);
for split in plan.splits() {
    let scanner = split.create_scanner(config.clone(), &ScanOptions::default())?;
    for row in scanner {
        let (key, columns) = row?;
        // columns: Vec<Option<StructuredColumnValue>>
    }
}
```

## Projection Notes

`ReadOptions`/`ScanOptions` projection is supported. The structured wrappers remap schema indices correctly for projected columns, so decoded values match the projected column order.
