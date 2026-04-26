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
  .add_list_column(None, 1, ListConfig {
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

## Structured Column Families

Structured wrappers use the same family-selection model as raw Cobble:

- add or delete typed columns with `Option<String>` family arguments on the schema builder;
- use `StructuredWriteOptions`, `StructuredReadOptions`, and `StructuredScanOptions` to select a named family at write/read/scan time.

```rust
use cobble_data_structure::{StructuredReadOptions, StructuredScanOptions, StructuredWriteOptions};

db.update_schema()
  .add_list_column(Some("metrics".to_string()), 0, ListConfig {
      max_elements: Some(100),
      retain_mode: ListRetainMode::Last,
      preserve_element_ttl: false,
  })
  .commit()?;

db.put_with_options(
    0,
    b"k-metrics",
    0,
    StructuredColumnValue::List(vec![Bytes::from_static(b"a")]),
    &StructuredWriteOptions::with_column_family("metrics"),
)?;

let row = db.get_with_options(
    0,
    b"k-metrics",
    &StructuredReadOptions::for_column_in_family("metrics", 0),
)?;

let scanner = db.scan_with_options(
    0,
    b"k".as_ref()..b"l".as_ref(),
    &StructuredScanOptions::for_column(0).with_column_family("metrics"),
)?;
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
  .add_list_column(None, 1, ListConfig {
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
- `current_schema().column_families()` — returns family-local structured schemas keyed by family name and always includes `default`
- `update_schema()` — returns a schema builder that directly holds and mutates inner `cobble::SchemaBuilder`
- builder `.commit()` — persists and applies schema changes
- `reload_schema()` — reloads from persisted underlying cobble schema

Builder methods are applied incrementally to the inner schema builder (matching core schema-builder semantics):

- `add_bytes_column(None, column)` / `add_bytes_column(Some("metrics".to_string()), column)` — set column type to `Bytes`
- `add_list_column(None, column, config)` / `add_list_column(Some("metrics".to_string()), column, config)` — set column type to `List`
- `delete_column(None, column)` / `delete_column(Some("metrics".to_string()), column)` — remove structured typing for this family-local column

Adding a typed column in a new family also creates that family in the underlying raw schema.

## StructuredReader and StructuredReadOnlyDb

Corresponding structured wrappers are provided for `Reader` and `ReadOnlyDb`:

```rust
use cobble::{Config, ReaderConfig, VolumeDescriptor};
use cobble_data_structure::{
    StructuredReadOnlyDb, StructuredReadOptions, StructuredReader, StructuredScanOptions,
};

let read_config = ReaderConfig {
    volumes: VolumeDescriptor::single_volume("file:///tmp/my-db"),
    total_buckets: 1024,
    ..ReaderConfig::default()
};

let mut reader = StructuredReader::open_current(read_config)?;
let row = reader.get_with_options(
    0,
    b"k1",
    &StructuredReadOptions::for_column_in_family("metrics", 0),
)?;
reader.refresh()?;

let mut config = Config::default();
config.num_columns = 2;
config.total_buckets = 1024;
config.volumes = VolumeDescriptor::single_volume("file:///tmp/my-db");
let ro = StructuredReadOnlyDb::open(config, snapshot_id, db_id)?;
let row2 = ro.get_with_options(
    0,
    b"k1",
    &StructuredReadOptions::for_column_in_family("metrics", 0),
)?;
let scanner = ro.scan_with_options(
    0,
    b"k".as_ref()..b"l".as_ref(),
    &StructuredScanOptions::for_column(0).with_column_family("metrics"),
)?;
```

## Structured Distributed Scan

```rust
use cobble_data_structure::{StructuredScanOptions, StructuredScanPlan};

let plan = StructuredScanPlan::new(global_manifest); // still bucket-only
for split in plan.splits() {
    let scanner = split.create_scanner(
        config.clone(),
        &StructuredScanOptions::for_column(0).with_column_family("metrics"),
    )?;
    for row in scanner {
        let (key, columns) = row?;
        // columns: Vec<Option<StructuredColumnValue>>
    }
}
```

As with raw scans, the family is chosen when creating the scanner. If you omit `with_column_family(...)`, the scanner reads the default family.

## Projection Notes

`StructuredReadOptions`/`StructuredScanOptions` projection is supported. Projection indices are interpreted inside the selected column family, and the structured wrappers remap schema indices correctly so decoded values match the projected column order.
