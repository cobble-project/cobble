<p align="center"><img src="logo.png" width="60%" alt="Cobble logo" /></p>
<p align="center">
  <a href="https://crates.io/crates/cobble"><img alt="crates.io" src="https://img.shields.io/crates/v/cobble?logo=rust" /></a>
  <a href="#"><img alt="GitHub License" src="https://img.shields.io/github/license/cobble-project/cobble" /></a>
  <a href="https://cobble-project.github.io/cobble/latest/"><img alt="Docs" src="https://img.shields.io/badge/docs-GitHub%20Pages-222222?logo=githubpages" /></a>
  <a href="https://central.sonatype.com/artifact/io.github.cobble-project/cobble"><img alt="Maven Central" src="https://img.shields.io/maven-central/v/io.github.cobble-project/cobble?logo=apachemaven" /></a>
  <a href="https://github.com/cobble-project/cobble/actions/workflows/ci.yml"><img alt="GitHub CI" src="https://img.shields.io/github/actions/workflow/status/cobble-project/cobble/ci.yml?label=CI&logo=githubactions" /></a>
  <a href="https://docs.rs/cobble/latest/cobble/"><img alt="docs.rs" src="https://img.shields.io/badge/docs-docs.rs-00A1FF" /></a>
</p>

Cobble is a high-performance, embedded key-value store designed for use in distributed systems as well as standalone applications.
This project aims to provide a highly flexible storage engine for various services or applications.
It is built-on Rust and implements an LSM (Log-Structured Merge) tree architecture using multiple file formats (SST, parquet, etc.).
It fulfills all your expectations for embedded LSM storage and can be integrated into various distributed systems and standalone applications as the underlying storage.

## Features

We list some of Cobble's key features below, they are either implemented or are planned for future releases:

- **Hybrid Media**: Local disk and remote object storage (S3, OSS, etc.) can be used individually or together; supports multi-volume distributed I/O scheduling.
- **Schema Support & Evolution**: User-defined column schemas with incremental evolution.
- **Multiple File Formats**: SST and Parquet for both point lookup and analytical queries.
- **One writer, multiple readers**: A single writer for consistency, with concurrent readers across processes or machines.
- **Remote Compaction**: Compaction can run on remote object storage to reduce local resource usage.
- **Multi-version Snapshots**: Read historical data states via versioned snapshots.
- **Key-value Separation**: Separates keys and values to optimize large-value, low-access patterns.
- **Time-to-live (TTL)**: Expire and clean up data automatically.
- **Hot/Cold Separation**: Optimize storage and access efficiency with multiple strategies.
- **Merge Operators**: Support for user-defined merge operations on values. Efficiently handle updates without reading existing values.
- **Multi-language Bindings**: Now java-binding supported. Planned support for C, C++, Python and Go bindings.

## Install

Add `cobble` to your `Cargo.toml`:

```toml
[dependencies]
cobble = "0.1.0"
```

Cobble uses Apache OpenDAL for volume backends.

The local `file://` backend is always enabled by default and does not require any Cargo feature.
Optional remote/storage-service features exposed by Cobble are:

- `storage-alluxio`
- `storage-cos`
- `storage-oss`
- `storage-s3`
- `storage-ftp`
- `storage-hdfs`
- `storage-sftp`

```toml
[dependencies]
cobble = { version = "0.1.0", default-features = false, features = ["storage-s3"] }
```

- Enable all optional remote/storage-service backends: `storage-all`
- Crates that depend on `cobble` in this workspace (for example `cobble-cli`, `cobble-web-monitor`, `cobble-cluster`, `cobble-bench`, `cobble-data-structure`, `cobble-java`) also re-expose the same `storage-*` feature names and forward them to `cobble`.

## Getting Started

Cobble usage can be viewed as **step 0 + five patterns**.

For complete guides and more examples, see docs:
- https://cobble-project.github.io/cobble/latest/
- https://cobble-project.github.io/cobble/latest/getting-started/

### 0) Config first: understand volumes

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

### 1) Single-machine embedded DB (`SingleDb`)

This is the simplest mode. You run one embedded process with local write/read and
single-node global snapshots.

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

### 2) Distributed write/read with `N x Db` + `1 x DbCoordinator`

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

See full distributed setup and restore examples:
https://cobble-project.github.io/cobble/latest/getting-started/distributed/

### 3) Real-time read while writing (`Reader`)

```rust
use cobble::{Reader, ReaderConfig, VolumeDescriptor};

let read_config = ReaderConfig {
    volumes: VolumeDescriptor::single_volume("file:///tmp/cobble"),
    total_buckets: 1024,
    ..ReaderConfig::default()
};
let mut reader = Reader::open_current(read_config)?;
let v = reader.get(0, b"user:1")?;
reader.refresh()?; // pull newer materialized snapshot
```

More `Reader` details:
https://cobble-project.github.io/cobble/latest/getting-started/reader-and-scan/

### 4) Distributed scan on one snapshot

```rust
use cobble::{ScanOptions, ScanPlan};

let plan = ScanPlan::new(global_manifest);
for split in plan.splits() {
    let scanner = split.create_scanner(config.clone(), &ScanOptions::default())?;
    for row in scanner {
        let (key, columns) = row?;
        // process row...
    }
}
```

### 5) Structured DB wrappers (typed columns)

`cobble-data-structure` provides typed wrappers for all flows above:
- Single-machine embedded: `StructuredSingleDb`
- Distributed write shards: `StructuredDb`
- Real-time read: `StructuredReader`
- Snapshot pinned read: `StructuredReadOnlyDb`
- Distributed scan: `StructuredScanPlan` / `StructuredScanSplit`

All snapshot/read/scan patterns are the same as core `cobble`, but values are
encoded/decoded as structured typed columns (`Bytes`/`List`).

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
  .add_list_column(1, ListConfig {
      max_elements: Some(100),
      retain_mode: ListRetainMode::Last,
      preserve_element_ttl: false,
  })
  .commit()?;

db.put(0, b"k1", 0, StructuredColumnValue::Bytes(Bytes::from_static(b"v0")))?;
```

More scan examples:
https://cobble-project.github.io/cobble/latest/getting-started/reader-and-scan/

More structured examples:
https://cobble-project.github.io/cobble/latest/getting-started/structured-db/

### Build / test

- Format: `cargo fmt --all`
- Lint: `cargo clippy -- -D warnings`
- Test: `cargo test`

## Contributing

We welcome contributions from the community! Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines on how to contribute to the project.

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Maintainers

- [Zakelly](https://github.com/zakelly) - Project Founder & Main Developer
