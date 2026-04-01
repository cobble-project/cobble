<p align="center"><img src="logo.png" width="60%" alt="Cobble logo" /></p>

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

## Getting Started

Cobble usage can be viewed as **step 0 + five patterns**.

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
config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble".to_string());
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
    config.volumes = VolumeDescriptor::single_volume("file:///tmp/cobble-single".to_string());

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

Core components:
- `N` shard writers (`Db`), each owning one or more bucket ranges.
- `1` `DbCoordinator`, storing/materializing global snapshot manifests.

Snapshot flow:
1. Each shard calls `Db::snapshot()` (or `snapshot_with_callback`) and produces `ShardSnapshotInput`.
2. Coordinator calls `take_global_snapshot(total_buckets, shard_inputs)`.
3. Coordinator persists it with `materialize_global_snapshot(&manifest)`.

Recover flow (historical read/write resume):
1. Restore coordinator state first (load global snapshot manifest/current pointer).
2. Then restore each shard DB with
   `Db::open_from_snapshot(config, shard_snapshot_id, db_id)`.
3. Continue write/read on restored shard DB instances.

Remote compaction (optional in distributed mode):
1. Start remote compaction worker process:
   - create `RemoteCompactionServer::new(server_config)?`
   - call `server.serve("0.0.0.0:PORT")?`
2. On each writer `Db` config, set:
   - `config.compaction_remote_addr = Some("HOST:PORT".to_string())`
3. Open DB as usual; compaction tasks will be offloaded to remote worker.

For structured wrappers, use `StructuredRemoteCompactionServer` in
`cobble-data-structure` when you need structured merge-operator resolution.

### 3) Real-time read while writing (`Reader`)

Use `Reader` in services that should read continuously while writer shards keep
writing and generating snapshots.

- Open: `Reader::open_current(reader_config)` (or `Reader::open(reader_config, snapshot_id)`).
- Read: `reader.get(...)` / `reader.scan(...)`.
- Refresh pointer: `reader.refresh()` to pick newer global snapshots.

### 4) Distributed scan on one snapshot

Given a `GlobalSnapshotManifest`:
1. Build `ScanPlan::new(manifest)` and set optional `with_start`/`with_end`.
2. Generate splits via `plan.splits()`.
3. Dispatch each `ScanSplit` to workers.
4. Worker opens scanner: `split.create_scanner(config, &scan_options)`.
5. Iterate `ScanSplitScanner` to consume key/column rows.

### 5) Structured DB wrappers (typed columns)

`cobble-data-structure` provides typed wrappers for all flows above:
- Single-machine embedded: `StructuredSingleDb`
- Distributed write shards: `StructuredDb`
- Real-time read: `StructuredReader`
- Snapshot pinned read: `StructuredReadOnlyDb`
- Distributed scan: `StructuredScanPlan` / `StructuredScanSplit`

All snapshot/read/scan patterns are the same as core `cobble`, but values are
encoded/decoded as structured typed columns (`Bytes`/`List`).

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
