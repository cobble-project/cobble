# cobble

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

## Storage backend features (OpenDAL)

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

## Simple Example

Creating a storage, write and read, then snapshot.

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
    let value1 = db.get(0, b"user:1").unwrap().expect("user:1 should have a value");
    assert_eq!(value1[0].as_ref().unwrap().as_ref(), b"Alice");
    assert_eq!(value1[1].as_ref().unwrap().as_ref(), b"premium");
    let global_snapshot_id = db.snapshot()?;
    println!("snapshot id = {}", global_snapshot_id);
    Ok(())
}
```

## Docs

- API docs: https://docs.rs/cobble
- Project docs: https://cobble-project.github.io/cobble/latest/
- Repository: https://github.com/cobble-project/cobble
