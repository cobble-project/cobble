---
title: Multi-Volume Storage
parent: Architecture
nav_order: 8
---

# Multi-Volume Storage

Cobble's volume system lets you spread data across multiple storage devices and manage capacity through automatic offload. This is key to running Cobble efficiently in production, where fast local SSDs, large network-attached storage, and cloud object stores each play a different role.

## Volume Roles

Each volume is tagged with one or more usage kinds that control what data lives there:

| Kind | What goes here | Typical hardware |
|------|---------------|-----------------|
| **PrimaryData (High)** | Active data files — SSTs, Parquet files, VLOG segments | Fast local SSD |
| **PrimaryData (Medium)** | Warm data files — offloaded from high-priority when full | Larger SSD or NVMe |
| **PrimaryData (Low)** | Cold data files — final offload tier | Network-attached or HDD |
| **Meta** | Snapshot manifests, schema files, global pointers | Any reliable storage |
| **Snapshot** | Materialized snapshot copies for sharing or backup | Cloud object store or shared FS |
| **Cache** | Disk tier for the [hybrid block cache](block-cache) | Fast local SSD |

A single volume can serve multiple roles. For example, a local SSD can be both `PrimaryData(High)` and `Meta`.

## How Data Flows Between Volumes

New data always lands on the highest-priority primary volume — this is where [memtable flushes](memtable) write SST/Parquet files and where [VLOG](key-value-separation) segments are created.

When that volume approaches its configured `size_limit`, Cobble's **offload runtime** automatically migrates files to the next lower-priority primary volume. The migration is asynchronous and non-blocking — reads continue from the original file until the copy is confirmed, at which point the file reference is atomically swapped.

```
Writes → High-priority SSD → (offload when full) → Medium-priority SSD → Low-priority storage
```

This tiered approach means your hottest data stays on the fastest storage, while colder data naturally migrates to cheaper, larger volumes without any application-level management.

## Offload Behavior

The offload system is designed to be safe and unobtrusive:

- **Non-blocking reads**: A file being migrated remains readable from its original location until the copy completes.
- **Backpressure**: Only a limited number of migrations run concurrently. New migrations are not scheduled while prior ones are in flight.
- **Snapshot awareness**: Files that exist only as [snapshot](snapshot) replicas are never migrated. If a snapshot replica already exists on the target volume, the offload promotes the existing copy instead of making a new one.

## Snapshot Volumes

When you take a [snapshot](snapshot), Cobble copies data files to snapshot-capable volumes. If you configure dedicated snapshot volumes (tagged `Snapshot`), copies go there. If no dedicated snapshot volumes exist, Cobble falls back to primary volumes.

When the `snapshot` and `primary` roles are bundled on the same volume, snapshots share storage with active data. This is optimal and best for most use cases, since snapshots will not involve additional I/O beyond the initial copy. During recovery, the snapshot system can read directly from the primary volume without needing to fetch from a separate snapshot location.

Snapshot volumes are especially useful when you want to:

- **Export to cloud storage**: Configure an S3-backed volume as a snapshot target. Snapshot materialization uploads files there, and any system with access to that bucket can restore from the snapshot.
- **Isolate snapshot I/O**: Dedicated snapshot volumes prevent snapshot copy traffic from competing with active read/write I/O on primary volumes.

{: .warning }
> When restoring from a snapshot, the restoring process must be able to access **all** files referenced by that snapshot. Ensure the snapshot volumes are reachable from any machine that may need to restore.

The `meta` role is a special kind of snapshot storage, and is used for storing the snapshot manifest and schema files. These files are small but critical for restore, so they should be on reliable storage. If no `meta` volumes are configured, the manifest and schema files are stored on the same primary volume as the data.


## Cache Volumes

The [hybrid block cache](block-cache) uses a cache volume as its disk tier. When enabled, frequently accessed blocks are cached on a local SSD, reducing reads from slower primary volumes.

Cache volumes must be on **local** storage (not network-attached). The cache is ephemeral — it is rebuilt on restart and cleaned up on shutdown.

## Minimal Configuration

For development or single-machine deployments, a single volume is sufficient:

```rust
use cobble::{Config, VolumeDescriptor};

let mut config = Config::default();
config.volumes = VolumeDescriptor::single_volume("file:///data/cobble");
```

This puts everything — data, metadata, and snapshots — in one directory. As your deployment grows, you can add more volumes without changing application code.

## Multi-Volume Example

A production setup might use three volumes:

```rust
use cobble::{Config, VolumeDescriptor, VolumeUsageKind};

let mut config = Config::default();

// Fast local SSD for active data + cache
let mut high = VolumeDescriptor::new(
    "file:///fast-ssd/cobble",
    vec![
        VolumeUsageKind::PrimaryDataPriorityHigh,
        VolumeUsageKind::Cache,
    ],
);
high.size_limit = Some(100 * 1024 * 1024 * 1024); // 100 GB

// Larger SSD for warm data
let medium = VolumeDescriptor::new(
    "file:///large-ssd/cobble",
    vec![VolumeUsageKind::PrimaryDataPriorityMedium],
);

// S3 for snapshot export
let snapshot = VolumeDescriptor::new(
    "s3://my-bucket/cobble-snapshots",
    vec![
        VolumeUsageKind::Snapshot,
        VolumeUsageKind::Meta,
    ],
);

config.volumes = vec![high, medium, snapshot];
```

With this configuration:
1. Writes land on the fast SSD.
2. When the fast SSD fills to 100 GB, older files are offloaded to the larger SSD.
3. Snapshots are materialized to S3 for cross-region or cross-cluster restore. But since the snapshot and primary roles are separated, there are many I/O during snapshot materialization as files are copied to S3, as well as during restore when files are fetched back from S3. If you want to avoid this, you can combine the snapshot and primary roles on the same local SSD or S3.
4. The hybrid block cache uses the fast SSD's cache volume for its disk tier.

Another example might use a shared network filesystem for primary data and a local SSD for cache:

```rust
use cobble::{Config, VolumeDescriptor, VolumeUsageKind};

let mut config = Config::default();

// Network-attached storage for primary data and snapshots
let primary = VolumeDescriptor::new(
    "file:///network-storage/cobble",
    vec![
        VolumeUsageKind::PrimaryDataPriorityHigh,
        VolumeUsageKind::Snapshot,
        VolumeUsageKind::Meta,
    ],
);

// Local SSD for block cache
let cache = VolumeDescriptor::new(
    "file:///local-ssd/cobble-cache",
    vec![VolumeUsageKind::Cache],
);

config.volumes = vec![primary, cache];
```

With this setup, all data and snapshots are on the network storage, but the block cache is on a local SSD for better performance. This is a common pattern when you have a shared filesystem that all nodes can access, but want to leverage local disks for caching.
During snapshot and restore, the system reads and writes directly to the primary volume, so there is no additional I/O overhead from copying to a separate snapshot volume. The local SSD is used solely for caching, so it does not need to be involved in snapshot materialization or restore.
This configuration is also suitable for remote compaction servers, which require access to the same volumes as the writers but do not need to perform writes themselves. The remote compaction server can be configured with the same volume list, and it will read from the primary volumes as needed to execute compactions.

## Path Normalization

Volume paths are normalized at startup — relative paths are resolved to absolute `file://` URLs, and trailing slashes are standardized. This means `./data`, `/tmp/data`, and `file:///tmp/data` all work and are treated consistently.
