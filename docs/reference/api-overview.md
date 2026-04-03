---
title: API Overview
parent: Reference
nav_order: 2
---

# API Overview

This page summarizes the public Rust API surface of the Cobble crates.

## cobble (Core Crate)

### Primary Types

| Type | Description |
|------|-------------|
| `SingleDb` | Single-machine embedded database (wraps Db + Coordinator) |
| `Db` | Shard database for distributed deployments |
| `ReadOnlyDb` | Read-only snapshot access |
| `Reader` | Snapshot-following read proxy (visibility advances by snapshot cadence) |
| `DbCoordinator` | Global snapshot coordinator |

### Scan Types

| Type | Description |
|------|-------------|
| `ScanPlan` | Distributed scan plan from a global snapshot |
| `ScanSplit` | Serializable unit of scan work (one per shard) |
| `ScanSplitScanner` | Iterator over key-value pairs within a split |

### Configuration Types

| Type | Description |
|------|-------------|
| `Config` | Main database configuration |
| `CoordinatorConfig` | Coordinator configuration |
| `VolumeDescriptor` | Storage volume descriptor |
| `VolumeUsageKind` | Volume usage kind enum |
| `ReadOptions` | Point lookup options |
| `ScanOptions` | Scan/iteration options |
| `WriteOptions` | Write operation options |

### Compaction Types

| Type | Description |
|------|-------------|
| `RemoteCompactionServer` | Remote compaction worker server |

### Key Operations

#### SingleDb

```rust
SingleDb::open(config) -> Result<SingleDb>
SingleDb::resume(config, global_snapshot_id) -> Result<SingleDb>
db.put(bucket, key, column, value) -> Result<()>
db.merge(bucket, key, column, value) -> Result<()>
db.delete(bucket, key, column) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.snapshot() -> Result<u64>
db.snapshot_with_callback(callback) -> Result<u64>
```

#### Db

```rust
Db::open(config, bucket_ranges) -> Result<Db>
Db::open_from_snapshot(config, snapshot_id, db_id) -> Result<Db>
ReadOnlyDb::open_with_db_id(config, snapshot_id, db_id) -> Result<ReadOnlyDb>
db.put(bucket, key, column, value) -> Result<()>
db.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
db.snapshot() -> Result<u64>
db.shard_snapshot_input(snapshot_id) -> Result<ShardSnapshotInput>
```

#### Reader

```rust
Reader::open_current(reader_config) -> Result<Reader>
reader.get_with_options(bucket, key, &read_options) -> Result<Option<Vec<Option<Bytes>>>>
reader.refresh() -> Result<()>
```

#### Scan

```rust
ScanPlan::new(manifest) -> ScanPlan
plan.splits() -> Vec<ScanSplit>
split.create_scanner(config, &scan_options) -> Result<ScanSplitScanner>
for row in scanner { let (key, columns) = row?; }
```

---

## cobble-data-structure (Structured Wrappers)

### Types

| Type | Description |
|------|-------------|
| `StructuredSingleDb` | Structured SingleDb wrapper |
| `StructuredDb` | Structured Db wrapper |
| `StructuredReadOnlyDb` | Structured ReadOnlyDb wrapper |
| `StructuredReader` | Structured Reader wrapper |
| `StructuredScanPlan` | Structured scan plan |
| `StructuredScanSplit` | Structured scan split |
| `StructuredScanSplitScanner` | Structured scan scanner |
| `StructuredRemoteCompactionServer` | Remote compaction with structured merge ops |

Structured values are represented with `StructuredColumnValue` and configured by `StructuredSchema` (`Bytes` / `List` column types).

---

## cobble-web-monitor

### Types

| Type | Description |
|------|-------------|
| `MonitorConfig` | Web monitor configuration |
| `MonitorServer` | HTTP server for monitoring dashboard |
| `MonitorServerHandle` | Handle to control the running server |

---

## cobble-java

The Java API mirrors the Rust API. See [Java Bindings](../ffi-bindings/java) for usage details.

### Java Classes

| Class | Rust Equivalent |
|-------|-----------------|
| `io.cobble.SingleDb` | `SingleDb` |
| `io.cobble.Db` | `Db` |
| `io.cobble.Reader` | `Reader` |
| `io.cobble.Config` | `Config` |
| `io.cobble.ScanPlan` | `ScanPlan` |
| `io.cobble.ScanSplit` | `ScanSplit` |
| `io.cobble.ScanCursor` | Raw scan iterator cursor |
| `io.cobble.structured.SingleDb` | Structured `SingleDb` |
| `io.cobble.structured.Db` | Structured `Db` |
| `io.cobble.structured.StructuredScanSplit` | Structured distributed split |
| `io.cobble.structured.ScanCursor` | Structured scan iterator cursor |
