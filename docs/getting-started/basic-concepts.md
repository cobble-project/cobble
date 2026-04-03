---
title: Basic Concepts
parent: Getting Started
nav_order: 1
---

# Basic Concepts

Before diving into specific usage patterns, it helps to understand the core building blocks of Cobble and how they relate to each other.

## The Key-Value Model

Cobble stores data as **key → multi-column value** pairs, grouped into numbered **buckets**. Buckets provide simple horizontal partitioning — you decide how many buckets exist, and how keys are distributed across them is your application's responsibility (typically a hash of the key).

Each column in a value is independent: you can put, merge, or delete individual columns without affecting the others. The number of columns is fixed per database configuration but can evolve over time with [schema evolution](../architecture/schema-evolution).

For merge operations, you define a [merge operator](../architecture/merge-operator.md) that specifies how to combine new values with existing ones. This is useful for patterns like counters, lists, sets, or any case where you want to update a value based on its current state without reading it first.

## Core Components

### Db

A `Db` is a **single writer shard** — it owns a set of buckets and handles all writes to them. Internally it manages a [memtable](../architecture/memtable), [LSM tree](../architecture/lsm-tree), [compaction](../architecture/compaction), and optionally [value-separated VLOG files](../architecture/key-value-separation).

In a distributed deployment you run multiple `Db` instances, each owning a non-overlapping range of buckets.

> IMPORTANT: All the APIs for `Db` is designed to be invoked in serial. No concurrent calls to the same `Db` instance are allowed. This simplifies the internal design and allows for high performance without locking. If you need concurrent access, use a `Reader` or `Scanner` against a snapshot (see below).

### Coordinator

The `Coordinator` is a **global snapshot manager**. It collects local snapshots from each `Db` shard and combines them into a single **global snapshot** that captures the consistent state of the entire cluster at a point in time.

The Coordinator does not store data — it only stores the lightweight snapshot manifest that points to each shard's data. In a distributed deployment there is exactly one Coordinator.

### SingleDb

`SingleDb` is a **single-machine convenience wrapper** that bundles a `Db` and a `Coordinator` together. It is the right choice for embedded, single-process use cases where you want the full feature set without the overhead of managing two separate components. See [Single-Machine Embedded DB](single-db).

### ReadOnlyDb

`ReadOnlyDb` opens a database **shard in read-only mode** from a saved snapshot. It does not participate in writes and does not maintain a live memtable. It is used for read-only analytical workloads and for driving a [Reader](#reader) or [Scanner](#scanner) from a consistent point-in-time view.

### Reader

A `Reader` provides **snapshot-following reads** against distributed snapshots. It does not observe every write immediately; it observes data once a newer global snapshot is materialized and the reader refreshes to it. Readers expose `get` and scan operations and are safe to use concurrently.

### Scanner

A `Scanner` is a **streaming iterator** over a range of keys within a single bucket or shard. It is the low-level primitive underlying both `Reader` scans and distributed scans.

For distributed analytical workloads, a **`ScanPlan`** is generated from a global snapshot, then split into independent **`ScanSplit`** objects — one per shard. Each split can be sent to a different worker, where it creates its own `Scanner` and iterates independently. See [Reader & Distributed Scan](reader-and-scan).

### StructuredDb (and related wrappers)

All of the above components have **`Structured`** variants (`StructuredDb`, `StructuredSingleDb`, `StructuredReadOnlyDb`, `StructuredReader`, `StructuredScanPlan`, `StructuredScanSplit`) that add typed row encoding and decoding on top of the raw byte API. See [Structured DB](structured-db).

## Snapshots

A **snapshot** is a lightweight, immutable pointer to the state of the database at a specific point in time. Snapshots do not copy data — they record which files and sequence numbers represent a consistent view.

Snapshots serve several purposes:

- **Recovery**: After a restart, you restore from the last known-good snapshot. Any writes that were not captured in a snapshot are lost (similar to how a WAL checkpoint works in other systems).
- **Distributed consistency**: In a multi-shard deployment, a global snapshot ties together the individual shard snapshots into a single consistent view across all `Db` instances.
- **Read isolation**: `ReadOnlyDb` and `Reader` operate against a snapshot, so they see a stable view of the data that doesn't change as new writes arrive.
- **Distributed scan**: A `ScanPlan` is always based on a global snapshot, ensuring that a parallel scan across many shards sees a consistent dataset.

Snapshots are inexpensive to take and are typically triggered after a batch of writes that you want to make durable and recoverable. See [Distributed Deployment](distributed) for the full snapshot workflow.
