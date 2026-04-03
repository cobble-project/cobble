---
title: Memtable
parent: Architecture
nav_order: 2
---

# Memtable

The memtable is Cobble's in-memory write buffer. All incoming writes — `put`, `merge`, `delete` — go into the memtable first, making writes fast and purely in-memory. When the memtable fills up, its contents are flushed to disk as a new L0 file in the [LSM tree](lsm-tree).

## Choosing a Memtable Type

Cobble provides three memtable implementations, each optimized for different access patterns. You select one via `memtable_type`:

### Hash (default)

Uses a hash table with chaining. Point lookups are O(1) on average, making it the best choice when you need to read recently written data frequently. At flush time, entries are sorted before writing to disk. This is the default and recommended for most workloads.

### Skiplist

Uses a probabilistic balanced tree. Entries are always maintained in sorted order, so iteration and range queries over the memtable are efficient. Point lookups are O(log n). Choose Skiplist if your application frequently scans over recently written data before it has been flushed.

### Vec

Uses an append-only vector with binary search for lookups. Writes are O(1) amortized, and the data structure has minimal memory overhead. Best for sequential, write-only workloads where you rarely read from the memtable.

## Capacity and Buffering

Two parameters control the memtable's memory behavior:

**`memtable_capacity`** (default: 64 MB) determines how much data a single memtable can hold before it is marked immutable and a new one takes over. Larger values mean fewer flushes (and fewer L0 files), but consume more memory.

**`memtable_buffer_count`** (default: 2) controls how many memtable buffers exist in parallel. One is always active for writes; the rest are immutable and waiting to be flushed. If all buffers fill up before the oldest finishes flushing, writes stall. Increasing this value provides more headroom for flush latency spikes, at the cost of additional memory.

## Value Separation During Flush

When [key-value separation](key-value-separation) is enabled, the flush process examines each value. Values larger than `value_separation_threshold` are written to a separate VLOG file instead of being inlined in the SST. The SST stores only a small pointer, which dramatically reduces the amount of data that flows through [compaction](compaction) in subsequent levels.

## Incremental Snapshots

For workloads with small, frequent updates, it can be wasteful to flush the memtable every time you need a [snapshot](snapshot). When `active_memtable_incremental_snapshot_ratio` is set, Cobble can include the active memtable's contents directly in the snapshot, avoiding an unnecessary flush. This is useful for applications that take snapshots more frequently than the memtable naturally fills up.
