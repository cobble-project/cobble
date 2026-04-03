---
title: Read & Write Paths
parent: Architecture
nav_order: 5
---

# Read & Write Paths

Understanding how data flows through Cobble helps you choose the right configuration for your workload and diagnose performance characteristics.

## Writing Data

When you call `put`, `merge`, or `delete`, the operation goes directly into the active [memtable](memtable) — an in-memory buffer. This is why writes in Cobble are fast: they never touch disk synchronously.

Once the memtable fills up (controlled by `memtable_capacity`), it becomes immutable and a fresh memtable takes over. The immutable memtable is flushed to disk in the background as a new L0 file in the [LSM tree](lsm-tree). If [value separation](key-value-separation) is enabled, large values are extracted into a VLOG file during this flush.

If the system can't flush fast enough — for example, because [compaction](compaction) is lagging — a **write stall** kicks in. This temporarily blocks new writes to prevent unbounded memory growth. The `write_stall_limit` parameter controls when this happens. In practice, write stalls are rare with well-tuned compaction, but they are an important safety mechanism.

### Write Batches

When you write multiple columns for the same key, or want to group several operations, use a write batch. The batch consolidates operations per key so that multi-column updates are atomic — the memtable sees a single multi-column value rather than separate per-column writes.

### Automatic Snapshots on Flush

If `snapshot_on_flush` is enabled, Cobble automatically takes a [snapshot](snapshot) after every successful memtable flush. This is useful for applications that want frequent consistent checkpoints without explicit snapshot calls.

## Reading Data

### Point Lookups

A `get` call searches for a key starting from the most recent data and working backward:

1. **Active memtable** — the current in-memory buffer (most recent writes).
2. **Immutable memtable(s)** — recently filled buffers waiting to be flushed.
3. **LSM tree levels** — starting from L0 (most recent on-disk data) through deeper levels.

At each level, Cobble uses the level's structure to find the relevant file efficiently. In L0, where files may overlap, all files are checked. In L1 and deeper, files have non-overlapping key ranges, so a binary search locates the single relevant file.

Within each file, [bloom filters](lsm-tree#file-formats) (if enabled) provide a quick "definitely not here" check that avoids unnecessary I/O. The [block cache](block-cache) keeps frequently accessed data and index blocks in memory to speed up repeated access patterns.

If the value is stored in a VLOG file (see [key-value separation](key-value-separation)), the pointer is transparently dereferenced. If multiple [merge operands](merge-operator) are found across levels, they are combined using the registered operator to produce the final value.

### Lock-Free Reads

Cobble uses an atomic swap mechanism for its database state, which means readers never block writers. When you call `get`, you acquire an immutable snapshot of the current state — no locks involved. Writers can continue inserting data concurrently. This design is fundamental to achieving high read throughput alongside sustained write loads.

### Scanning

Scans create a composable iterator stack that merges data from all sources (memtable + LSM levels), deduplicates entries, applies [schema evolution](schema-evolution), and optionally projects only requested columns.

For distributed workloads, the [distributed scan](../getting-started/reader-and-scan#distributed-scan) feature lets you partition a scan across multiple workers, each processing a different shard of the data in parallel.

**Column projection** is an important optimization for scans: if you only need a subset of columns, set `column_indices` in `ScanOptions` and Cobble will skip reading unnecessary column data entirely — especially beneficial with the Parquet file format.

**Read-ahead buffering** (`read_ahead_bytes` in `ScanOptions`) pre-fetches data during sequential scans, reducing I/O latency for large scans over cold data.

## Choosing the Right Configuration

| Workload | Recommended settings |
|----------|---------------------|
| Write-heavy (ingestion) | Larger `memtable_capacity`, higher `l0_file_limit`, [remote compaction](../getting-started/remote-compaction) |
| Read-heavy (serving) | Enable `sst_bloom_filter_enabled`, larger `block_cache_size`, keep defaults for compaction |
| Large values | Enable [value separation](key-value-separation) with appropriate `value_separation_threshold` |
| Analytical scans | Use Parquet `data_file_type`, enable column projection, set `read_ahead_bytes` |
