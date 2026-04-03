---
title: LSM Tree
parent: Architecture
nav_order: 1
---

# LSM Tree

The Log-Structured Merge Tree is the core data structure in Cobble. It provides high write throughput by buffering writes in memory and flushing them to sorted, immutable files on disk. Over time, background [compaction](compaction) merges these files to keep read performance high.

## Level Structure

Cobble organizes data files into numbered levels, where each level has different properties:

**L0 (tiered)** receives sorted runs directly from [memtable](memtable) flushes. Files in L0 may have overlapping key ranges because each flush produces an independent sorted run. This makes writes fast, but means point lookups must check all L0 files. Compaction is triggered when L0 accumulates more files than `l0_file_limit` (default: 4).

**L1 through Ln (leveled)** are strictly sorted — files within each level have non-overlapping key ranges. This means a point lookup needs to check at most one file per level (found via binary search). Each level has a target size that grows exponentially: L1 targets `l1_base_bytes` (default: 64 MB), and each subsequent level is `level_size_multiplier` times larger (default: 10×). With default settings, L2 targets 640 MB, L3 targets 6.4 GB, and so on up to `max_level` (default: 6).

This design — tiered L0 for fast writes, leveled L1+ for efficient reads — is a deliberate trade-off. L0 absorbs bursts quickly, while deeper levels provide the sorted structure that makes reads predictable.

## File Formats

Cobble supports two output formats, selectable via `data_file_type`:

**SSTable (default)** is a row-based block format. It supports optional bloom filters for fast negative lookups (`sst_bloom_filter_enabled`), two-level partitioned indexes for large files (`sst_partitioned_index`), and per-level compression (`sst_compression_by_level`, default: no compression for L0–L1, LZ4 for L2+). SSTable is the best choice for general-purpose workloads with a mix of point lookups and range scans.

**Apache Parquet** is a columnar storage format. It provides better compression ratios and supports efficient column projection — if you only need a subset of columns, Cobble skips reading the rest entirely. Parquet is ideal for analytical workloads with large scans over wide rows, where you typically read only a few columns at a time. Row group size is configurable via `parquet_row_group_size_bytes` (default: 256 KB).

## Multi-LSM Support

A single `Db` instance can host multiple LSM trees, each responsible for a different bucket range. This enables horizontal scaling within a single process: as data volume grows, Cobble can split a tree into multiple trees at a configured level (`lsm_split_trigger_level`), each handling a subset of the bucket key space. Each tree compacts independently, allowing parallel background work.

## Version Management

LSM tree state is managed through immutable versions. Every [compaction](compaction) or flush produces a version edit describing which files are added and removed. The edit is applied atomically to produce a new version, while the old version remains available for any in-progress reads or [snapshots](snapshot). This copy-on-write approach is what enables Cobble's [lock-free reads](read-write-path#lock-free-reads).
