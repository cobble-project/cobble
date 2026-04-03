---
title: Block Cache
parent: Architecture
nav_order: 9
---

# Block Cache

Cobble's block cache keeps frequently accessed data in memory to avoid repeated disk I/O. This is especially impactful for point lookups, where the same index blocks and data blocks tend to be accessed repeatedly by different queries.

## What Gets Cached

The cache stores blocks from both SST and Parquet files:

- **Data blocks** — the actual key-value data from SST files.
- **Index blocks** — the lookup structures within each SST file.
- **Bloom filter blocks** — for quick "definitely not here" checks (see [LSM Tree](lsm-tree#file-formats)).
- **Parquet column chunks** — column data from Parquet files.

Each block is identified by its file and block position, so there's no duplication across concurrent readers.

## Memory-Only Cache (default)

By default, Cobble uses an in-memory LRU cache sized by `block_cache_size` (default: 64 MB). This is sufficient for many workloads, especially when the working set (the set of keys accessed frequently) fits in cache.

Set `block_cache_size = 0` to disable the cache entirely — useful for benchmarking or pure-write workloads.

## Hybrid Cache (Memory + Disk)

For larger working sets that don't fit entirely in memory, enable the **hybrid cache** by setting `block_cache_hybrid_enabled = true`. This adds a disk-based second tier powered by [foyer](https://github.com/foyer-rs/foyer):

- **Memory tier**: Fast in-process LRU (size = `block_cache_size`).
- **Disk tier**: Local SSD storage for evicted blocks (size = `block_cache_hybrid_disk_size`, defaults to memory size).

The disk tier requires a [volume](multi-volume) with `Cache` usage kind and must be on local storage (`file://` URL). When a block is evicted from the memory tier but still "warm," it's promoted to the disk tier, giving it a second chance before being evicted entirely.

You must configure the cache on a faster storage than your primary data volumes to ensure the disk tier provides a meaningful performance boost. For example, if your primary data is on S3, using a local SSD for the cache tier can significantly reduce latency for frequently accessed blocks.

This is particularly useful when your dataset is much larger than available RAM but exhibits temporal locality — for example, a time-series workload where recent data is queried frequently but older data is still occasionally accessed.

## Getting the Most from the Cache

- **Enable [bloom filters](lsm-tree#file-formats)** (`sst_bloom_filter_enabled = true`) to avoid polluting the cache with blocks from negative lookups. Without bloom filters, a "key not found" still loads index and data blocks into cache.
- **Use [partitioned indexes](lsm-tree#file-formats)** (`sst_partitioned_index = true`) for large files so that only relevant portions of the index are cached, rather than the entire index block.
- **Size the cache** to hold your working set.