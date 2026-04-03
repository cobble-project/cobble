---
title: File Management
parent: Architecture
nav_order: 7
---

# File Management

Cobble manages potentially thousands of data files across multiple storage backends. The file management layer handles where files are placed, how long they live, and what happens when storage fills up.

## File Lifecycle

Every data file in Cobble — whether SST, Parquet, or VLOG — is reference-counted. A file stays alive as long as something needs it: the current [LSM tree](lsm-tree) version, an active [snapshot](snapshot), or an in-progress [compaction](compaction). When the last reference is released, the file is automatically cleaned up.

This reference-counting model is essential for safe concurrent operation. [Compaction](compaction) can replace files while readers are still using the old versions. Snapshots can hold references to files long after they've been replaced in the live LSM tree. No explicit locking is needed — the lifecycle is handled automatically.

## Metadata Files

Metadata files (snapshot manifests, schema files) follow a different lifecycle. They are created as persistent files that survive database restarts. They are only removed through explicit cleanup — for example, when a snapshot is expired via `snapshot_retention`, its manifest is deleted along with any exclusively-referenced data files.

## Storage Backends

Cobble abstracts file I/O behind a filesystem interface, supporting multiple backends:

| Backend            | URL Scheme | Best for |
|--------------------|-----------|----------|
| **POSIX**          | `file://` | Local SSDs and HDDs — lowest latency, most common |
| **Apache OpenDAL** | `s3://`, `oss://`, etc. | Cloud object storage — elastic capacity, shared access |

This abstraction means the same `Db` instance can read files from local disk and cloud storage simultaneously — for example, hot data on a local SSD and cold data offloaded to S3.

For details on configuring multiple volumes with different backends, see [Multi-Volume Storage](multi-volume).

## Reader Cache

The file manager maintains an LRU cache of open file readers (capacity: 512). This avoids repeatedly opening and closing files for sequential reads, which is especially important during point lookups.

## Volume Selection

When a new file is created (memtable flush or compaction output), the file manager selects a volume based on the file's usage kind and the volume's available capacity. Higher-priority volumes are preferred when multiple candidates exist. See [Multi-Volume Storage](multi-volume) for details on how volume selection and offloading work.
