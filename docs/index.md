---
title: Home
layout: home
nav_order: 1
---

# Cobble Project

**Cobble** is a high-performance LSM-based key-value storage engine designed for both embedded and distributed systems.
It provides a flexible and efficient storage solution for various workloads, from small-scale applications to large distributed services.
Compared with other embedded key-value stores like [RocksDB](https://github.com/facebook/rocksdb), it offers multiple file formats (SSTable and Parquet), distributed storage support, distributed snapshots, remote compaction, and more.
Thus, it really fits the needs of modern distributed systems that require a versatile and scalable storage engine.

> Note: Cobble is still in active development, and all the APIs might be changed without deprecation for early versions. We will try our best to keep the documentation up to date, but please check the code and tests for the latest usage patterns.

## Key Features

- **Multi-volume storage** with local disk and remote object storage (S3, OSS, etc.) support (powered by Apache OpenDAL)
- **LSM Tree storage** optimized for high write throughput and efficient merge reads
- **Key-value separation** for large-value workloads
- **Multi-column schema** with online schema evolution (add/delete columns)
- **Distributed snapshots** with coordinator-based global consistency
- **Distributed scan** for batch processing and analytics across multiple nodes
- **Remote compaction** to offload CPU/IO from writer nodes
- **Pluggable merge operators** for read-modify-write patterns (counters, lists, etc.)
- **Hybrid block cache** with optional memory + disk tiers (powered by Foyer)
- **Multiple file formats**: SSTable and Apache Parquet for different workload needs
- **Multiple readers**: concurrent read access across processes and machines with snapshot isolation
- **Row-level TTL support** with configurable time providers
- **Data structures support** with typed row encoding/decoding and structured APIs
- **Java bindings** for JVM-based applications
- **Web monitoring dashboard** for operational visibility

## Documentation Structure

| Section | Description |
|---------|-------------|
| [**Getting Started**](getting-started/) | Configuration, basic usage, and integration guides |
| [**Architecture**](architecture/) | Internal design, data structures, and algorithms |
| [**Reference**](reference/) | Complete configuration reference and API documentation |
| [**FFI Bindings**](ffi-bindings/) | Using Cobble from other languages (Java, …) |
| [**Tools**](tools/) | Operational tooling: CLI commands and Web Monitor |
