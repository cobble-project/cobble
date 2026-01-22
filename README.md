# Cobble: A high-performance, embedded key-value store designed for use in distributed systems and standalone applications

Cobble is a Rust-based key-value storage engine that implements an LSM (Log-Structured Merge) tree architecture using multiple file formats (SST, parquet, etc.).
It fulfills all your expectations for embedded LSM storage and can be integrated into various distributed systems and standalone applications as the underlying storage.

## Features

We list some of Cobble's key features below, they are either implemented or are planned for future releases:

- **Hybrid Media**: Local disk and remote object storage (S3, OSS, etc.) can be used individually or together; supports multi-volume distributed I/O scheduling.
- **Schema Support & Evolution**: User-defined column schemas with incremental evolution.
- **Multiple File Formats**: SST and Parquet for both point lookup and analytical queries.
- **One writer, multiple readers**: A single writer for consistency, with concurrent readers across processes or machines.
- **Writer/Reader Upgrade/Downgrade**: Writers can downgrade to readers; readers can upgrade to writers when no other writers/readers exist, with readers unaffected during transitions.
- **Remote Compaction**: Compaction can run on remote object storage to reduce local resource usage.
- **Multi-version Snapshots**: Read historical data states via versioned snapshots.
- **Key-value Separation**: Separates keys and values to optimize large-value, low-access patterns.
- **Time-to-live (TTL)**: Expire and clean up data automatically.
- **Hot/Cold Separation**: Optimize storage and access efficiency with multiple strategies.
- **High performance**: Built on Rust, optimized for low latency and high throughput workloads.
- **Multi-language Bindings**: Planned support for C, C++, Python, Go, and Java bindings.

## Getting Started

TODO

## Contributing

We welcome contributions from the community! Please refer to the [CONTRIBUTING.md](CONTRIBUTING.md) file for guidelines on how to contribute to the project.

## License

This project is licensed under the Apache-2.0 License. See the [LICENSE](LICENSE) file for details.

## Maintainers

- [Zakelly](https://github.com/zakelly) - Project Founder & Main Developer

