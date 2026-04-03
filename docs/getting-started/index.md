---
title: Getting Started
nav_order: 2
has_children: true
---

# Getting Started

This section guides you through setting up and using Cobble for different use cases — from a single-machine embedded store to a distributed cluster with snapshot-following reads and parallel scans.

Start with [Basic Concepts](basic-concepts) for an overview of the key components, then [Configuration](configuration) to understand how Cobble organizes storage, and finally choose the deployment model that fits your application:

| Guide | When to use |
|-------|-------------|
| [Basic Concepts](basic-concepts) | Core components — Db, Coordinator, Reader, Scanner, snapshots |
| [Configuration](configuration) | Every deployment starts here — volumes, tuning, and storage layout |
| [Single-Machine Embedded DB](single-db) | Simple embedded key-value store on a single machine |
| [Distributed DB](distributed) | Multiple writer shards coordinated by a global snapshot system |
| [Reader & Distributed Scan](reader-and-scan) | Serve snapshot-following reads or run parallel analytical scans across shards |
| [Remote Compaction](remote-compaction) | Offload background compaction to dedicated worker nodes |
| [Structured DB](structured-db) | Type-safe row-oriented wrappers for all of the above |
