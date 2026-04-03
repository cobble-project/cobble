---
title: Remote Compaction
parent: Getting Started
nav_order: 7
---

# Remote Compaction

Remote compaction offloads the CPU and IO cost of LSM compaction from writer nodes to dedicated worker services. This is useful in distributed deployments where writer throughput is critical.

## How It Works

1. When the LSM tree triggers compaction, the writer serializes the compaction task and sends it to the remote server.
2. The remote server executes the compaction (reading input files, merging, writing output files).
3. The result (new file metadata) is sent back to the writer, which applies the version edit to its LSM tree.

Both the writer and the remote compaction server must have access to the same storage volumes.

## Setting Up the Server

```rust
use cobble::{Config, RemoteCompactionServer};

let mut server_config = Config::default();
server_config.volumes = /* same volumes as writers */;

let server = RemoteCompactionServer::new(server_config)?;
server.serve("0.0.0.0:9000")?;
```

### Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compaction_threads` | 4 | Thread pool size for compaction tasks |
| `compaction_server_max_concurrent` | 4 | Max concurrent compaction tasks |
| `compaction_server_max_queued` | 64 | Max queued tasks before rejecting connections |

## Configuring Writers

Point the writer's config to the remote compaction server:

```rust
let mut config = Config::default();
config.compaction_remote_addr = Some("127.0.0.1:9000".to_string());
config.compaction_remote_timeout_ms = 300_000; // 5 minute timeout

let db = Db::open(config, bucket_ranges)?;
```

When `compaction_remote_addr` is set, the local compaction executor is bypassed and all compaction work is delegated to the remote server.

## Structured Mode

If you are using `StructuredDb` with custom merge operators, use `StructuredRemoteCompactionServer` from the `cobble-data-structure` crate. It automatically registers the list merge operator resolver:

```rust
use cobble_data_structure::StructuredRemoteCompactionServer;

let server = StructuredRemoteCompactionServer::new(server_config)?;
server.serve("0.0.0.0:9000")?;
```

## Deployment Notes

- The remote compaction server is **stateless** — it only needs access to the shared storage volumes.
- Multiple compaction servers can run behind a load balancer (each writer connects to one server).
- If the remote server is unavailable, compaction tasks will timeout after `compaction_remote_timeout_ms`.
- Monitor compaction lag on writers to ensure the remote server keeps up with the compaction demand.
