---
title: CLI
parent: Tools
nav_order: 1
---

# CLI

`cobble-cli` provides operational commands for compaction workers and the web monitor service.

## Install

```bash
cargo install cobble-cli
```

Or run directly from this repository:

```bash
cargo run -p cobble-cli -- --help
```

## Commands

| Command | Description | Required args |
|---------|-------------|---------------|
| `remote-compactor` | Start a remote compaction server process | none |
| `compact` | Run a multi-DB shared-storage dedicated compactor | `--config <path> <directory>...` |
| `web-monitor` | Start the monitor HTTP server/UI | `--config <path>` |

### `remote-compactor`

```bash
cobble-cli remote-compactor --config ./config.yaml --bind 127.0.0.1:18888
```

- `--config <path>`: optional Cobble config file path (if omitted, uses in-process defaults).
- `--bind <host:port>` / `--address <host:port>`: optional listen address.

### `compact`

```bash
cobble-cli compact \
  --config ./config.yaml \
  --workers 4 \
  /var/lib/cobble/orders \
  /var/lib/cobble/customers/shard-0
```

- `--config <path>`: writer-compatible Cobble configuration with access to the same metadata and
  data volumes.
- Each positional directory can be either a DB directory or a parent whose immediate child
  directories are DBs. Repeat directories as needed; canonical duplicates are ignored.
- `--workers <n>`: maximum number of DB shards compacted concurrently. Defaults to
  `compaction_threads`.
- `--scan-interval <ms>`: directory scan and result/observation poll interval. Defaults to
  `compaction_dedicated_poll_interval_ms`.

The process derives each `db_id` from the DB directory name; no DB ID argument is required. One
scanner validates manifests and referenced files, then dispatches eligible shards to the worker
pool. A shard has at most one in-flight compaction.

The process can start before its writers. It keeps scanning unavailable paths and parent
directories. With the default `runtime_manifest_mode: auto`, a discovered shard waits for its
writer's first runtime manifest.

### `web-monitor`

```bash
cobble-cli web-monitor --config ./config.yaml --bind 127.0.0.1:8080
```

- `--config <path>`: required; monitor loads Cobble volumes and reader settings from this file.
- `--bind <host:port>` / `--address <host:port>`: optional listen address (default `127.0.0.1:0`).
