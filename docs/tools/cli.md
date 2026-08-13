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
| `compact` | Run a multi-DB shared-storage dedicated compactor | `--config <path> <path-or-url>...` |
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
  /var/lib/cobble \
  s3://state-bucket/cobble
```

- `--config <path>`: writer-compatible Cobble configuration with access to the same metadata and
  data volumes.
- Each positional value can be a DB directory or a local/object-storage prefix containing DBs.
  Discovery is recursive and bounded; repeat paths as needed and duplicates are ignored.
- `--workers <n>`: maximum number of DB shards compacted concurrently. Defaults to
  `compaction_threads`.
- `--scan-interval <ms>`: interval for discovering and checking DBs. Defaults to
  `compaction_dedicated_poll_interval_ms`.

The process derives each `db_id` from the DB directory name; no DB ID argument is required. The
config file must contain a metadata/data volume that is an ancestor of every storage URL being
scanned, including credentials and backend options. The process can start before its writers and
continues checking the configured paths. Only DBs configured for dedicated compaction are handled.

### `web-monitor`

```bash
cobble-cli web-monitor --config ./config.yaml --bind 127.0.0.1:8080
```

- `--config <path>`: required; monitor loads Cobble volumes and reader settings from this file.
- `--bind <host:port>` / `--address <host:port>`: optional listen address (default `127.0.0.1:0`).
