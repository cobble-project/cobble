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
| `compact` | Run a shared-storage dedicated compactor | `--config <path> --db-id <db-id>` |
| `web-monitor` | Start the monitor HTTP server/UI | `--config <path>` |

### `remote-compactor`

```bash
cobble-cli remote-compactor --config ./config.yaml --bind 127.0.0.1:18888
```

- `--config <path>`: optional Cobble config file path (if omitted, uses in-process defaults).
- `--bind <host:port>` / `--address <host:port>`: optional listen address.

### `compact`

```bash
cobble-cli compact --config ./config.yaml --db-id orders-shard-0
```

- `--config <path>`: writer-compatible Cobble configuration with access to the same metadata and
  data volumes.
- `--db-id <db-id>`: the writer database ID.
- `--poll-interval <ms>`: optional result/observation poll interval override.

The process can start before its writer. With the default `runtime_manifest_mode: auto`, it waits
for the writer's first runtime manifest.

### `web-monitor`

```bash
cobble-cli web-monitor --config ./config.yaml --bind 127.0.0.1:8080
```

- `--config <path>`: required; monitor loads Cobble volumes and reader settings from this file.
- `--bind <host:port>` / `--address <host:port>`: optional listen address (default `127.0.0.1:0`).
