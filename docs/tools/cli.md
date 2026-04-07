---
title: CLI
parent: Tools
nav_order: 1
---

# CLI

`cobble-cli` provides operational commands for running remote compaction workers and the web monitor service.

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
| `web-monitor` | Start the monitor HTTP server/UI | `--config <path>` |

### `remote-compactor`

```bash
cobble-cli remote-compactor --config ./config.yaml --bind 127.0.0.1:18888
```

- `--config <path>`: optional Cobble config file path (if omitted, uses in-process defaults).
- `--bind <host:port>` / `--address <host:port>`: optional listen address.

### `web-monitor`

```bash
cobble-cli web-monitor --config ./config.yaml --bind 127.0.0.1:8080
```

- `--config <path>`: required; monitor loads Cobble volumes and reader settings from this file.
- `--bind <host:port>` / `--address <host:port>`: optional listen address (default `127.0.0.1:0`).
