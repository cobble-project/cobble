# cobble-cli

`cobble-cli` contains command-line tools for operating Cobble, which is a embedded versioned key-value store with multi-layer architecture. The CLI provides commands for running remote compaction workers and the web monitor service.

## Storage backend features

`cobble-cli` re-exposes `storage-*` features and forwards them to dependent Cobble crates.
For the full feature list and usage examples, refer to `cobble`:
https://crates.io/crates/cobble

## Install

```bash
cargo install cobble-cli
```

## Commands

- `remote-compactor`: run a remote compaction worker
- `web-monitor`: run the Cobble web monitor server

```bash
cobble-cli remote-compactor --config ./config.yaml --bind 127.0.0.1:18888
cobble-cli web-monitor --config ./config.yaml --bind 127.0.0.1:8080
```

## Docs

- Project docs: https://cobble-project.github.io/cobble/latest/
- CLI docs: https://cobble-project.github.io/cobble/latest/tools/cli.html
- Repository: https://github.com/cobble-project/cobble
