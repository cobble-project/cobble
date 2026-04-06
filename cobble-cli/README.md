# cobble-cli

`cobble-cli` contains command-line tools for operating Cobble services.

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
- Repository: https://github.com/cobble-project/cobble
