# cobble-web-monitor

`cobble-web-monitor` provides an embedded HTTP monitor server for Cobble DB instances.
Cobble is a embedded versioned key-value store with multi-layer architecture, and the web monitor allows you to inspect snapshots and keys through a browser UI.
This crate provides a lib for starting the monitor server from Rust code, and also a CLI entry point via [`cobble-cli`](https://crates.io/crates/cobble-cli).

## Storage backend features

`cobble-web-monitor` re-exposes `storage-*` features and forwards them to `cobble`.
For the full feature list and usage examples, refer to `cobble`:
https://crates.io/crates/cobble

## Frontend SPA

A Vue 3 + Vite + Tailwind single-page app is located at `cobble-web-monitor/web-ui`.

- Main pages:
  - Snapshots: list snapshots and switch tracking mode (`current` or specific snapshot).
  - Inspect:
    - Left sidebar navigation with nested `lookup` and `scan`.
    - Scan supports empty prefix to list all keys.
    - Scan row menu (`...`) supports copy key (utf8/base64) and add key to lookup cache.
    - Lookup supports per-row bucket + key input, row-level copy key, and column copy (utf8/base64).
    - Local cache persists scan bucket/prefix/result and lookup rows.
    - Optional auto refresh intervals: `5s`, `10s`, `30s`, `1min`, `5min`, `10min`.

### Build frontend assets

The Rust server serves static files from `cobble-web-monitor/web-ui/dist`.

```bash
cd cobble-web-monitor/web-ui
npm install
npm run build
```

Then start monitor from CLI:

```bash
cargo run -p cobble-cli -- web-monitor --config <config-path> --bind 127.0.0.1:8080
```

Open `http://127.0.0.1:8080` in browser.

If dist assets are missing, a fallback page is shown with build instructions.

## Cargo-integrated frontend build

`cobble-web-monitor` uses `build.rs` to build the Vue app automatically during Cargo build/run.

- Default behavior: run `npm install` + `npm run build` in `web-ui/`.
- Override npm binary: set `NPM=/path/to/npm`.
- Backend-only build (skip Node/npm): set `COBBLE_WEB_MONITOR_SKIP_UI_BUILD=1` (a fallback HTML is embedded).

So both of these will auto-build frontend assets:

```bash
cargo run -p cobble-cli -- web-monitor --config ./cobble-monitor-test-config.json --bind 127.0.0.1:8080
```

or:

```bash
cargo run --bin cobble-cli -- web-monitor --config ./cobble-monitor-test-config.json --bind 127.0.0.1:8080
```

## Docs

- Project docs: https://cobble-project.github.io/cobble/latest/
- Web monitor docs: https://cobble-project.github.io/cobble/latest/tools/web-monitor.html
- Repository: https://github.com/cobble-project/cobble
