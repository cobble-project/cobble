---
title: Monitoring
parent: Reference
nav_order: 3
---

# Monitoring

The `cobble-web-monitor` crate provides an embeddable HTTP server with a web dashboard for inspecting Cobble databases. It is designed for development, debugging, and operational visibility — point it at a database path and browse its contents through a browser or API client.

## When to Use

The monitor is useful when you want to:

- **Browse data visually** — search by key prefix, view column values, and page through results without writing code.
- **Check database health** — see the current [snapshot](../architecture/snapshot) state, shard count, and total buckets at a glance.
- **Debug during development** — verify that writes landed correctly or that a [merge operator](../architecture/merge-operator) produced the expected result.

The monitor opens the database in **read-only mode** and never modifies data. It is safe to run alongside a live writer.

## Setup

Add the crate as a dependency and start the server:

```rust
use cobble_web_monitor::{MonitorConfig, MonitorServer};

let config = MonitorConfig { /* configure as needed */ };
let server = MonitorServer::new(config);
let handle = server.start("0.0.0.0:8080").await?;

// Server runs in background; stop with handle.shutdown().await
```

The monitor reads data through an internal `Reader`, which caches shard snapshots and supports periodic refresh.

## Web Dashboard

The built-in browser UI provides a metadata overview and an interactive inspect tab with key prefix search, column value display, and periodic auto-refresh. The UI is compiled into the binary at build time.

### Skipping the UI Build

For faster development builds that don't need the browser UI:

```bash
COBBLE_WEB_MONITOR_SKIP_UI_BUILD=1 cargo build
```

The API endpoints remain fully functional even when the UI build is skipped.
