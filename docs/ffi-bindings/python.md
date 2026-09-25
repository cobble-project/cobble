---
title: Python
parent: FFI Bindings
nav_order: 3
---

# Python Bindings

`pycobble` provides Python APIs for embedded databases, sharded writers,
snapshot readers, and distributed scans. Use raw byte columns or structured
BYTES and LIST columns. Table/Catalog APIs and custom schema transforms are
not currently exposed in Python.

## Installation

Requires CPython 3.11 or newer:

```bash
pip install pycobble
```

Import public APIs from `pycobble`.

## Basic reads and writes

Use `SingleDb` for an embedded database. `open` accepts JSON configuration;
`open_file` accepts a configuration file path.

```python
import json
import pycobble

config = json.dumps({
    "volumes": [{
        "base_dir": "file:///tmp/cobble-python-example",
        "kinds": ["meta", "primary_data_priority_high", "snapshot"],
    }],
    "num_columns": 1,
    "total_buckets": 1,
})

with pycobble.SingleDb.open(config) as db:
    db.put(0, b"user:1", 0, b"Alice")
    row = db.get(0, b"user:1")
    if row:
        print(bytes(row.column(0)))

    snapshot = db.take_snapshot()
    db.retain_snapshot(snapshot.id)
```

The first argument to `put` and `get` is the bucket ID; the second `0` in
`put` selects the value column. Keys and values are bytes. A missing key
returns a row that evaluates to `False`.

Use `multi_get([(bucket, key), ...])` to read several keys, and `WriteBatch`
with `db.write(batch)` to batch writes. `scan` returns a cursor that reads
rows in batches; close it when finished:

```python
reader = pycobble.Reader.open(config, snapshot.id)
cursor = reader.scan(0, b"user:", b"user;")
try:
    while True:
        batch = cursor.next(128)
        for index in range(len(batch)):
            row = batch.row(index)
            print(bytes(row.key), bytes(row.column(0)))
        if batch.end:
            break
finally:
    cursor.close()
```

## Snapshots and readers

`take_snapshot()` waits for the snapshot to finish. Use
`SingleDb.resume(config, snapshot_id)` to reopen a saved global snapshot.
Keep snapshots retained while readers or scans need them.

- `Reader.open_current(config)` follows committed global snapshots on access;
  `refresh()` explicitly checks for a newer snapshot.
- `Reader.open(config, snapshot_id)` stays on a fixed global snapshot.
- `ReadOnlyDb.open(config, shard_snapshot_id, db_id)` reads one shard snapshot.

If you already have a global manifest path, load its metadata and open a reader:

```python
snapshot = pycobble.load_global_snapshot_metadata(config, manifest_path)
reader = pycobble.Reader.open_from_global_snapshot(config, snapshot)
```

`manifest_path` must be an absolute path or URL within a configured metadata
volume. `load_shard_snapshot_metadata(config, db_id, manifest_path)` loads a
shard's snapshot and schema metadata. Both loaders have `_file` variants for
configuration files and do not open a database.

## Structured columns

Use `StructuredSingleDb` for BYTES and LIST columns. Run this example against
a separate database from the raw example above:

```python
settings = json.loads(config)
settings["volumes"][0]["base_dir"] = "file:///tmp/cobble-python-structured"
structured_config = json.dumps(settings)

db = pycobble.StructuredSingleDb.open(structured_config)
try:
    schema = db.update_schema()
    schema.add_list_column(1, pycobble.ListConfig(max_elements=8))
    schema.commit()

    db.put_bytes(0, b"user:1", 0, b"Alice")
    db.put_list(0, b"user:1", 1, [b"reader", b"writer"])
    row = db.get(0, b"user:1")
    print(bytes(row.bytes(0)))
    print(bytes(row.list_element(1, 0)))
finally:
    db.close()
```

`StructuredReader` and `StructuredReadOnlyDb` provide the corresponding
snapshot reads, including `multi_get`, scans, and column projection through
`StructuredReadOptions` / `StructuredScanOptions`.

## Choosing an API

| API | Use it for |
|-----|------------|
| `SingleDb` / `StructuredSingleDb` | Embedded reads, writes, and snapshots. |
| `Db` / `StructuredDb` | Writers responsible for assigned bucket ranges. |
| `Reader` / `StructuredReader` | Reading a global snapshot across shards. |
| `ReadOnlyDb` / `StructuredReadOnlyDb` | Reading one fixed shard snapshot. |
| `DbCoordinator` | Combining shard snapshots into a global snapshot. |
| `ScanPlan` / `StructuredScanPlan` | Splitting snapshot scans across workers. |

See [Distributed Deployment](../getting-started/distributed) and
[Reader & Distributed Scan](../getting-started/reader-and-scan) for the workflow.

## Usage notes

- Use `bytes(value)` for a regular Python byte string, or `value.view()` for
  a read-only view of a returned byte value.
- Close database handles and cursors when finished. Do not use the same handle
  concurrently from multiple Python threads without synchronization.
- Catch `pycobble.CobbleError` to handle storage errors.
