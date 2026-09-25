# pycobble

`pycobble` is the official high-performance Python binding for Cobble.

It requires CPython 3.11 or newer and ships a stable-ABI native wheel. Install it
with:

```bash
pip install pycobble
```

Applications should import public names from `pycobble`, not
`pycobble._native`.

## Raw key/value API

```python
import json
import pycobble

config = json.dumps({
    "volumes": [{
        "base_dir": "file:///tmp/example-cobble",
        "kinds": ["meta", "primary_data_priority_high", "snapshot"],
    }],
    "num_columns": 2,
    "total_buckets": 16,
})

with pycobble.SingleDb.open(config) as db:
    db.put(0, b"key", 0, b"value")
    row = db.get(0, b"key")
    assert bytes(row.column(0)) == b"value"
```

`OwnedBytes` implements Python's read-only buffer protocol, so `memoryview` can
read a Rust-owned result without copying its payload:

```python
value = row.column(0)
view = value.view()
assert view.readonly
```

For reusable caller-owned memory, `get_column_into`, `next_batch_into`, and the
structured `get_into`, `multi_get_into`, `next_into`, and priority-queue `*into`
methods return a `BufferResult`. A `BufferTooSmall` result reports the required
size and leaves the output unchanged, so the same operation can be retried.

## Structured API

```python
db = pycobble.StructuredSingleDb.open(config)
schema = db.update_schema()
schema.add_list_column(1, pycobble.ListConfig(max_elements=8))
schema.commit()

batch = pycobble.StructuredWriteBatch()
batch.put_bytes(0, b"row", 0, b"payload")
batch.put_list(0, b"row", 1, [b"a", b"b"])
db.write(batch)

row = db.get(0, b"row")
assert bytes(row.bytes(0)) == b"payload"
assert bytes(row.list_element(1, 0)) == b"a"
db.close()
```

For snapshot reads, `StructuredReader.open_current(config)` follows the latest
global snapshot on access and also supports explicit `refresh()`, while
`StructuredReader.open(config, id)` stays fixed.
`StructuredReadOnlyDb.open(config, shard_snapshot_id, db_id)`
reads one shard snapshot. Both return the same typed rows and scans as the
writable structured databases, including caller-owned CSRB buffer methods.

## API surface

The binding includes:

- raw `SingleDb` and sharded `Db` CRUD, batches, multi-get, scans, schemas,
  snapshots, recovery, metrics, lifecycle operations, and rescaling;
- `Reader`, `ReadOnlyDb`, `DbCoordinator`, and typed distributed scan plans;
- structured BYTES/LIST rows, batches, scans, schema evolution, snapshots,
  recovery, rescaling, snapshot readers, typed distributed scans, and priority queues;
- typed errors and complete `.pyi` declarations.

All database calls are synchronous. Blocking storage work releases the Python
GIL where the underlying cursor or borrowed Python buffer does not require the
originating thread. Database handles and cursors should be externally
synchronized rather than used concurrently from multiple Python threads.

## Build from source

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install maturin pytest
maturin develop --manifest-path cobble-binding/pycobble/Cargo.toml
pytest cobble-binding/pycobble/tests
```
