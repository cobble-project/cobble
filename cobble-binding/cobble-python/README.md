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
structured `get_into`, `multi_get_into`, `next_batch_into`, and priority-queue `*into`
methods return a `BufferResult`. `BufferStatus.BUFFER_TOO_SMALL` reports the required
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

`load_shard_snapshot_metadata(config, db_id, manifest_path)` and
`load_global_snapshot_metadata(config, manifest_path)` read only the snapshot
metadata files. Each has a `_file` variant for a configuration path. A loaded
`GlobalSnapshot` can be passed to `Reader.open_from_global_snapshot` or
`StructuredReader.open_from_global_snapshot` to open a fixed view without
reloading its global manifest.
`SingleDb` and `StructuredSingleDb` return snapshot objects from
`list_snapshots()` and IDs from `list_snapshot_ids()`.

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
maturin develop --manifest-path cobble-binding/cobble-python/Cargo.toml
pytest cobble-binding/cobble-python/tests
```

## Publishing to PyPI

Publishing a GitHub Release runs `python-package.yml`: it builds and smoke-tests
the platform wheels, builds the source distribution, then uploads them to PyPI
as `pycobble` and attaches them to the GitHub Release. The release tag must match
the workspace version (for example, `v0.5.0`). Manual workflow runs only build
artifacts and do not publish. Pre-release GitHub Releases also trigger publishing;
use a pre-release package version and matching tag for those.

One-time setup (no repository secrets or custom variables are required):

1. Create a GitHub Actions environment named `pypi`. Optionally require approval
   and restrict it to release tags.
2. In PyPI, configure a **Trusted Publisher** for `pycobble`:
   - Owner: `cobble-project`
   - Repository: `cobble`
   - Workflow: `python-package.yml`
   - Environment: `pypi`
3. If the PyPI project does not exist yet, use PyPI's pending publisher setup
   with project name `pycobble` before the first release.

GitHub supplies the short-lived publishing credentials automatically. Re-running
the PyPI job skips files already uploaded; changed packages need a new version.
See [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
for setup instructions.
