from __future__ import annotations

import json
from pathlib import Path

import pycobble
import pytest


def config(root: Path, source_root: Path | None = None) -> str:
    volumes = [
        {
            "base_dir": root.as_uri(),
            "kinds": ["meta", "primary_data_priority_high", "snapshot"],
        }
    ]
    if source_root is not None:
        volumes.append({"base_dir": source_root.as_uri(), "kinds": ["readonly"]})
    return json.dumps(
        {
            "volumes": volumes,
            "num_columns": 1,
            "total_buckets": 4,
            "memtable_capacity": "8KB",
            "base_file_size": "16KB",
            "block_cache_size": 0,
            "wal_enabled": False,
        }
    )


@pytest.mark.parametrize("structured", [False, True])
@pytest.mark.parametrize(
    "mode",
    [
        pycobble.ExpandStorageMode.ADOPT_ASYNC,
        pycobble.ExpandStorageMode.REFERENCE_PERSISTENT,
        pycobble.ExpandStorageMode.REFERENCE_PERSISTENT_WITH_CACHE,
    ],
)
def test_expand_manifest_changed_root(tmp_path: Path, structured: bool, mode) -> None:
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    database = pycobble.StructuredDb if structured else pycobble.Db
    source = database.open(config(old_root), [pycobble.BucketRange(2, 3)])
    if structured:
        source.put_bytes(2, b"moved", 0, b"value")
    else:
        source.put(2, b"moved", 0, b"value")
    source.switch_memtable_type(pycobble.MemtableType.SKIPLIST, flush_current=True)
    if structured:
        source.put_bytes(2, b"active", 0, b"tail")
    else:
        source.put(2, b"active", 0, b"tail")
    snapshot = source.take_snapshot()
    assert source.retain_snapshot(snapshot.snapshot_id)
    target_config = config(new_root, old_root)
    target = database.open(target_config, [pycobble.BucketRange(0, 1)])
    with pytest.raises(pycobble.ConfigurationError):
        target.expand_bucket_from_manifest(
            source.id, snapshot.manifest_path + "?secret=not-persisted"
        )
    with pytest.raises(pycobble.InputError):
        target.expand_bucket_from_manifest(source.id, snapshot.manifest_path, ranges=[])
    ranges = (
        None
        if mode == pycobble.ExpandStorageMode.REFERENCE_PERSISTENT
        else [pycobble.BucketRange(2, 3)]
    )
    target.expand_bucket_from_manifest(
        source.id, snapshot.manifest_path, ranges=ranges, storage_mode=mode
    )
    target.wait_for_expand_adoption(10.0)

    def value(db, key: bytes) -> bytes:
        row = db.get(2, key)
        return bytes(row.bytes(0) if structured else row.column(0))

    assert value(target, b"moved") == b"value"
    assert value(target, b"active") == b"tail"
    imported = target.take_snapshot()
    assert imported.manifest_path.startswith(new_root.as_uri() + "/")
    target_id = target.id
    target.close()
    resumed = database.resume_from_snapshot(target_config, imported.snapshot_id, target_id)
    assert value(resumed, b"moved") == b"value"
    assert value(resumed, b"active") == b"tail"
    resumed.close()
    source.close()
