from __future__ import annotations

import json
from pathlib import Path

import pycobble
import pytest


def config(root: Path, total_buckets: int = 4) -> str:
    return json.dumps(
        {
            "volumes": [
                {
                    "base_dir": root.as_uri(),
                    "kinds": ["meta", "primary_data_priority_high", "snapshot"],
                },
                {"base_dir": root.as_uri(), "kinds": ["wal"]},
            ],
            "num_columns": 2,
            "total_buckets": total_buckets,
            "memtable_capacity": "8KB",
            "base_file_size": "16KB",
            "block_cache_size": 0,
            "time_provider": "manual",
            "wal_enabled": True,
            "wal_flush_interval_ms": 5,
        }
    )


def test_sharded_ranges_crud_batch_multi_scan_and_owner(tmp_path: Path) -> None:
    cfg = config(tmp_path / "ranges")
    with pytest.raises(pycobble.InputError):
        pycobble.Db.open(cfg, [])
    with pytest.raises(pycobble.InputError):
        pycobble.Db.open(cfg, [pycobble.BucketRange(3, 4)])

    db = pycobble.Db.open(
        cfg,
        [pycobble.BucketRange(0, 1), pycobble.BucketRange(3, 3)],
    )
    assert db.id
    batch = pycobble.WriteBatch()
    for bucket in (0, 1, 3):
        for index in range(12):
            batch.put(bucket, f"key-{index:02d}".encode(), 0, f"v-{bucket}-{index}".encode())
    db.write(batch)
    assert not batch

    rows = db.multi_get([(3, b"key-03"), (0, b"key-03"), (1, b"missing")])
    assert bytes(rows.row(0).column(0)) == b"v-3-3"
    assert bytes(rows.row(1).column(0)) == b"v-0-3"
    assert not rows.row(2)

    cursor = db.scan(1, b"key-02", b"key-08")
    with pytest.raises(pycobble.InternalStateError):
        db.close()
    keys: list[bytes] = []
    while True:
        result = cursor.next(2)
        keys.extend(bytes(result.row(index).key) for index in range(len(result)))
        if result.end:
            break
    assert keys == [f"key-{index:02d}".encode() for index in range(2, 8)]
    cursor.close()
    db.close()


def test_sharded_snapshot_exact_latest_restore_and_switch(tmp_path: Path) -> None:
    cfg = config(tmp_path / "recovery")
    db = pycobble.Db.open(cfg)
    source_id = db.id
    db.put(0, b"stable", 0, b"snapshot")
    snapshot = db.take_snapshot()
    assert snapshot.db_id == source_id
    assert db.get_shard_snapshot(snapshot.snapshot_id).manifest_path == snapshot.manifest_path
    assert db.retain_snapshot(snapshot.snapshot_id)
    db.put(0, b"wal-tail", 0, b"latest")
    db.close()

    exact = pycobble.Db.resume_from_snapshot(cfg, snapshot.snapshot_id, source_id)
    assert exact.get(0, b"stable")
    assert not exact.get(0, b"wal-tail")
    exact.close()

    latest = pycobble.Db.resume(cfg, source_id)
    assert bytes(latest.get(0, b"wal-tail").column(0)) == b"latest"
    latest.close()

    restored = pycobble.Db.restore_new_from_manifest(cfg, snapshot.manifest_path)
    assert restored.id != source_id
    assert bytes(restored.get(0, b"stable").column(0)) == b"snapshot"
    restored.close()

    switch_cfg = config(tmp_path / "switch")
    switched = pycobble.Db.open(switch_cfg)
    switched.put(0, b"old", 0, b"value")
    old = switched.take_snapshot()
    switched.put(0, b"new", 0, b"value")
    switched.take_snapshot()
    cursor = switched.scan(0)
    with pytest.raises(pycobble.InternalStateError):
        switched.switch_to_snapshot(old.snapshot_id)
    cursor.close()
    switched.switch_to_snapshot(old.snapshot_id)
    assert switched.get(0, b"old")
    assert not switched.get(0, b"new")
    switched.close()


def test_sharded_reference_expand_and_shrink(tmp_path: Path) -> None:
    cfg = config(tmp_path / "rescale", total_buckets=4)
    source = pycobble.Db.open(cfg, [pycobble.BucketRange(2, 3)])
    target = pycobble.Db.open(cfg, [pycobble.BucketRange(0, 1)])
    source.put(2, b"moved", 0, b"value")
    source_snapshot = source.take_snapshot()
    assert source.retain_snapshot(source_snapshot.snapshot_id)

    target.expand_bucket(
        source.id,
        source_snapshot=source_snapshot.snapshot_id,
        ranges=[pycobble.BucketRange(2, 3)],
        storage_mode=pycobble.ExpandStorageMode.ReferencePersistent,
    )
    target.wait_for_expand_adoption(1.0)
    assert bytes(target.get(2, b"moved").column(0)) == b"value"
    target.shrink_bucket([pycobble.BucketRange(2, 3)])
    try:
        removed = not target.get(2, b"moved")
    except pycobble.CobbleError:
        removed = True
    assert removed

    source.close()
    target.close()
