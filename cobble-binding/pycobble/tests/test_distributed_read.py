from __future__ import annotations

import gc
import json
from pathlib import Path

import pycobble
import pytest


def config(root: Path) -> str:
    return json.dumps(
        {
            "volumes": [
                {
                    "base_dir": root.as_uri(),
                    "kinds": ["meta", "primary_data_priority_high", "snapshot"],
                }
            ],
            "num_columns": 1,
            "total_buckets": 4,
            "block_cache_size": 0,
            "wal_enabled": False,
            "reader": {
                "block_cache_size": 0,
                "pin_partition_in_memory_count": 2,
                "reload_tolerance_seconds": 60,
            },
        }
    )


def collect(cursor: pycobble.ScanCursor) -> list[tuple[int, bytes, bytes]]:
    rows: list[tuple[int, bytes, bytes]] = []
    while True:
        batch = cursor.next(2)
        for index in range(len(batch)):
            row = batch.row(index)
            rows.append((row.bucket, bytes(row.key), bytes(row.column(0))))
        if batch.end:
            break
    cursor.close()
    return rows


def reconstruct_snapshot(value: pycobble.ShardSnapshot) -> pycobble.ShardSnapshot:
    assert value.schema_id is not None
    assert value.schema_column_families is not None
    return pycobble.ShardSnapshot(
        value.ranges,
        value.column_families,
        value.db_id,
        value.snapshot_id,
        value.manifest_path,
        value.timestamp_seconds,
        value.data_size_bytes,
        value.incremental_data_size_bytes,
        value.schema_id,
        [
            pycobble.SnapshotColumnFamily(
                family.name,
                family.id,
                family.num_columns,
                family.value_has_ttl,
                family.metadata_json,
            )
            for family in value.schema_column_families
        ],
    )


def test_coordinator_reader_read_only_and_distributed_scan(tmp_path: Path) -> None:
    cfg = config(tmp_path / "distributed")
    left = pycobble.Db.open(cfg, [pycobble.BucketRange(0, 1)])
    right = pycobble.Db.open(cfg, [pycobble.BucketRange(2, 3)])
    for bucket, owner in ((0, left), (1, left), (2, right), (3, right)):
        owner.put(bucket, b"alpha", 0, f"old-{bucket}-a".encode())
        owner.put(bucket, b"beta", 0, f"old-{bucket}-b".encode())
    left_snapshot = left.take_snapshot()
    right_snapshot = right.take_snapshot()
    assert left.retain_snapshot(left_snapshot.snapshot_id)
    assert right.retain_snapshot(right_snapshot.snapshot_id)

    coordinator = pycobble.DbCoordinator.open(cfg)
    with pytest.raises(pycobble.InputError):
        coordinator.materialize_global_snapshot(4, 99, [left_snapshot])
    first = coordinator.materialize_global_snapshot(
        4, 100, [reconstruct_snapshot(left_snapshot), reconstruct_snapshot(right_snapshot)]
    )
    assert all(shard.schema_id is None for shard in first.shards)
    with pytest.raises(pycobble.InputError, match="schema metadata is required"):
        coordinator.materialize_global_snapshot(4, 99, first.shards)
    assert coordinator.get_global_snapshot(100).id == 100
    assert coordinator.load_current_global_snapshot().id == 100

    read_only = pycobble.ReadOnlyDb.open(
        cfg, left_snapshot.snapshot_id, left_snapshot.db_id
    )
    assert bytes(read_only.get(0, b"alpha").column(0)) == b"old-0-a"
    rows = read_only.multi_get([(1, b"beta"), (0, b"missing")])
    assert bytes(rows.row(0).column(0)) == b"old-1-b"
    assert not rows.row(1)
    assert read_only.current_schema().column_families
    assert read_only.metrics()
    orphan_cursor = read_only.scan(0)
    del read_only
    gc.collect()
    assert collect(orphan_cursor)[0][1] == b"alpha"

    pinned = pycobble.Reader.open(cfg, first.id)
    current = pycobble.Reader.open_current(cfg)
    assert pinned.mode == pycobble.ReaderMode.Snapshot
    assert pinned.configured_snapshot_id == first.id
    assert current.mode == pycobble.ReaderMode.Current
    assert current.configured_snapshot_id is None
    assert bytes(pinned.get(2, b"alpha").column(0)) == b"old-2-a"
    routed = pinned.multi_get([(0, b"alpha"), (3, b"beta"), (1, b"missing")])
    assert bytes(routed.row(0).column(0)) == b"old-0-a"
    assert bytes(routed.row(1).column(0)) == b"old-3-b"
    assert not routed.row(2)
    with pytest.raises(pycobble.InternalStateError):
        pinned.refresh()

    plan = pycobble.ScanPlan.from_global_snapshot(first)
    plan.with_start(b"alpha")
    plan.with_end(b"z")
    splits = plan.splits()
    assert len(splits) == 2
    assert pycobble.ScanSplit.from_json(splits[0].to_json()).shard.db_id == splits[0].shard.db_id
    with pytest.raises(pycobble.InputError):
        splits[0].open_scanner(
            cfg, pycobble.ScanOptions(stop_at_block_boundary=True)
        )
    all_rows = [row for split in splits for row in collect(split.open_scanner(cfg))]
    assert [(bucket, key) for bucket, key, _ in all_rows] == [
        (bucket, key)
        for bucket in range(4)
        for key in (b"alpha", b"beta")
    ]

    partition = splits[0].split_after(0, b"alpha")
    before = collect(partition.before.open_scanner(cfg))
    after = collect(partition.after.open_scanner(cfg))
    assert before + after == all_rows[:4]
    assert partition.before.end_at_inclusive.bucket == 0
    assert partition.after.start_after_exclusive.key == b"alpha"

    # Publish a new global snapshot and verify current vs pinned behavior.
    left.put(0, b"alpha", 0, b"new-0-a")
    right.put(2, b"alpha", 0, b"new-2-a")
    left_next = left.take_snapshot()
    right_next = right.take_snapshot()
    second = coordinator.materialize_global_snapshot(4, 101, [left_next, right_next])
    assert second.id == 101
    assert bytes(pinned.get(0, b"alpha").column(0)) == b"old-0-a"
    current.refresh()
    assert current.current_global_snapshot.id == 101
    assert bytes(current.get(0, b"alpha").column(0)) == b"new-0-a"
    assert [snapshot.id for snapshot in current.list_global_snapshots()] == [100, 101]

    assert coordinator.retain_snapshot(100)
    assert coordinator.expire_snapshot(100)
    del pinned
    del current
    gc.collect()
    left.close()
    right.close()
