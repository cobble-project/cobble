from __future__ import annotations

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
                },
                {"base_dir": root.as_uri(), "kinds": ["wal"]},
            ],
            "num_columns": 1,
            "total_buckets": 4,
            "block_cache_size": 0,
            "ttl_enabled": True,
            "time_provider": "manual",
            "wal_enabled": True,
            "wal_flush_interval_ms": 5,
        }
    )


def default_family(schema: pycobble.Schema) -> pycobble.ColumnFamily:
    return next(family for family in schema.column_families if family.name == "default")


def test_typed_sync_and_async_snapshots(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "snapshots")) as db:
        db.put(0, b"async", 0, b"one")
        pending = db.start_snapshot()
        first = pending.wait()
        assert first.id == pending.id
        assert first.total_buckets == 4
        assert len(first.shards) == 1
        assert first.shards[0].ranges[0].start_inclusive == 0
        assert first.shards[0].ranges[0].end_inclusive == 3
        with pytest.raises(pycobble.InternalStateError):
            pending.wait()

        db.put(0, b"sync", 0, b"two")
        second = db.take_snapshot()
        assert second.id > first.id
        assert db.get_snapshot(second.id).id == second.id
        assert [snapshot.id for snapshot in db.list_snapshots()] == [
            first.id,
            second.id,
        ]
        assert db.list_snapshot_ids() == [first.id, second.id]
        assert db.retain_snapshot(second.id)
        assert db.expire_snapshot(first.id)


def test_schema_builder_is_owned_consumed_and_projects_defaults(tmp_path: Path) -> None:
    db = pycobble.SingleDb.open(config(tmp_path / "schema"))
    initial = db.current_schema()
    initial_family = default_family(initial)
    assert initial_family.column_count == 1
    assert initial_family.merge_operators[0].id

    db.put(0, b"before", 0, b"old")
    builder = db.update_schema()
    with pytest.raises(pycobble.InternalStateError):
        db.close()

    builder.set_column_operator(0, initial_family.merge_operators[0].id)
    builder.add_column(1, default_value=memoryview(b"new-default"))
    builder.set_column_family_ttl(False)
    evolved = builder.commit()
    assert evolved.version > initial.version
    evolved_family = default_family(evolved)
    assert evolved_family.column_count == 2
    assert not evolved_family.value_has_ttl
    with pytest.raises(pycobble.InternalStateError):
        builder.commit()

    row = db.get(0, b"before")
    assert bytes(row.column(0)) == b"old"
    assert bytes(row.column(1)) == b"new-default"
    db.close()


def test_lifecycle_memtable_and_typed_metrics(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "lifecycle")) as db:
        db.set_time(2_000)
        assert db.now_seconds() == 2_000
        db.put(0, b"metric", 0, b"value")
        db.switch_memtable_type(pycobble.MemtableType.SKIPLIST, flush_current=True)
        db.switch_memtable_type(pycobble.MemtableType.ADAPTIVE)
        assert db.load_readonly_files_to_primary() == 0

        samples = db.metrics()
        assert samples
        assert any(
            label.key == "db_id" and label.value
            for sample in samples
            for label in sample.labels
        )
        assert all(
            isinstance(
                sample.value,
                (
                    pycobble.CounterValue,
                    pycobble.GaugeValue,
                    pycobble.HistogramValue,
                ),
            )
            for sample in samples
        )
