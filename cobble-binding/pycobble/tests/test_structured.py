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
        }
    )


def test_structured_bytes_lists_schema_scan_and_priority_queue(tmp_path: Path) -> None:
    db = pycobble.StructuredSingleDb.open(config(tmp_path / "single"))
    builder = db.update_schema()
    with pytest.raises(pycobble.InternalStateError):
        db.close()
    builder.add_list_column(
        1,
        pycobble.ListConfig(
            max_elements=3,
            retain_mode=pycobble.ListRetainMode.Last,
        ),
    )
    evolved = builder.commit()
    family = next(family for family in evolved.families if family.name == "default")
    assert family.columns[0].index == 1
    assert family.columns[0].kind == pycobble.StructuredColumnKind.List
    with pytest.raises(pycobble.InternalStateError):
        builder.commit()

    db.put_bytes(0, b"row", 0, b"bytes")
    db.put_list(0, b"row", 1, [b"a", bytearray(b"b")])
    db.merge_list(0, b"row", 1, [memoryview(b"c"), b"d"])
    row = db.get(0, b"row")
    assert row.kind(0) == pycobble.StructuredColumnKind.Bytes
    assert bytes(row.bytes(0)) == b"bytes"
    assert [bytes(row.list_element(1, index)) for index in range(row.list_size(1))] == [
        b"b",
        b"c",
        b"d",
    ]

    cursor = db.scan(0)
    with pytest.raises(pycobble.InternalStateError):
        db.close()
    batch = cursor.next(10)
    assert len(batch) == 1
    assert bytes(batch.row(0).key) == b"row"
    assert bytes(batch.row(0).value.bytes(0)) == b"bytes"
    cursor.close()

    queue = db.new_priority_queue("timers")
    assert queue.column_family
    queue.offer(0, b"002", b"two")
    queue.offer(0, b"001", b"one")
    queue.offer(0, b"003", b"three")
    assert bytes(queue.peek(0).key) == b"001"
    first = queue.poll(0)
    assert bytes(first.value) == b"one"
    remaining = queue.poll_batch(0, 2)
    assert [bytes(remaining.entry(index).key) for index in range(len(remaining))] == [
        b"002",
        b"003",
    ]
    assert queue.peek(0) is None
    assert queue.cursor(0) == b"003"
    with pytest.raises(pycobble.InternalStateError):
        db.close()
    del queue
    gc.collect()

    snapshot = db.take_snapshot()
    assert snapshot.total_buckets == 4
    db.close()


def test_structured_sharded_db_typed_snapshot(tmp_path: Path) -> None:
    db = pycobble.StructuredDb.open(
        config(tmp_path / "sharded"), [pycobble.BucketRange(1, 2)]
    )
    builder = db.update_schema()
    builder.add_list_column(1, pycobble.ListConfig())
    builder.commit()
    db.put_bytes(1, b"key", 0, b"value")
    db.put_list(1, b"key", 1, [b"x", b"y"])
    snapshot = db.take_snapshot()
    assert snapshot.db_id == db.id
    assert snapshot.ranges[0].start_inclusive == 1
    assert snapshot.ranges[0].end_inclusive == 2
    db.close()


@pytest.mark.parametrize("sharded", [False, True])
def test_structured_batch_and_multi_get(tmp_path: Path, sharded: bool) -> None:
    root = tmp_path / ("sharded-batch" if sharded else "single-batch")
    if sharded:
        db = pycobble.StructuredDb.open(
            config(root), [pycobble.BucketRange(0, 3)]
        )
    else:
        db = pycobble.StructuredSingleDb.open(config(root))

    builder = db.update_schema()
    builder.add_list_column(1, pycobble.ListConfig())
    builder.commit()

    mutable_key = bytearray(b"alpha")
    mutable_value = bytearray(b"value-a")
    mutable_element = bytearray(b"list-a")
    batch = pycobble.StructuredWriteBatch()
    batch.put_bytes(0, mutable_key, 0, mutable_value)
    batch.put_list(0, mutable_key, 1, [mutable_element, b"list-b"])
    batch.put_bytes(3, b"omega", 0, b"value-z")
    mutable_key[:] = b"xxxxx"
    mutable_value[:] = b"changed"
    mutable_element[:] = b"xxxxxx"

    assert len(batch) == 3
    db.write(batch)
    assert not batch

    result = db.multi_get(
        [(0, b"alpha"), (0, b"missing"), (3, b"omega"), (0, b"alpha")]
    )
    assert len(result) == 4
    assert bytes(result.row(0).bytes(0)) == b"value-a"
    assert not result.row(1)
    assert bytes(result.row(2).bytes(0)) == b"value-z"
    assert bytes(result.row(3).list_element(1, 0)) == b"list-a"

    batch.merge_bytes(0, b"alpha", 0, b"-tail")
    db.write(batch)
    assert not batch
    assert bytes(db.get(0, b"alpha").bytes(0)) == b"value-a-tail"

    invalid = pycobble.StructuredWriteBatch()
    invalid.put_bytes(1, b"atomic", 0, b"must-not-land")
    invalid.put_list(1, b"atomic", 0, [b"wrong-column-kind"])
    with pytest.raises(pycobble.CobbleError):
        db.write(invalid)
    assert len(invalid) == 2
    assert not db.get(1, b"atomic")
    invalid.clear()
    assert not invalid
    db.close()
