from __future__ import annotations

import gc
import json
import struct
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
            "time_provider": "manual",
            "wal_enabled": False,
        }
    )


def decode_csrb(value: bytearray, length: int) -> list[tuple[int, bytes, bool, list[object]]]:
    data = memoryview(value)[:length]
    assert data[:4] == b"CSRB"
    version, header_size, _flags, row_count, required = struct.unpack_from(
        "<HHIIQ", data, 4
    )
    assert version == 1
    assert header_size == 24
    assert required == length
    offset = header_size
    rows: list[tuple[int, bytes, bool, list[object]]] = []
    for _ in range(row_count):
        bucket, row_flags, key_len, column_count, reserved = struct.unpack_from(
            "<HHIII", data, offset
        )
        assert reserved == 0
        offset += 16
        key = bytes(data[offset : offset + key_len])
        offset += key_len
        columns: list[object] = []
        for _ in range(column_count):
            tag, flags, reserved, element_count, payload_size = struct.unpack_from(
                "<BBHIQ", data, offset
            )
            assert flags == 0 and reserved == 0
            offset += 16
            payload_end = offset + payload_size
            if tag == 0:
                assert element_count == 0 and payload_size == 0
                columns.append(None)
            elif tag == 1:
                assert element_count == 1
                columns.append(bytes(data[offset:payload_end]))
                offset = payload_end
            else:
                assert tag == 2
                elements: list[bytes] = []
                for _ in range(element_count):
                    element_len = struct.unpack_from("<Q", data, offset)[0]
                    offset += 8
                    elements.append(bytes(data[offset : offset + element_len]))
                    offset += element_len
                assert offset == payload_end
                columns.append(elements)
        rows.append((bucket, key, bool(row_flags & 1), columns))
    assert offset == length
    return rows


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
    retained_row = batch.row(0)
    del batch
    gc.collect()
    assert bytes(retained_row.key) == b"row"
    assert bytes(retained_row.value.bytes(0)) == b"bytes"
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


def test_structured_snapshot_recovery_and_lifecycle(tmp_path: Path) -> None:
    config_json = config(tmp_path / "structured-recovery")
    config_path = tmp_path / "structured-config.json"
    config_path.write_text(config_json)

    db = pycobble.StructuredDb.open_file(config_path)
    db_id = db.id
    db.put_bytes(0, b"versioned", 0, b"v1")
    first = db.start_snapshot()
    assert first.id >= 0
    first_snapshot = first.wait()
    with pytest.raises(pycobble.InternalStateError):
        first.wait()
    assert db.get_shard_snapshot(first_snapshot.snapshot_id).db_id == db_id

    db.put_bytes(0, b"versioned", 0, b"v2")
    second_snapshot = db.take_snapshot()
    assert second_snapshot.snapshot_id > first_snapshot.snapshot_id
    assert db.retain_snapshot(first_snapshot.snapshot_id)
    assert db.retain_snapshot(first_snapshot.snapshot_id)
    next_time = db.now_seconds() + 1234
    db.set_time(next_time)
    assert db.now_seconds() == next_time
    db.switch_memtable_type(pycobble.MemtableType.Skiplist)
    assert isinstance(db.load_readonly_files_to_primary(), int)
    assert isinstance(db.metrics(), list)
    db.close()

    exact = pycobble.StructuredDb.resume_from_snapshot_file(
        config_path,
        first_snapshot.snapshot_id,
        db_id,
        pycobble.RecoveryMode.SnapshotOnly,
    )
    assert bytes(exact.get(0, b"versioned").bytes(0)) == b"v1"
    exact.close()

    latest = pycobble.StructuredDb.resume(
        config_json, db_id, pycobble.RecoveryMode.SnapshotOnly
    )
    assert bytes(latest.get(0, b"versioned").bytes(0)) == b"v2"
    cursor = latest.scan(0)
    with pytest.raises(pycobble.InternalStateError):
        latest.switch_to_snapshot(first_snapshot.snapshot_id)
    cursor.close()
    latest.switch_to_snapshot(first_snapshot.snapshot_id)
    assert bytes(latest.get(0, b"versioned").bytes(0)) == b"v1"
    latest.close()

    restored = pycobble.StructuredDb.restore_new(
        config_json, first_snapshot.snapshot_id, db_id
    )
    assert restored.id != db_id
    assert bytes(restored.get(0, b"versioned").bytes(0)) == b"v1"
    restored.close()


def test_structured_single_snapshot_management(tmp_path: Path) -> None:
    with pycobble.StructuredSingleDb.open(config(tmp_path / "single-snapshots")) as db:
        db.put_bytes(0, b"key", 0, b"value")
        pending = db.start_snapshot()
        manifest = pending.wait()
        assert db.get_snapshot(manifest.id).id == manifest.id
        assert [item.id for item in db.list_snapshots()] == [manifest.id]
        assert db.retain_snapshot(manifest.id)
        assert db.retain_snapshot(manifest.id)
        next_time = db.now_seconds() + 4321
        db.set_time(next_time)
        assert db.now_seconds() == next_time
        db.switch_memtable_type(pycobble.MemtableType.Hash)
        assert isinstance(db.load_readonly_files_to_primary(), int)
    db.close()


def test_structured_distributed_scan_plan(tmp_path: Path) -> None:
    config_json = config(tmp_path / "structured-plan")
    config_path = tmp_path / "structured-plan.json"
    config_path.write_text(config_json)
    left = pycobble.StructuredDb.open(
        config_json, [pycobble.BucketRange(0, 1)]
    )
    right = pycobble.StructuredDb.open(
        config_json, [pycobble.BucketRange(2, 3)]
    )
    for db in (left, right):
        builder = db.update_schema()
        builder.add_list_column(1, pycobble.ListConfig())
        builder.commit()
    left.put_bytes(0, b"a", 0, b"left-a")
    left.put_list(1, b"b", 1, [b"left-b"])
    right.put_bytes(2, b"c", 0, b"right-c")
    right.put_list(3, b"d", 1, [b"right-d"])
    left_snapshot = left.take_snapshot()
    right_snapshot = right.take_snapshot()
    left.close()
    right.close()

    coordinator = pycobble.DbCoordinator.open(config_json)
    global_snapshot = coordinator.materialize_global_snapshot(
        4, 77, [left_snapshot, right_snapshot]
    )
    plan = pycobble.StructuredScanPlan.from_global_snapshot(global_snapshot)
    splits = plan.splits()
    assert len(splits) == 2

    rows: list[tuple[int, bytes, bytes | None]] = []
    for split in splits:
        restored = pycobble.StructuredScanSplit.from_json(split.to_json())
        scanner = restored.open_scanner_file(config_path)
        while True:
            batch = scanner.next(2)
            for index in range(len(batch)):
                row = batch.row(index)
                value = row.value
                payload = bytes(value.bytes(0)) if value.has_column(0) else None
                rows.append((row.bucket, bytes(row.key), payload))
            if batch.end:
                break
        scanner.close()

    assert rows == [
        (0, b"a", b"left-a"),
        (1, b"b", None),
        (2, b"c", b"right-c"),
        (3, b"d", None),
    ]

    partition = splits[0].split_after(0, b"a")
    assert partition.before.end_at_inclusive.bucket == 0
    assert partition.before.end_at_inclusive.key == b"a"
    assert partition.after.start_after_exclusive.bucket == 0
    assert partition.after.start_after_exclusive.key == b"a"
    with pytest.raises(pycobble.InputError):
        splits[0].open_scanner(
            config_json,
            pycobble.StructuredScanOptions(stop_at_block_boundary=True),
        )


def test_structured_caller_owned_buffers_and_retry(tmp_path: Path) -> None:
    db = pycobble.StructuredSingleDb.open(config(tmp_path / "structured-csrb"))
    builder = db.update_schema()
    builder.add_list_column(1, pycobble.ListConfig())
    builder.commit()
    db.put_bytes(0, b"a", 0, b"value-a")
    db.put_list(0, b"a", 1, [b"x", b"y"])
    db.put_bytes(0, b"b", 0, b"value-b")

    too_small = bytearray(b"\xa5" * 8)
    before = too_small[:]
    result = db.get_into(0, b"a", too_small)
    assert result.status == pycobble.BufferStatus.BufferTooSmall
    assert result.bytes_written == 0
    assert too_small == before
    output = bytearray(result.bytes_required)
    result = db.get_into(0, b"a", output)
    assert result.status == pycobble.BufferStatus.Ok
    assert decode_csrb(output, result.bytes_written) == [
        (0, b"a", True, [b"value-a", [b"x", b"y"]])
    ]

    keys = [(0, b"a"), (0, b"missing"), (0, b"b"), (0, b"a")]
    probe = bytearray()
    result = db.multi_get_into(keys, probe)
    assert result.status == pycobble.BufferStatus.BufferTooSmall
    output = bytearray(result.bytes_required)
    result = db.multi_get_into(keys, output)
    rows = decode_csrb(output, result.bytes_written)
    assert [row[1] for row in rows] == [b"a", b"missing", b"b", b"a"]
    assert [row[2] for row in rows] == [True, False, True, True]

    cursor = db.scan(0)
    probe = bytearray(b"\x5a" * 12)
    before = probe[:]
    result = cursor.next_into(1, probe)
    assert result.status == pycobble.BufferStatus.BufferTooSmall
    assert probe == before
    output = bytearray(result.bytes_required)
    result = cursor.next_into(1, output)
    assert decode_csrb(output, result.bytes_written)[0][1] == b"a"
    output = bytearray(256)
    result = cursor.next_into(1, output)
    assert decode_csrb(output, result.bytes_written)[0][1] == b"b"
    cursor.close()

    queue = db.new_priority_queue("caller-buffer")
    queue.offer(0, b"001", b"one")
    queue.offer(0, b"002", b"two")
    probe = bytearray(b"\x33" * 4)
    before = probe[:]
    result = queue.poll_into(0, probe)
    assert result.status == pycobble.BufferStatus.BufferTooSmall
    assert probe == before
    with pytest.raises(pycobble.InternalStateError):
        queue.peek(0)
    with pytest.raises(pycobble.InternalStateError):
        queue.peek_into(0, bytearray(result.bytes_required))
    output = bytearray(result.bytes_required)
    result = queue.poll_into(0, output)
    assert decode_csrb(output, result.bytes_written) == [
        (0, b"001", True, [b"one"])
    ]
    assert bytes(queue.peek(0).key) == b"002"
    output = bytearray(256)
    result = queue.poll_batch_into(0, output)
    assert decode_csrb(output, result.bytes_written) == [
        (0, b"002", True, [b"two"])
    ]
    result = queue.peek_batch_into(0, output)
    assert result.status == pycobble.BufferStatus.End
    assert result.row_count == 0
    assert decode_csrb(output, result.bytes_written) == []
    del queue
    gc.collect()
    db.close()
