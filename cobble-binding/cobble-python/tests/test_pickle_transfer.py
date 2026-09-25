from __future__ import annotations

import json
import multiprocessing
import os
import pickle
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pycobble
import pytest


def _config(root: Path) -> str:
    return json.dumps(
        {
            "volumes": [
                {
                    "base_dir": root.as_uri(),
                    "kinds": ["meta", "primary_data_priority_high", "snapshot"],
                }
            ],
            "num_columns": 2,
            "total_buckets": 4,
            "block_cache_size": 0,
            "wal_enabled": False,
        }
    )


def _roundtrip(value):
    return pickle.loads(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))


ROWS_PER_BUCKET = 512
SCAN_FIRST = 3
SCAN_LAST = ROWS_PER_BUCKET - 4
BATCH_ROWS = 31


def _key(index: int) -> bytes:
    return f"k-{index:04d}".encode()


def _primary(bucket: int, index: int) -> bytes:
    prefix = f"value:{bucket}:{index}:".encode()
    if (bucket, index) == (2, 256):
        return prefix + b"L" * 70_000
    return prefix + bytes([65 + index % 26]) * (index % 41)


def _secondary(bucket: int, index: int) -> bytes:
    return f"meta:{bucket}:{index}".encode()


def _snapshot_families(shard) -> list[tuple[str, int, int, bool, str | None]]:
    return [
        (
            family.name,
            family.id,
            family.num_columns,
            family.value_has_ttl,
            family.metadata_json,
        )
        for family in shard.schema_column_families
    ]


def _writer_batch(start_bucket: int, structured: bool, options):
    batch = pycobble.StructuredWriteBatch() if structured else pycobble.WriteBatch()
    for bucket in (start_bucket, start_bucket + 1):
        for index in range(ROWS_PER_BUCKET):
            key = _key(index)
            if structured:
                batch.put_bytes(bucket, key, 0, _primary(bucket, index), options)
                batch.put_list(
                    bucket,
                    key,
                    1,
                    [_secondary(bucket, index), b"tail:" + key],
                    options,
                )
            else:
                batch.put(bucket, key, 0, _primary(bucket, index), options)
                batch.put(bucket, key, 1, _secondary(bucket, index), options)
    return batch


def _snapshot_worker(config_json: str, structured: bool, start_bucket: int, batch, options):
    cls = pycobble.StructuredDb if structured else pycobble.Db
    db = cls.open(
        config_json, [pycobble.BucketRange(start_bucket, start_bucket + 1)]
    )
    if structured:
        builder = db.update_schema()
        builder.add_list_column(1, pycobble.ListConfig(max_elements=3))
        builder.commit()
        db.write(batch)
        db.put_bytes(start_bucket, _key(0), 0, _primary(start_bucket, 0), options)
    else:
        db.write(batch, await_durable=options.await_durable)
        db.put(start_bucket, _key(0), 0, _primary(start_bucket, 0), options)
    assert len(batch) == 0
    snapshot = db.take_snapshot()
    db.close()
    return os.getpid(), snapshot


def _scan_worker(config_json: str, plan, split, options):
    assert len(plan.splits()) == 2
    assert split.shard.db_id in {part.shard.db_id for part in plan.splits()}
    cursor = split.open_scanner(config_json, options)
    batches = []
    while True:
        batch = cursor.next_batch(BATCH_ROWS)
        batches.append(batch)
        if batch.end:
            break
    cursor.close()
    return os.getpid(), batches


def _reader_worker(config_json: str, structured: bool, snapshot, options):
    cls = pycobble.StructuredReader if structured else pycobble.Reader
    reader = cls.open_from_global_snapshot(config_json, snapshot)
    assert reader.configured_snapshot_id == snapshot.id
    large = reader.get(2, _key(256), options)
    multi = reader.multi_get(
        [(0, _key(3)), (3, _key(SCAN_LAST)), (1, b"missing")], options
    )
    return os.getpid(), large, multi


def _pid_worker() -> int:
    return os.getpid()


def _error_worker():
    raise pycobble.InputError("worker failure")


def _decoded_values(row, structured: bool, key: bytes) -> tuple[bytes, bytes]:
    if structured:
        assert row.kind(0) == pycobble.StructuredColumnKind.LIST
        assert row.list_size(0) == 2
        assert bytes(row.list_element(0, 1)) == b"tail:" + key
        return bytes(row.bytes(1)), bytes(row.list_element(0, 0))
    return bytes(row.column(1)), bytes(row.column(0))


def _decoded_batches(batches, structured: bool) -> list[tuple[int, bytes, bytes, bytes]]:
    assert len(batches) > 1
    assert batches[-1].end
    rows = []
    for batch in batches:
        assert len(batch) <= BATCH_ROWS
        for index in range(len(batch)):
            row = batch.row(index)
            key = bytes(row.key)
            value = row.value if structured else row
            primary, secondary = _decoded_values(value, structured, key)
            rows.append((row.bucket, key, primary, secondary))
    return rows


@pytest.mark.parametrize("structured", [False, True])
def test_spawn_snapshot_and_scan_transfer(tmp_path: Path, structured: bool) -> None:
    cfg = _config(tmp_path / ("structured" if structured else "raw"))
    context = multiprocessing.get_context("spawn")
    write_options = pycobble.WriteOptions(column_family="default", await_durable=False)
    left_batch = _writer_batch(0, structured, write_options)
    right_batch = _writer_batch(2, structured, write_options)
    with (
        ProcessPoolExecutor(max_workers=1, mp_context=context) as left_pool,
        ProcessPoolExecutor(max_workers=1, mp_context=context) as right_pool,
    ):
        left_future = left_pool.submit(
            _snapshot_worker, cfg, structured, 0, left_batch, write_options
        )
        right_future = right_pool.submit(
            _snapshot_worker, cfg, structured, 2, right_batch, write_options
        )
        left_pid, left_shard = left_future.result(timeout=120)
        right_pid, right_shard = right_future.result(timeout=120)
    writer_pids = {left_pid, right_pid}
    assert len(writer_pids) == 2
    assert len(left_batch) == len(right_batch) == ROWS_PER_BUCKET * 2 * 2
    assert left_shard.schema_id is not None
    assert right_shard.schema_id is not None
    assert left_shard.schema_column_families is not None
    assert right_shard.schema_column_families is not None
    captured_left_families = _snapshot_families(left_shard)
    captured_right_families = _snapshot_families(right_shard)
    left_shard = _roundtrip(left_shard)
    right_shard = _roundtrip(right_shard)
    assert left_shard.schema_id == right_shard.schema_id
    assert _snapshot_families(left_shard) == captured_left_families
    assert _snapshot_families(right_shard) == captured_right_families
    assert captured_left_families == captured_right_families

    coordinator = pycobble.DbCoordinator.open(cfg)
    global_snapshot = coordinator.materialize_global_snapshot(
        4, 7, [left_shard, right_shard]
    )
    restored_global = _roundtrip(global_snapshot)
    assert all(shard.schema_id is None for shard in restored_global.shards)

    if structured:
        plan = pycobble.StructuredScanPlan.from_global_snapshot(restored_global)
        scan_options = pycobble.StructuredScanOptions(columns=[1, 0])
        read_options = pycobble.StructuredReadOptions(columns=[1, 0])
    else:
        plan = pycobble.ScanPlan.from_global_snapshot(restored_global)
        scan_options = pycobble.ScanOptions(columns=[1, 0], read_ahead_bytes=4096)
        read_options = pycobble.ReadOptions(columns=[1, 0])
    plan.with_start(_key(SCAN_FIRST)).with_end(_key(SCAN_LAST + 1))
    restored_plan = _roundtrip(plan)
    splits = [_roundtrip(split) for split in restored_plan.splits()]
    assert len(splits) == 2
    partition = _roundtrip(splits[0].split_after(0, _key(255)))
    assert partition.before.end_at_inclusive.key == _key(255)
    assert partition.after.start_after_exclusive.key == _key(255)

    with (
        ProcessPoolExecutor(max_workers=1, mp_context=context) as reader_pool,
        ProcessPoolExecutor(max_workers=1, mp_context=context) as left_scanner_pool,
        ProcessPoolExecutor(max_workers=1, mp_context=context) as right_scanner_pool,
    ):
        reader_future = reader_pool.submit(
            _reader_worker, cfg, structured, restored_global, read_options
        )
        scan_futures = [
            left_scanner_pool.submit(
                _scan_worker, cfg, restored_plan, splits[0], scan_options
            ),
            left_scanner_pool.submit(
                _scan_worker, cfg, restored_plan, partition.before, scan_options
            ),
            right_scanner_pool.submit(
                _scan_worker, cfg, restored_plan, partition.after, scan_options
            ),
            right_scanner_pool.submit(
                _scan_worker, cfg, restored_plan, splits[1], scan_options
            ),
        ]
        reader_pid, large_row, multi = reader_future.result(timeout=120)
        scanned = [future.result(timeout=120) for future in scan_futures]
        with pytest.raises(pycobble.InputError, match="worker failure"):
            left_scanner_pool.submit(_error_worker).result(timeout=30)
        assert left_scanner_pool.submit(_pid_worker).result(timeout=30) == scanned[0][0]

    assert reader_pid not in writer_pids
    assert all(pid not in writer_pids for pid, _ in scanned)
    assert scanned[0][0] == scanned[1][0]
    assert scanned[2][0] == scanned[3][0]
    assert len({reader_pid, scanned[0][0], scanned[2][0]}) == 3
    assert _decoded_values(large_row, structured, _key(256)) == (
        _primary(2, 256), _secondary(2, 256)
    )
    assert len(_primary(2, 256)) > 65_536
    assert _decoded_values(multi.row(0), structured, _key(3)) == (
        _primary(0, 3), _secondary(0, 3)
    )
    assert _decoded_values(multi.row(1), structured, _key(SCAN_LAST)) == (
        _primary(3, SCAN_LAST), _secondary(3, SCAN_LAST)
    )
    assert not multi.row(2).found

    full_left, before, after, full_right = [
        _decoded_batches(batches, structured) for _, batches in scanned
    ]
    assert before + after == full_left
    actual = full_left + full_right
    expected = [
        (bucket, _key(index), _primary(bucket, index), _secondary(bucket, index))
        for bucket in range(4)
        for index in range(SCAN_FIRST, SCAN_LAST + 1)
    ]
    assert len(actual) == len(expected) == 4 * (SCAN_LAST - SCAN_FIRST + 1)
    assert actual == expected


def test_pickle_public_value_inventory_and_errors() -> None:
    live = {
        "Database",
        "Db",
        "DbCoordinator",
        "ReadOnlyDb",
        "Reader",
        "SingleDb",
        "PendingSnapshot",
        "PendingShardSnapshot",
        "ScanCursor",
        "SchemaBuilder",
        "PriorityQueue",
        "StructuredDb",
        "StructuredReadOnlyDb",
        "StructuredReader",
        "StructuredScanCursor",
        "StructuredSchemaBuilder",
        "StructuredSingleDb",
    }
    errors = {name for name in pycobble.__all__ if name.endswith("Error")}
    public_classes = {
        name for name in pycobble.__all__ if isinstance(getattr(pycobble, name), type)
    }
    value_types = public_classes - live - errors
    assert value_types == {
        "BucketRange", "ColumnFamily", "ColumnFamilyId", "SnapshotColumnFamily",
        "CounterValue", "ExpandStorageMode", "GaugeValue", "GlobalSnapshot",
        "HistogramValue", "MemtableType", "MergeOperatorSpec", "MetricLabel",
        "MetricSample", "OwnedBytes", "OwnedBatch", "OwnedRow", "OwnedMultiGetResult",
        "ReadOptions", "ReaderMode", "RecoveryMode", "ScanOptions", "ScanPlan",
        "ScanSplit", "ScanSplitBoundary", "ScanSplitPartition", "ScanRow", "Schema",
        "ShardSnapshot", "ListConfig", "ListRetainMode", "PriorityQueueBatch",
        "PriorityQueueEntry", "StructuredBatch", "StructuredColumn",
        "StructuredColumnKind", "StructuredMultiGetResult", "StructuredReadOptions",
        "StructuredRow", "StructuredScanOptions", "StructuredScanPlan",
        "StructuredScanRow", "StructuredScanSplit", "StructuredScanSplitBoundary",
        "StructuredScanSplitPartition", "StructuredSchema", "StructuredFamily",
        "StructuredWriteBatch", "WriteOptions", "WriteBatch", "BufferResult",
        "BufferStatus",
    }
    assert all("__reduce__" in getattr(pycobble, name).__dict__ for name in value_types)
    for name in errors:
        error_type = getattr(pycobble, name)
        restored = _roundtrip(error_type("message"))
        assert type(restored) is error_type
        assert str(restored) == "message"

    for value in (
        pycobble.RecoveryMode.LATEST_WITH_WAL,
        pycobble.MemtableType.ADAPTIVE,
        pycobble.ExpandStorageMode.ADOPT_ASYNC,
        pycobble.ReaderMode.CURRENT,
        pycobble.BufferStatus.BUFFER_TOO_SMALL,
        pycobble.StructuredColumnKind.LIST,
        pycobble.ListRetainMode.FIRST,
    ):
        assert _roundtrip(value) == value
    assert _roundtrip(pycobble.OwnedBytes._restore(b"payload")).to_bytes() == b"payload"
    assert not _roundtrip(pycobble.OwnedRow._restore(None)).found
    assert _roundtrip(pycobble.OwnedRow._restore([])).found
    read_options = _roundtrip(pycobble.ReadOptions(column_family="default", columns=[]))
    assert (read_options.column_family, read_options.columns) == ("default", [])
    write_options = _roundtrip(
        pycobble.WriteOptions(
            ttl_seconds=7, column_family="default", await_durable=False
        )
    )
    assert (
        write_options.ttl_seconds,
        write_options.column_family,
        write_options.await_durable,
    ) == (7, "default", False)
    with pytest.raises(pycobble.InputError):
        pycobble.StructuredRow._restore([(8, [])])
    with pytest.raises(pycobble.InputError):
        pycobble.WriteBatch._restore([(9, 0, b"key", 0, None, None, None)])
    with pytest.raises(pycobble.InputError):
        pycobble.StructuredWriteBatch._restore(
            [(9, 0, b"key", 0, None, None, None, True)]
        )

    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
        with pytest.raises(pycobble.InputError, match="worker failure"):
            pool.submit(_error_worker).result(timeout=30)


def test_pickle_detached_values_and_write_batches(tmp_path: Path) -> None:
    raw = pycobble.SingleDb.open(_config(tmp_path / "raw-results"))
    raw_batch = pycobble.WriteBatch()
    raw_batch.put(0, b"ordered", 0, b"first")
    raw_batch.delete(0, b"ordered", 0)
    raw_batch.put(0, b"ordered", 0, b"last")
    restored_raw_batch = _roundtrip(raw_batch)
    assert len(restored_raw_batch) == 3
    assert len(raw_batch) == 3  # Pickling does not consume the source batch.
    raw.write(restored_raw_batch)
    assert len(restored_raw_batch) == 0
    assert len(raw_batch) == 3
    assert bytes(raw.get(0, b"ordered").column(0)) == b"last"
    empty_projection = _roundtrip(pycobble.ReadOptions(columns=[]))
    assert raw.get(0, b"ordered", empty_projection).column_count == 0
    assert bytes(_roundtrip(raw.get(0, b"ordered")).column(0)) == b"last"
    assert not _roundtrip(raw.get(0, b"missing")).found
    multi = _roundtrip(raw.multi_get([(0, b"ordered"), (0, b"missing")]))
    assert bytes(multi.row(0).column(0)) == b"last"
    assert not multi.row(1).found
    cursor = raw.scan(0)
    detached = _roundtrip(cursor.next_batch(4))
    cursor.close()
    assert bytes(_roundtrip(detached.row(0)).column(0)) == b"last"
    assert _roundtrip(raw.current_schema()).column_families[0].column_count == 2
    assert _roundtrip(raw.metrics())[0].name == raw.metrics()[0].name
    raw.close()

    structured = pycobble.StructuredSingleDb.open(_config(tmp_path / "structured-results"))
    builder = structured.update_schema()
    builder.add_list_column(1, pycobble.ListConfig(max_elements=4))
    builder.commit()
    typed_batch = pycobble.StructuredWriteBatch()
    typed_batch.put_bytes(0, b"ordered", 0, b"bytes")
    typed_batch.put_list(0, b"ordered", 1, [b"first"])
    typed_batch.merge_list(0, b"ordered", 1, [b"second"])
    restored_typed_batch = _roundtrip(typed_batch)
    assert len(restored_typed_batch) == 3
    assert len(typed_batch) == 3
    structured.write(restored_typed_batch)
    assert len(restored_typed_batch) == 0
    assert len(typed_batch) == 3
    value = _roundtrip(structured.get(0, b"ordered"))
    assert bytes(value.bytes(0)) == b"bytes"
    assert [
        bytes(value.list_element(1, i)) for i in range(value.list_size(1))
    ] == [b"first", b"second"]
    reordered = _roundtrip(pycobble.StructuredReadOptions(columns=[1, 0]))
    assert bytes(structured.get(0, b"ordered", reordered).bytes(1)) == b"bytes"
    typed_multi = _roundtrip(structured.multi_get([(0, b"ordered"), (0, b"missing")]))
    assert typed_multi.row(0).kind(1) == pycobble.StructuredColumnKind.LIST
    assert not typed_multi.row(1).found
    typed_cursor = structured.scan(0)
    typed_detached = _roundtrip(typed_cursor.next_batch(4))
    typed_cursor.close()
    assert bytes(_roundtrip(typed_detached.row(0)).value.bytes(0)) == b"bytes"
    typed_schema = _roundtrip(structured.current_schema())
    assert any(
        column.list_config is not None and column.list_config.max_elements == 4
        for family in typed_schema.families
        for column in family.columns
    )
    structured.close()

    entry = _roundtrip(pycobble.PriorityQueueEntry._restore(b"k", b"v"))
    assert (bytes(entry.key), bytes(entry.value)) == (b"k", b"v")
    pq_batch = _roundtrip(pycobble.PriorityQueueBatch._restore([(b"k", b"v")]))
    assert bytes(pq_batch.entry(0).value) == b"v"
    for metric in (
        pycobble.CounterValue._restore(2),
        pycobble.GaugeValue._restore(1.5),
        pycobble.HistogramValue._restore(1, 2.5, 2.5, 2.5),
    ):
        assert type(_roundtrip(metric)) is type(metric)
        sample = pycobble.MetricSample._restore(
            "sample", [pycobble.MetricLabel._restore("a", "b")], metric
        )
        assert type(_roundtrip(sample).value) is type(metric)
    result = _roundtrip(
        pycobble.BufferResult._restore(pycobble.BufferStatus.OK, 2, 2, 1)
    )
    assert result.bytes_written == 2
