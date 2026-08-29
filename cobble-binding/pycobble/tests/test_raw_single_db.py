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
            "num_columns": 2,
            "total_buckets": 4,
            "block_cache_size": 0,
        }
    )


def test_raw_crud_owned_views_and_close(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "db")) as db:
        db.put(0, b"key", 0, b"value-0")
        db.put(0, bytearray(b"key"), 1, bytearray(b"value-1"))
        db.put(0, memoryview(b"view-key"), 0, memoryview(b"view-value"))
        assert bytes(db.get(0, memoryview(b"view-key")).column(0)) == b"view-value"

        row = db.get(0, b"key")
        assert row.found
        assert row.column_count == 2
        value = row.column(0)
        assert value is not None
        view = value.view()
        assert view.readonly
        assert view.tobytes() == b"value-0"
        assert bytes(row.column(1)) == b"value-1"

        del row
        del value
        gc.collect()
        assert view.tobytes() == b"value-0"

        db.delete(0, b"key", 0)
        assert not db.get(0, b"missing")

    db.close()


def test_errors_have_stable_codes() -> None:
    try:
        pycobble.SingleDb.open("not-json")
    except pycobble.ConfigurationError as error:
        assert error.code == "CB_CONFIGURATION"
        assert "Configuration error" in str(error)
    else:
        raise AssertionError("invalid JSON unexpectedly opened")


def test_scan_cursor_owns_database_access_and_preserves_order(tmp_path: Path) -> None:
    db = pycobble.SingleDb.open(config(tmp_path / "scan"))
    for index in range(19):
        key = f"key-{index:03d}".encode()
        db.put(0, key, 0, b"value-" + key)

    cursor = db.scan(0, b"key-003", b"key-015")
    with pytest.raises(pycobble.InternalStateError):
        db.close()

    keys: list[bytes] = []
    while True:
        batch = cursor.next(4)
        keys.extend(bytes(batch.row(index).key) for index in range(len(batch)))
        if batch.end:
            break
    assert keys == [f"key-{index:03d}".encode() for index in range(3, 15)]

    cursor.close()
    db.close()


def test_write_batch_is_reusable_after_success(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "batch")) as db:
        batch = pycobble.WriteBatch()
        for index in range(32):
            batch.put(index % 4, f"key-{index}".encode(), 0, f"value-{index}".encode())
        assert len(batch) == 32
        db.write(batch)
        assert not batch

        batch.put(0, b"reused", 0, b"value")
        db.write(batch, await_durable=False)
        assert bytes(db.get(0, b"reused").column(0)) == b"value"


def test_multi_get_preserves_order_duplicates_and_missing(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "multi")) as db:
        db.put(0, b"same", 0, b"zero")
        db.put(1, b"same", 0, b"one")
        db.put(0, b"", 0, b"empty")

        rows = db.multi_get(
            [
                (0, b"same"),
                (0, memoryview(b"same")),
                (1, bytearray(b"same")),
                (0, b""),
                (3, b"missing"),
            ]
        )
        assert len(rows) == 5
        assert bytes(rows.row(0).column(0)) == b"zero"
        assert bytes(rows.row(1).column(0)) == b"zero"
        assert bytes(rows.row(2).column(0)) == b"one"
        assert bytes(rows.row(3).column(0)) == b"empty"
        assert not rows.row(4)


def test_caller_buffers_retry_without_mutation_or_advancement(tmp_path: Path) -> None:
    with pycobble.SingleDb.open(config(tmp_path / "into")) as db:
        db.put(0, b"key", 0, b"caller-owned")
        options = pycobble.ReadOptions(columns=[0])
        too_small = bytearray(b"unchanged")
        result = db.get_column_into(0, b"key", too_small, options)
        assert result.status == pycobble.BufferStatus.BufferTooSmall
        assert result.bytes_required == len(b"caller-owned")
        assert too_small == b"unchanged"
        output = bytearray(result.bytes_required)
        result = db.get_column_into(0, b"key", output, options)
        assert result.status == pycobble.BufferStatus.Ok
        assert output == b"caller-owned"

        cursor = db.scan(0)
        tiny = bytearray(b"sentinel")
        result = cursor.next_batch_into(8, tiny)
        assert result.status == pycobble.BufferStatus.BufferTooSmall
        assert tiny == b"sentinel"
        encoded = bytearray(result.bytes_required)
        result = cursor.next_batch_into(8, encoded)
        assert result.status == pycobble.BufferStatus.Ok
        assert result.row_count == 1
        assert encoded[:4] == b"CBRB"
        cursor.close()
