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
