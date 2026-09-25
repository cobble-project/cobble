from __future__ import annotations

import json
import os
import time
from urllib.parse import urlsplit

import pycobble
import pytest


def s3_environment() -> tuple[str, str, str, str]:
    names = (
        "COBBLE_S3_ENDPOINT",
        "COBBLE_S3_BUCKET",
        "COBBLE_S3_ACCESS_ID",
        "COBBLE_S3_SECRET_KEY",
    )
    values = tuple(os.environ.get(name) for name in names)
    if not all(values):
        pytest.skip("COBBLE_S3_* environment is not configured")
    return values  # type: ignore[return-value]


def s3_config(
    endpoint: str,
    bucket: str,
    access_id: str,
    secret_key: str,
    prefix: str,
) -> str:
    parsed = urlsplit(endpoint)
    assert parsed.scheme in {"http", "https"}
    assert parsed.netloc
    base_dir = (
        f"s3://{parsed.netloc}/{bucket}/{prefix}"
        f"?endpoint_scheme={parsed.scheme}&region=us-east-1"
        "&disable_config_load=true&disable_ec2_metadata=true"
        "&enable_virtual_host_style=false"
    )
    return json.dumps(
        {
            "volumes": [
                {
                    "base_dir": base_dir,
                    "access_id": access_id,
                    "secret_key": secret_key,
                    "kinds": ["meta", "primary_data_priority_high", "snapshot"],
                }
            ],
            "num_columns": 1,
            "total_buckets": 1,
            "block_cache_size": 0,
        }
    )


def test_s3_raw_and_structured_exact_snapshot_resume() -> None:
    endpoint, bucket, access_id, secret_key = s3_environment()
    nonce = time.time_ns()

    raw_config = s3_config(
        endpoint, bucket, access_id, secret_key, f"pycobble-s3-{nonce}/raw"
    )
    raw = pycobble.SingleDb.open(raw_config)
    batch = pycobble.WriteBatch()
    expected_raw: dict[bytes, bytes] = {}
    for index in range(256):
        key = f"raw-{index:06d}".encode()
        value = f"row={index};".encode().ljust(384, bytes([97 + index % 26]))
        expected_raw[key] = value
        batch.put(0, key, 0, value)
        if len(batch) == 64:
            raw.write(batch)
    assert not batch
    raw_snapshot = raw.take_snapshot()
    raw.close()

    raw = pycobble.SingleDb.resume(
        raw_config, raw_snapshot.id, pycobble.RecoveryMode.SNAPSHOT_ONLY
    )
    for key in list(expected_raw)[::29]:
        assert bytes(raw.get(0, key).column(0)) == expected_raw[key]
    raw_keys: list[bytes] = []
    cursor = raw.scan(0)
    while True:
        rows = cursor.next_batch(47)
        raw_keys.extend(bytes(rows.row(index).key) for index in range(len(rows)))
        if rows.end:
            break
    cursor.close()
    assert raw_keys == sorted(expected_raw)
    raw.close()

    structured_config = s3_config(
        endpoint,
        bucket,
        access_id,
        secret_key,
        f"pycobble-s3-{nonce}/structured",
    )
    structured = pycobble.StructuredDb.open(structured_config)
    schema = structured.update_schema()
    schema.add_list_column(1, pycobble.ListConfig(max_elements=8))
    schema.commit()
    batch = pycobble.StructuredWriteBatch()
    expected_structured: dict[bytes, tuple[bytes, list[bytes]]] = {}
    for index in range(192):
        key = f"structured-{index:06d}".encode()
        value = f"row={index};".encode().ljust(320, bytes([65 + index % 26]))
        elements = [f"group={index % 17}".encode(), f"row={index}".encode()]
        expected_structured[key] = (value, elements)
        batch.put_bytes(0, key, 0, value)
        batch.put_list(0, key, 1, elements)
        if len(batch) == 96:
            structured.write(batch)
    assert not batch
    db_id = structured.id
    structured_snapshot = structured.take_snapshot()
    structured.close()

    structured = pycobble.StructuredDb.resume_from_snapshot(
        structured_config,
        structured_snapshot.snapshot_id,
        db_id,
        pycobble.RecoveryMode.SNAPSHOT_ONLY,
    )
    for key in list(expected_structured)[::23]:
        row = structured.get(0, key)
        value, elements = expected_structured[key]
        assert bytes(row.bytes(0)) == value
        assert [bytes(row.list_element(1, i)) for i in range(row.list_size(1))] == elements
    structured_keys: list[bytes] = []
    cursor = structured.scan(0)
    while True:
        rows = cursor.next_batch(41)
        structured_keys.extend(bytes(rows.row(index).key) for index in range(len(rows)))
        if rows.end:
            break
    cursor.close()
    assert structured_keys == sorted(expected_structured)
    structured.close()
