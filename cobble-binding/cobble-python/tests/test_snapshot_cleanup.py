from __future__ import annotations

import gc
import json
import pickle
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import pycobble
import pytest


def descriptor(root: Path) -> dict:
    return {
        "base_dir": root.as_uri(),
        "kinds": ["meta", "primary_data_priority_high", "snapshot"],
        "access_id": "test-access",
        "secret_key": "test-secret",
        "custom_options": {},
    }


def volume(root: Path) -> str:
    return json.dumps(descriptor(root))


def config(root: Path, *, incremental: bool = True) -> str:
    return json.dumps(
        {
            "volumes": [descriptor(root)],
            "num_columns": 1,
            "total_buckets": 1,
            "memtable_capacity": "4MB",
            "l0_file_limit": 100,
            "snapshot_on_flush": False,
            "snapshot_only_track": True,
            "snapshot_disable_incremental_base_link": not incremental,
            "value_separation_threshold": 16,
        }
    )


def path(manifest_path: str) -> Path:
    return Path(url2pathname(urlparse(manifest_path).path))


def snapshot(db: pycobble.Db, key: bytes) -> pycobble.ShardSnapshot:
    db.put(0, key, 0, b"v" * 128)
    return db.take_snapshot()


def test_shard_preservation_forcing_results_pickle_and_errors(tmp_path: Path) -> None:
    with pycobble.Db.open(config(tmp_path)) as db:
        base = snapshot(db, b"base")
        next_snapshot = snapshot(db, b"next")
    result = pycobble.cleanup_shard_snapshot(
        volume(tmp_path), "[]", base.db_id, base.snapshot_id
    )
    assert isinstance(result, pycobble.SnapshotCleanupResult)
    assert result.deleted_files == result.deleted_shard_snapshots == 0
    assert result.deleted_global_snapshots == 0
    assert len(result.preserved_snapshots) == 1
    restored = pickle.loads(pickle.dumps(result))
    assert restored.deleted_files == restored.deleted_shard_snapshots == 0
    assert restored.deleted_global_snapshots == 0
    identity = restored.preserved_snapshots[0]
    assert isinstance(identity, pycobble.ShardSnapshotIdentity)
    assert identity.db_id == base.db_id
    assert identity.snapshot_id == base.snapshot_id
    assert path(identity.manifest_path) == path(base.manifest_path)
    assert pickle.loads(pickle.dumps(identity)).manifest_path == identity.manifest_path
    restored.preserved_snapshots.clear()
    assert len(restored.preserved_snapshots) == 1
    with pytest.raises(AttributeError):
        restored.deleted_files = 1
    with pytest.raises(AttributeError):
        identity.snapshot_id = 3
    assert path(base.manifest_path).exists()

    forced = pycobble.cleanup_shard_snapshot(
        volume(tmp_path), "[]", base.db_id, base.snapshot_id, preserve_dependencies=False
    )
    assert forced.deleted_shard_snapshots == 1
    assert forced.deleted_global_snapshots == 0
    assert forced.deleted_files > 0
    assert forced.preserved_snapshots == []
    assert not path(base.manifest_path).exists()
    assert path(next_snapshot.manifest_path).exists()

    for bad in ("{", "null", "{}"):
        with pytest.raises(pycobble.InputError):
            pycobble.cleanup_global_snapshot(bad, "[]", 0)
    for bad in ("{", "null", "[null]"):
        with pytest.raises(pycobble.InputError):
            pycobble.cleanup_global_snapshot(volume(tmp_path), bad, 0)
    with pytest.raises(pycobble.ConfigurationError):
        pycobble.cleanup_shard_snapshot(volume(tmp_path), "[]", "../escape", 0)
    with pytest.raises(OverflowError):
        pycobble.cleanup_global_snapshot(volume(tmp_path), "[]", -1)
    with pytest.raises(TypeError):
        pycobble.cleanup_global_snapshot(volume(tmp_path), "[]", 0, False)


def test_global_older_selection_uses_global_ids_and_separate_routes(tmp_path: Path) -> None:
    metadata = tmp_path / "metadata"
    shard_root = tmp_path / "shards"
    coordinator = pycobble.DbCoordinator.open(config(metadata))
    with pycobble.Db.open(config(shard_root, incremental=False)) as db:
        shards = []
        for index in range(3):
            shards.append(snapshot(db, str(index).encode()))
            coordinator.materialize_global_snapshot(1, index + 7, [shards[-1]])
    del coordinator
    gc.collect()
    assert shards[-1].snapshot_id < 8
    result = pycobble.cleanup_global_snapshot(
        volume(metadata), json.dumps([descriptor(shard_root)]), 8,
        include_older_snapshots=True,
    )
    assert result.deleted_global_snapshots == result.deleted_shard_snapshots == 2
    assert result.preserved_snapshots == []
    for index, shard in enumerate(shards):
        assert path(shard.manifest_path).exists() == (index == 2)
        assert (metadata / f"snapshot/SNAPSHOT-{index + 7}").exists() == (index == 2)
    reader_config = json.loads(config(metadata))
    reader_config["volumes"].append(descriptor(shard_root))
    reader = pycobble.Reader.open_current(json.dumps(reader_config))
    assert bytes(reader.get(0, b"2").column(0)) == b"v" * 128


def test_exported_shard_and_global_metadata_bases(tmp_path: Path) -> None:
    source = tmp_path / "source"
    standalone = tmp_path / "standalone"
    exported = tmp_path / "exported"
    coordinator = pycobble.DbCoordinator.open(config(source))
    with pycobble.Db.open(config(source)) as db:
        original = snapshot(db, b"export")
        shard = db.export_snapshot(
            original.snapshot_id, volume(standalone / "shards")
        ).wait()
        global_shard = db.export_snapshot(
            original.snapshot_id, volume(exported / "shards")
        ).wait()
        global_snapshot = coordinator.take_exported_global_snapshot(1, [global_shard])
        coordinator.materialize_exported_global_snapshot(global_snapshot, volume(exported))
    del coordinator
    gc.collect()
    shard_result = pycobble.cleanup_shard_snapshot(
        volume(standalone / "shards"), "[]", shard.db_id, shard.snapshot_id
    )
    assert shard_result.deleted_shard_snapshots == 1
    assert shard_result.deleted_global_snapshots == 0
    assert shard_result.deleted_files > 0
    global_result = pycobble.cleanup_global_snapshot(volume(exported), "[]", 0)
    assert global_result.deleted_shard_snapshots == global_result.deleted_global_snapshots == 1
    assert global_result.deleted_files > 0
    assert not path(shard.manifest_path).exists()
    assert not path(global_shard.manifest_path).exists()
    assert not (exported / "snapshot/SNAPSHOT-0").exists()
    assert path(original.manifest_path).exists()
