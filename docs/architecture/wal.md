---
title: Write-Ahead Log
parent: Architecture
nav_order: 7
---

# Write-Ahead Log

Cobble's write-ahead log (WAL) is an optional crash-recovery log. When enabled, each mutation is
recorded before it is applied to the memtable. WAL records are published in groups so concurrent
writes can share storage I/O; writes wait for durable publication by default.

WAL is disabled by default. Enabling it requires exactly one volume with the `Wal` usage kind:

```rust
config.wal_enabled = true;
config.volumes.push(VolumeDescriptor::new(
    "s3://shared-storage/cobble-wal",
    vec![VolumeUsageKind::Wal],
));
```

The `wal_flush_interval_ms` setting controls the maximum group-publication interval and defaults to
5 milliseconds.

## Recovery Modes

Recovery always starts from a snapshot. The selected `RecoveryMode` controls whether Cobble stops
at that snapshot or also replays its durable WAL tail:

| Mode | Behavior |
|------|----------|
| `SnapshotOnly` | Restore exactly the selected snapshot. |
| `LatestWithWal` | If the selected snapshot is the latest one, replay durable WAL records written after it. Historical snapshots are restored exactly. |

```rust
let db = Db::open_from_snapshot_with_recovery_mode(
    config,
    snapshot_id,
    db_id,
    RecoveryMode::LatestWithWal,
)?;
```

`Db::resume` defaults to `LatestWithWal`, while `Db::open_from_snapshot` and `SingleDb::resume`
default to `SnapshotOnly`. Their `*_with_recovery_mode` variants let the caller choose explicitly.
This choice is independent of whether WAL is enabled for new writes in the current configuration.

The snapshot manifest records the WAL checkpoint and storage route used for replay. Recovery uses
that recorded route even if the current configuration selects a different WAL volume for new
writes. Storage credentials are not persisted and must still be supplied by the runtime.

WAL replay does not create a snapshot. The next normal snapshot includes the recovered writes and,
after successful publication, removes WAL segments that are no longer needed.
