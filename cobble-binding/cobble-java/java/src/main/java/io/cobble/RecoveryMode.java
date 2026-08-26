package io.cobble;

/** Selects whether recovery restores only a snapshot or also replays its durable WAL tail. */
public enum RecoveryMode {
    SNAPSHOT_ONLY,
    LATEST_WITH_WAL
}
