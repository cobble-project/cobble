package io.cobble;

/** Options for offline snapshot cleanup. */
public final class SnapshotCleanupOptions {
    /**
     * Select all IDs at or below the requested ID. Global cleanup compares global IDs, not shard
     * IDs.
     */
    public boolean includeOlderSnapshots = false;
}
