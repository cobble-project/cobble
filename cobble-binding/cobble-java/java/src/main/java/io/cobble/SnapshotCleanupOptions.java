package io.cobble;

/** Options for offline snapshot cleanup. */
public final class SnapshotCleanupOptions {
    /**
     * Preserve selected snapshots and files required by surviving snapshots. Disabling this can
     * make surviving snapshots unrecoverable.
     */
    public boolean preserveDependencies = true;

    /**
     * Select all IDs at or below the requested ID. Global cleanup compares global IDs, not shard
     * IDs.
     */
    public boolean includeOlderSnapshots = false;
}
