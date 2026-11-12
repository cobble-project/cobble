package io.cobble;

/** Controls immediate snapshots around bucket expansion or shrinking. */
public final class RescaleOptions {
    /** Create a pre-operation handoff/recovery snapshot. */
    public boolean snapshotBefore = true;

    /**
     * Create and wait for a post-operation snapshot. If disabled, a later snapshot is needed to
     * recover the topology change; WAL replay does not replace it. Automatic flush snapshots and
     * adoption's safety snapshot are unaffected.
     */
    public boolean snapshotAfter = true;
}
