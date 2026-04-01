package io.cobble;

import java.util.ArrayList;
import java.util.List;

/**
 * Distributed scan plan built from a global snapshot manifest.
 *
 * <p>The plan can generate one {@link ScanSplit} per shard snapshot. Optional start/end bounds are
 * copied to each split.
 */
public final class ScanPlan {
    private final GlobalSnapshot manifest;
    private byte[] startInclusive;
    private byte[] endExclusive;

    private ScanPlan(GlobalSnapshot manifest) {
        this.manifest = manifest;
    }

    public static ScanPlan fromGlobalSnapshot(GlobalSnapshot manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest must not be null");
        }
        return new ScanPlan(manifest);
    }

    /** Optional inclusive scan start bound. */
    public ScanPlan withStart(byte[] startInclusive) {
        this.startInclusive = copyOrNull(startInclusive);
        return this;
    }

    /** Optional exclusive scan end bound. */
    public ScanPlan withEnd(byte[] endExclusive) {
        this.endExclusive = copyOrNull(endExclusive);
        return this;
    }

    /** Generates one split per shard snapshot in the global snapshot manifest. */
    public List<ScanSplit> splits() {
        List<ScanSplit> out = new ArrayList<ScanSplit>();
        if (manifest.shardSnapshots == null) {
            return out;
        }
        for (ShardSnapshot shard : manifest.shardSnapshots) {
            out.add(new ScanSplit(shard, startInclusive, endExclusive));
        }
        return out;
    }

    private static byte[] copyOrNull(byte[] value) {
        if (value == null) {
            return null;
        }
        byte[] copied = new byte[value.length];
        System.arraycopy(value, 0, copied, 0, value.length);
        return copied;
    }
}
