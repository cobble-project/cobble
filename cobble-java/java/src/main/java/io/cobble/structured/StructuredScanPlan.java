package io.cobble.structured;

import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * Structured distributed scan plan built from a global snapshot manifest.
 *
 * <p>The plan can generate one {@link StructuredScanSplit} per shard snapshot. Optional start/end
 * bounds are copied to each split.
 */
public final class StructuredScanPlan {
    private final GlobalSnapshot manifest;
    private byte[] startInclusive;
    private byte[] endExclusive;

    private StructuredScanPlan(GlobalSnapshot manifest) {
        this.manifest = manifest;
    }

    public static StructuredScanPlan fromGlobalSnapshot(GlobalSnapshot manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("manifest must not be null");
        }
        return new StructuredScanPlan(manifest);
    }

    /** Optional inclusive scan start bound. */
    public StructuredScanPlan withStart(byte[] startInclusive) {
        this.startInclusive = copyOrNull(startInclusive);
        return this;
    }

    /** Optional exclusive scan end bound. */
    public StructuredScanPlan withEnd(byte[] endExclusive) {
        this.endExclusive = copyOrNull(endExclusive);
        return this;
    }

    /** Generates one split per shard snapshot in the global snapshot manifest. */
    public List<StructuredScanSplit> splits() {
        List<StructuredScanSplit> out = new ArrayList<StructuredScanSplit>();
        if (manifest.shardSnapshots == null) {
            return out;
        }
        for (ShardSnapshot shard : manifest.shardSnapshots) {
            out.add(new StructuredScanSplit(shard, startInclusive, endExclusive));
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
