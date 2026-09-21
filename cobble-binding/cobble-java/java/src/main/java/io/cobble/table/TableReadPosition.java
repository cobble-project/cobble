package io.cobble.table;

import java.util.Arrays;

/** Durable position immediately after a logical row, including its physical-entry row offset. */
public final class TableReadPosition {
    private final int bucket;
    private final byte[] physicalKey;
    private final int intraEntryOffset;

    public TableReadPosition(int bucket, byte[] physicalKey, int intraEntryOffset) {
        if (intraEntryOffset < 0) {
            throw new IllegalArgumentException("intraEntryOffset must be >= 0");
        }
        this.bucket = bucket;
        this.physicalKey =
                physicalKey == null ? null : Arrays.copyOf(physicalKey, physicalKey.length);
        this.intraEntryOffset = intraEntryOffset;
    }

    public int bucket() {
        return bucket;
    }

    public byte[] physicalKey() {
        return physicalKey == null ? null : Arrays.copyOf(physicalKey, physicalKey.length);
    }

    public int intraEntryOffset() {
        return intraEntryOffset;
    }
}
