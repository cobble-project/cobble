package io.cobble.table;

import java.util.Arrays;

/** Durable position immediately after a logical row, including its physical-entry row offset. */
public final class TableReadPosition {
    private final int bucket;
    private final byte[] physicalKey;
    private final int intraEntryOffset;

    public TableReadPosition(int bucket, byte[] physicalKey, int intraEntryOffset) {
        this(bucket, physicalKey, intraEntryOffset, true);
    }

    /**
     * Retains a cursor-private key without copying it. Callers must not mutate the key after this
     * method returns; public {@link #physicalKey()} remains defensive.
     */
    static TableReadPosition owned(int bucket, byte[] physicalKey, int intraEntryOffset) {
        return new TableReadPosition(bucket, physicalKey, intraEntryOffset, false);
    }

    private TableReadPosition(
            int bucket, byte[] physicalKey, int intraEntryOffset, boolean copyPhysicalKey) {
        if (intraEntryOffset < 0) {
            throw new IllegalArgumentException("intraEntryOffset must be >= 0");
        }
        this.bucket = bucket;
        this.physicalKey =
                physicalKey == null || !copyPhysicalKey
                        ? physicalKey
                        : Arrays.copyOf(physicalKey, physicalKey.length);
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
