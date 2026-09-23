package io.cobble.table;

import java.util.Arrays;

/** Fully encoded physical key and its already-routed bucket. */
public final class TablePhysicalKey {
    private final int bucket;
    private final byte[] bytes;

    public TablePhysicalKey(int bucket, byte[] bytes) {
        if (bucket < 0 || bucket > 65535) {
            throw new IllegalArgumentException("bucket must be in range 0..65535");
        }
        if (bytes == null) throw new IllegalArgumentException("bytes must not be null");
        this.bucket = bucket;
        this.bytes = Arrays.copyOf(bytes, bytes.length);
    }

    public int bucket() {
        return bucket;
    }

    public byte[] bytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }
}
