package io.cobble;

import java.io.Serializable;
import java.util.Arrays;

/** Portable dedicated compaction plan produced by {@link DedicatedCompactionMonitor}. */
public final class DedicatedCompactionPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] encoded;

    DedicatedCompactionPlan(byte[] encoded) {
        if (encoded == null || encoded.length == 0) {
            throw new IllegalArgumentException("encoded plan must not be empty");
        }
        this.encoded = encoded;
    }

    public static DedicatedCompactionPlan decode(byte[] encoded) {
        return new DedicatedCompactionPlan(Arrays.copyOf(encoded, encoded.length));
    }

    public byte[] encode() {
        return Arrays.copyOf(encoded, encoded.length);
    }

    public String jobId() {
        NativeLoader.load();
        return jobIdInternal(encoded);
    }

    public String dbId() {
        NativeLoader.load();
        return dbIdInternal(encoded);
    }

    byte[] encodedUnsafe() {
        return encoded;
    }

    private static native String jobIdInternal(byte[] encoded);

    private static native String dbIdInternal(byte[] encoded);
}
