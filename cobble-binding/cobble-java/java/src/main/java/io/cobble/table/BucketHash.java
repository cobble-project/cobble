package io.cobble.table;

import java.nio.ByteBuffer;

/** Stable bucket hash matching {@link java.util.Arrays#hashCode(byte[])}. */
public final class BucketHash {
    private final int totalBuckets;
    private final int powerOfTwoMask;

    public BucketHash(int totalBuckets) {
        if (totalBuckets < 1 || totalBuckets > 65536)
            throw new IllegalArgumentException("bucket count must be in [1, 65536]");
        this.totalBuckets = totalBuckets;
        this.powerOfTwoMask = (totalBuckets & (totalBuckets - 1)) == 0 ? totalBuckets - 1 : -1;
    }

    static int hash(byte[] encodedBucketKey) {
        return hash(encodedBucketKey, 0, encodedBucketKey.length);
    }

    private static int hash(byte[] bytes, int offset, int length) {
        int hash = 1;
        int end = offset + length;
        for (int index = offset; index < end; index++) hash = 31 * hash + bytes[index];
        return hash;
    }

    public int bucket(byte[] encodedBucketKey) {
        return bucket(hash(encodedBucketKey));
    }

    public int bucket(ByteBuffer encodedBucketKey) {
        return bucket(hash(encodedBucketKey));
    }

    private int bucket(int hash) {
        // floorMod by a power of two is exactly the corresponding low bits, including for
        // negative two's-complement hashes.
        return powerOfTwoMask >= 0 ? hash & powerOfTwoMask : Math.floorMod(hash, totalBuckets);
    }

    private static int hash(ByteBuffer encodedBucketKey) {
        int position = encodedBucketKey.position();
        int remaining = encodedBucketKey.remaining();
        if (encodedBucketKey.hasArray())
            return hash(
                    encodedBucketKey.array(), encodedBucketKey.arrayOffset() + position, remaining);

        int hash = 1;
        int limit = position + remaining;
        for (int index = position; index < limit; index++)
            hash = 31 * hash + encodedBucketKey.get(index);
        return hash;
    }
}
