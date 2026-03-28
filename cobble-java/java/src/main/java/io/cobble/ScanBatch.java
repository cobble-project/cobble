package io.cobble;

/** One batched chunk of scan results for selected columns. */
public final class ScanBatch {
    private static final byte[][] EMPTY_KEYS = new byte[0][];
    private static final byte[][][] EMPTY_VALUES = new byte[0][][];
    private static final ScanBatch EMPTY = new ScanBatch(EMPTY_KEYS, EMPTY_VALUES, null, false);

    /** Keys in this batch. */
    public final byte[][] keys;

    /** Selected column values aligned with {@link #keys}. */
    public final byte[][][] values;

    /** Last key in this batch (exclusive resume anchor for next batch). */
    public final byte[] nextStartAfterExclusive;

    /** Whether more rows are available after this batch. */
    public final boolean hasMore;

    ScanBatch(byte[][] keys, byte[][][] values, byte[] nextStartAfterExclusive, boolean hasMore) {
        this.keys = keys == null ? EMPTY_KEYS : keys;
        this.values = values == null ? EMPTY_VALUES : values;
        this.nextStartAfterExclusive = nextStartAfterExclusive;
        this.hasMore = hasMore;
    }

    static ScanBatch empty() {
        return EMPTY;
    }
}
