package io.cobble.structured;

/**
 * A batch of typed scan results from a structured scan cursor.
 *
 * <p>Each batch contains zero or more rows. Use {@link #getRow(int)} to access individual rows with
 * typed column values.
 */
public final class ScanBatch {

    private static final byte[][] EMPTY_KEYS = new byte[0][];
    private static final Object[][] EMPTY_COLS = new Object[0][];
    private static final ScanBatch EMPTY = new ScanBatch(EMPTY_KEYS, EMPTY_COLS, null, false);

    final byte[][] keys;
    // [row][col] -> null | byte[] (Bytes) | byte[][] (List)
    final Object[][] rawColumns;

    /** Last key in this batch; use as exclusive start anchor for subsequent batch. */
    public final byte[] nextStartAfterExclusive;

    /** Whether more rows are available after this batch. */
    public final boolean hasMore;

    ScanBatch(
            byte[][] keys, Object[][] rawColumns, byte[] nextStartAfterExclusive, boolean hasMore) {
        this.keys = keys == null ? EMPTY_KEYS : keys;
        this.rawColumns = rawColumns == null ? EMPTY_COLS : rawColumns;
        this.nextStartAfterExclusive = nextStartAfterExclusive;
        this.hasMore = hasMore;
    }

    static ScanBatch empty() {
        return EMPTY;
    }

    /** Returns the number of rows in this batch. */
    public int size() {
        return keys.length;
    }

    /**
     * Returns a typed {@link Row} at the given index.
     *
     * @param index the zero-based row index
     */
    public Row getRow(int index) {
        Object[] cols = rawColumns[index];
        ColumnValue[] values = new ColumnValue[cols.length];
        for (int i = 0; i < cols.length; i++) {
            if (cols[i] == null) {
                values[i] = null;
            } else if (cols[i] instanceof byte[]) {
                values[i] = ColumnValue.ofBytes((byte[]) cols[i]);
            } else if (cols[i] instanceof byte[][]) {
                values[i] = ColumnValue.ofList((byte[][]) cols[i]);
            }
        }
        return new Row(keys[index], values);
    }
}
