package io.cobble;

/**
 * Native-backed scan options for range iteration.
 *
 * <p>One {@code ScanOptions} instance can be reused across multiple scan calls.
 */
public final class ScanOptions extends NativeObject {
    private static final int[] DEFAULT_COLUMNS = new int[] {0};

    public ScanOptions() {
        super(loadAndCreateHandle());
    }

    public ScanOptions readAheadBytes(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("readAheadBytes must be >= 0");
        }
        setReadAheadBytes(nativeHandle, value);
        return this;
    }

    public ScanOptions batchSize(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        setBatchSize(nativeHandle, value);
        return this;
    }

    public ScanOptions maxRows(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("maxRows must be > 0");
        }
        setMaxRows(nativeHandle, value);
        return this;
    }

    public ScanOptions columns(int... columnIndices) {
        int[] effective = columnIndices;
        if (effective == null || effective.length == 0) {
            throw new IllegalArgumentException("columns must not be empty");
        }
        for (int column : effective) {
            if (column < 0) {
                throw new IllegalArgumentException("column index must be >= 0");
            }
        }
        setColumns(nativeHandle, effective);
        return this;
    }

    /** Target one column family for subsequent scans. */
    public ScanOptions columnFamily(String columnFamily) {
        if (columnFamily == null || columnFamily.trim().isEmpty()) {
            throw new IllegalArgumentException("columnFamily must not be blank");
        }
        setColumnFamily(nativeHandle, columnFamily);
        return this;
    }

    /** Clear any previously selected column family and fall back to default family. */
    public ScanOptions clearColumnFamily() {
        clearColumnFamily(nativeHandle);
        return this;
    }

    public static ScanOptions forColumn(int columnIndex) {
        return new ScanOptions().columns(columnIndex);
    }

    public static ScanOptions forColumns(int... columnIndices) {
        return new ScanOptions().columns(columnIndices);
    }

    public static ScanOptions defaults() {
        return new ScanOptions().columns(DEFAULT_COLUMNS);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long createHandle();

    private static long loadAndCreateHandle() {
        NativeLoader.load();
        return createHandle();
    }

    private static native void setReadAheadBytes(long nativeHandle, int readAheadBytes);

    private static native void setBatchSize(long nativeHandle, int batchSize);

    private static native void setMaxRows(long nativeHandle, int maxRows);

    private static native void setColumns(long nativeHandle, int[] columns);

    private static native void setColumnFamily(long nativeHandle, String columnFamily);

    private static native void clearColumnFamily(long nativeHandle);
}
