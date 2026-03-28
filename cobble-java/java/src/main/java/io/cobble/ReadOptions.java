package io.cobble;

/** Native-backed read options for point lookups. */
public final class ReadOptions extends NativeObject {
    private static final int[] DEFAULT_COLUMNS = new int[] {0};

    public ReadOptions() {
        super(loadAndCreateHandle());
    }

    /** Select one column to read. */
    public ReadOptions column(int columnIndex) {
        if (columnIndex < 0) {
            throw new IllegalArgumentException("column index must be >= 0");
        }
        setColumn(nativeHandle, columnIndex);
        return this;
    }

    /** Select multiple columns to read, preserving the given order. */
    public ReadOptions columns(int... columnIndices) {
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

    /** Create options for one selected column. */
    public static ReadOptions forColumn(int columnIndex) {
        return new ReadOptions().column(columnIndex);
    }

    /** Create options for selected columns. */
    public static ReadOptions forColumns(int... columnIndices) {
        return new ReadOptions().columns(columnIndices);
    }

    /** Create default options selecting column 0. */
    public static ReadOptions defaults() {
        return new ReadOptions().columns(DEFAULT_COLUMNS);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long createHandle();

    private static native void setColumn(long nativeHandle, int columnIndex);

    private static native void setColumns(long nativeHandle, int[] columns);

    private static long loadAndCreateHandle() {
        NativeLoader.load();
        return createHandle();
    }
}
