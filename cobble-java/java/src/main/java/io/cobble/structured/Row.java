package io.cobble.structured;

/**
 * A typed row returned from a structured get or scan operation.
 *
 * <p>Each row contains a key and a fixed-size array of column values. Column values may be null
 * (absent), bytes, or list depending on the schema.
 */
public class Row {

    private final byte[] key;
    private final ColumnValue[] columns;

    Row(byte[] key, ColumnValue[] columns) {
        this.key = key;
        this.columns = columns;
    }

    /**
     * Converts a raw JNI result array into a typed Row.
     *
     * @param key the row key
     * @param raw JNI result: null | Object[] where each element is null | byte[] | byte[][]
     * @return typed Row, or null if raw is null
     */
    static Row fromRawColumns(byte[] key, Object[] raw) {
        if (raw == null) {
            return null;
        }
        ColumnValue[] columns = new ColumnValue[raw.length];
        for (int i = 0; i < raw.length; i++) {
            if (raw[i] == null) {
                columns[i] = null;
            } else if (raw[i] instanceof byte[]) {
                columns[i] = ColumnValue.ofBytes((byte[]) raw[i]);
            } else if (raw[i] instanceof byte[][]) {
                columns[i] = ColumnValue.ofList((byte[][]) raw[i]);
            }
        }
        return new Row(key, columns);
    }

    /** Returns the row key. */
    public byte[] getKey() {
        return key;
    }

    /** Returns the number of columns in this row. */
    public int getColumnCount() {
        return columns.length;
    }

    /**
     * Returns the typed column value at the given index, or null if absent.
     *
     * @param column the zero-based column index
     */
    public ColumnValue getColumnValue(int column) {
        if (column < 0 || column >= columns.length) {
            return null;
        }
        return columns[column];
    }

    /**
     * Returns the bytes value at the given column index, or null if absent.
     *
     * @param column the zero-based column index
     * @throws IllegalStateException if the column value is not bytes-type
     */
    public byte[] getBytes(int column) {
        ColumnValue v = getColumnValue(column);
        if (v == null) {
            return null;
        }
        return v.asBytes();
    }

    /**
     * Returns the list elements at the given column index, or null if absent.
     *
     * @param column the zero-based column index
     * @throws IllegalStateException if the column value is not list-type
     */
    public byte[][] getList(int column) {
        ColumnValue v = getColumnValue(column);
        if (v == null) {
            return null;
        }
        return v.asList();
    }
}
