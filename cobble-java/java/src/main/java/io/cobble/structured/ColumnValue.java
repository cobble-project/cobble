package io.cobble.structured;

/**
 * A typed column value in a structured row.
 *
 * <p>Each value is either a {@link ColumnType#BYTES} value (raw byte array) or a {@link
 * ColumnType#LIST} value (list of byte arrays / elements).
 *
 * <p>Use {@link #ofBytes(byte[])} and {@link #ofList(byte[][])} factory methods to create
 * instances.
 */
public abstract class ColumnValue {

    ColumnValue() {}

    /** Returns the column type of this value. */
    public abstract ColumnType getType();

    /** Returns true if this is a bytes-type value. */
    public boolean isBytes() {
        return getType() == ColumnType.BYTES;
    }

    /** Returns true if this is a list-type value. */
    public boolean isList() {
        return getType() == ColumnType.LIST;
    }

    /**
     * Returns the raw byte array for a bytes-type value.
     *
     * @throws IllegalStateException if this is not a bytes-type value
     */
    public byte[] asBytes() {
        throw new IllegalStateException("Not a BYTES value; actual type is " + getType());
    }

    /**
     * Returns the list of elements for a list-type value.
     *
     * @throws IllegalStateException if this is not a list-type value
     */
    public byte[][] asList() {
        throw new IllegalStateException("Not a LIST value; actual type is " + getType());
    }

    /** Creates a bytes-type column value. */
    public static ColumnValue ofBytes(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }
        return new BytesColumnValue(data);
    }

    /** Creates a list-type column value from an array of elements. */
    public static ColumnValue ofList(byte[][] elements) {
        if (elements == null) {
            throw new IllegalArgumentException("elements must not be null");
        }
        return new ListColumnValue(elements);
    }

    // ── Internal concrete types ──

    static final class BytesColumnValue extends ColumnValue {
        private final byte[] data;

        BytesColumnValue(byte[] data) {
            this.data = data;
        }

        @Override
        public ColumnType getType() {
            return ColumnType.BYTES;
        }

        @Override
        public byte[] asBytes() {
            return data;
        }
    }

    static final class ListColumnValue extends ColumnValue {
        private final byte[][] elements;

        ListColumnValue(byte[][] elements) {
            this.elements = elements;
        }

        @Override
        public ColumnType getType() {
            return ColumnType.LIST;
        }

        @Override
        public byte[][] asList() {
            return elements;
        }
    }
}
