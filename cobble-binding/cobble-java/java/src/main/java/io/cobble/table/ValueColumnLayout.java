package io.cobble.table;

import java.util.Objects;

/** Rust-compiled mapping of one semantic field to one Cobble value column. */
final class ValueColumnLayout {
    private final long fieldId;
    private final int columnIndex;
    private final ValueStorage storage;

    ValueColumnLayout(long fieldId, int columnIndex, ValueStorage storage) {
        if (fieldId < 0 || fieldId > 0xffffffffL || columnIndex < 0 || columnIndex > 0xffff)
            throw new IllegalArgumentException("invalid value column layout");
        this.fieldId = fieldId;
        this.columnIndex = columnIndex;
        this.storage = Objects.requireNonNull(storage, "storage");
    }

    long fieldId() {
        return fieldId;
    }

    int columnIndex() {
        return columnIndex;
    }

    ValueStorage storage() {
        return storage;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueColumnLayout)) return false;
        ValueColumnLayout that = (ValueColumnLayout) other;
        return fieldId == that.fieldId
                && columnIndex == that.columnIndex
                && storage == that.storage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldId, columnIndex, storage);
    }
}
