package io.cobble.table;

import java.util.Objects;

public final class StructType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private final RecordType recordType;

    StructType(RecordType recordType, boolean nullable) {
        super(Kind.STRUCT, nullable);
        this.recordType = Objects.requireNonNull(recordType, "recordType");
        validate();
    }

    public RecordType recordType() {
        return recordType;
    }

    @Override
    StructType withNullability(boolean nullable) {
        return new StructType(recordType, nullable);
    }

    @Override
    public StructType nullable() {
        return withNullability(true);
    }

    @Override
    public StructType notNull() {
        return withNullability(false);
    }

    @Override
    void validate() {
        recordType.validate();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StructType
                && baseEquals((LogicalType) o)
                && recordType.equals(((StructType) o).recordType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), recordType);
    }
}
