package io.cobble.table;

public final class BinaryType extends PrimitiveType {
    public BinaryType(boolean nullable) {
        super(Kind.BINARY, nullable);
    }

    @Override
    public BinaryType withNullability(boolean nullable) {
        return LogicalTypes.binary(nullable);
    }

    @Override
    public BinaryType nullable() {
        return withNullability(true);
    }

    @Override
    public BinaryType notNull() {
        return withNullability(false);
    }
}
