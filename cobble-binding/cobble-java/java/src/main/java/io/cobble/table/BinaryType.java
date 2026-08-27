package io.cobble.table;

public final class BinaryType extends PrimitiveType {
    private static final long serialVersionUID = 1L;

    BinaryType(boolean nullable) {
        super(Kind.BINARY, nullable);
    }

    @Override
    BinaryType withNullability(boolean nullable) {
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
