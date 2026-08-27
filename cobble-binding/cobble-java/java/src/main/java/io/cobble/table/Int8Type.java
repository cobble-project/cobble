package io.cobble.table;

public final class Int8Type extends PrimitiveType {
    private static final long serialVersionUID = 1L;

    Int8Type(boolean nullable) {
        super(Kind.INT8, nullable);
    }

    @Override
    Int8Type withNullability(boolean nullable) {
        return LogicalTypes.int8(nullable);
    }

    @Override
    public Int8Type nullable() {
        return withNullability(true);
    }

    @Override
    public Int8Type notNull() {
        return withNullability(false);
    }
}
