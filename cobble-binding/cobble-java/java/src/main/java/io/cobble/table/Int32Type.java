package io.cobble.table;

public final class Int32Type extends PrimitiveType {
    public Int32Type(boolean nullable) {
        super(Kind.INT32, nullable);
    }

    @Override
    public Int32Type withNullability(boolean nullable) {
        return LogicalTypes.int32(nullable);
    }

    @Override
    public Int32Type nullable() {
        return withNullability(true);
    }

    @Override
    public Int32Type notNull() {
        return withNullability(false);
    }
}
