package io.cobble.table;

public final class Int16Type extends PrimitiveType {
    private static final long serialVersionUID = 1L;

    Int16Type(boolean nullable) {
        super(Kind.INT16, nullable);
    }

    @Override
    Int16Type withNullability(boolean nullable) {
        return LogicalTypes.int16(nullable);
    }

    @Override
    public Int16Type nullable() {
        return withNullability(true);
    }

    @Override
    public Int16Type notNull() {
        return withNullability(false);
    }
}
