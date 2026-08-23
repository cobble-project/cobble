package io.cobble.table;

public final class Int64Type extends PrimitiveType {
    Int64Type(boolean nullable) {
        super(Kind.INT64, nullable);
    }

    @Override
    Int64Type withNullability(boolean nullable) {
        return LogicalTypes.int64(nullable);
    }

    @Override
    public Int64Type nullable() {
        return withNullability(true);
    }

    @Override
    public Int64Type notNull() {
        return withNullability(false);
    }
}
