package io.cobble.table;

public final class Float64Type extends PrimitiveType {
    Float64Type(boolean nullable) {
        super(Kind.FLOAT64, nullable);
    }

    @Override
    Float64Type withNullability(boolean nullable) {
        return LogicalTypes.float64(nullable);
    }

    @Override
    public Float64Type nullable() {
        return withNullability(true);
    }

    @Override
    public Float64Type notNull() {
        return withNullability(false);
    }
}
