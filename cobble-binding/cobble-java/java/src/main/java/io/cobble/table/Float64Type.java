package io.cobble.table;

public final class Float64Type extends PrimitiveType {
    public Float64Type(boolean nullable) {
        super(Kind.FLOAT64, nullable);
    }

    @Override
    public Float64Type withNullability(boolean nullable) {
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
