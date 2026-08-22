package io.cobble.table;

public final class Float32Type extends PrimitiveType {
    public Float32Type(boolean nullable) {
        super(Kind.FLOAT32, nullable);
    }

    @Override
    public Float32Type withNullability(boolean nullable) {
        return LogicalTypes.float32(nullable);
    }

    @Override
    public Float32Type nullable() {
        return withNullability(true);
    }

    @Override
    public Float32Type notNull() {
        return withNullability(false);
    }
}
