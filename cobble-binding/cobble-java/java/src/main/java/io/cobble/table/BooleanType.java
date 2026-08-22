package io.cobble.table;

public final class BooleanType extends PrimitiveType {
    public BooleanType(boolean nullable) {
        super(Kind.BOOLEAN, nullable);
    }

    @Override
    public BooleanType withNullability(boolean nullable) {
        return LogicalTypes.booleanType(nullable);
    }

    @Override
    public BooleanType nullable() {
        return withNullability(true);
    }

    @Override
    public BooleanType notNull() {
        return withNullability(false);
    }
}
