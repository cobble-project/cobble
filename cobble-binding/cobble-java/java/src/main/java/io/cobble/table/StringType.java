package io.cobble.table;

public final class StringType extends PrimitiveType {
    StringType(boolean nullable) {
        super(Kind.STRING, nullable);
    }

    @Override
    StringType withNullability(boolean nullable) {
        return LogicalTypes.string(nullable);
    }

    @Override
    public StringType nullable() {
        return withNullability(true);
    }

    @Override
    public StringType notNull() {
        return withNullability(false);
    }
}
