package io.cobble.table;

public final class DateType extends PrimitiveType {
    DateType(boolean nullable) {
        super(Kind.DATE, nullable);
    }

    @Override
    DateType withNullability(boolean nullable) {
        return LogicalTypes.date(nullable);
    }

    @Override
    public DateType nullable() {
        return withNullability(true);
    }

    @Override
    public DateType notNull() {
        return withNullability(false);
    }
}
