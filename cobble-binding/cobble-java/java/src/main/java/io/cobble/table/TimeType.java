package io.cobble.table;

import java.util.Objects;

public final class TimeType extends LogicalType {
    private final int precision;

    public TimeType(int precision, boolean nullable) {
        super(Kind.TIME, nullable);
        this.precision = precision;
        validate();
    }

    public int precision() {
        return precision;
    }

    @Override
    public TimeType withNullability(boolean nullable) {
        return new TimeType(precision, nullable);
    }

    @Override
    public TimeType nullable() {
        return withNullability(true);
    }

    @Override
    public TimeType notNull() {
        return withNullability(false);
    }

    @Override
    public void validate() {
        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("invalid time precision");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimeType
                && baseEquals((LogicalType) o)
                && precision == ((TimeType) o).precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), precision);
    }
}
