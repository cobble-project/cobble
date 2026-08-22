package io.cobble.table;

import java.util.Objects;

public final class DecimalType extends LogicalType {
    private final int precision, scale;

    public DecimalType(int precision, int scale, boolean nullable) {
        super(Kind.DECIMAL, nullable);
        this.precision = precision;
        this.scale = scale;
        validate();
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    @Override
    public DecimalType withNullability(boolean nullable) {
        return new DecimalType(precision, scale, nullable);
    }

    @Override
    public DecimalType nullable() {
        return withNullability(true);
    }

    @Override
    public DecimalType notNull() {
        return withNullability(false);
    }

    @Override
    public void validate() {
        if (precision < 1 || precision > 38 || scale < 0 || scale > precision)
            throw new IllegalArgumentException("invalid decimal precision/scale");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof DecimalType
                && baseEquals((LogicalType) o)
                && precision == ((DecimalType) o).precision
                && scale == ((DecimalType) o).scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), precision, scale);
    }
}
