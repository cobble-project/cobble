package io.cobble.table;

import java.util.Objects;

public final class TimestampType extends LogicalType {
    private final int precision;
    private final TimestampKind timestampKind;

    TimestampType(int precision, TimestampKind timestampKind, boolean nullable) {
        super(Kind.TIMESTAMP, nullable);
        this.precision = precision;
        this.timestampKind = Objects.requireNonNull(timestampKind, "timestampKind");
        validate();
    }

    public int precision() {
        return precision;
    }

    public TimestampKind timestampKind() {
        return timestampKind;
    }

    @Override
    TimestampType withNullability(boolean nullable) {
        return new TimestampType(precision, timestampKind, nullable);
    }

    @Override
    public TimestampType nullable() {
        return withNullability(true);
    }

    @Override
    public TimestampType notNull() {
        return withNullability(false);
    }

    @Override
    void validate() {
        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("invalid timestamp precision");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TimestampType
                && baseEquals((LogicalType) o)
                && precision == ((TimestampType) o).precision
                && timestampKind == ((TimestampType) o).timestampKind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), precision, timestampKind);
    }
}
