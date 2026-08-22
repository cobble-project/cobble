package io.cobble.table;

import java.util.Objects;

public final class MapType extends LogicalType {
    private final LogicalType keyType, valueType;

    public MapType(LogicalType keyType, LogicalType valueType, boolean nullable) {
        super(Kind.MAP, nullable);
        this.keyType = Objects.requireNonNull(keyType, "keyType");
        this.valueType = Objects.requireNonNull(valueType, "valueType");
        validate();
    }

    public LogicalType keyType() {
        return keyType;
    }

    public LogicalType valueType() {
        return valueType;
    }

    @Override
    public MapType withNullability(boolean nullable) {
        return new MapType(keyType, valueType, nullable);
    }

    @Override
    public MapType nullable() {
        return withNullability(true);
    }

    @Override
    public MapType notNull() {
        return withNullability(false);
    }

    @Override
    public void validate() {
        if (keyType.isNullable())
            throw new IllegalArgumentException("map keys must not be nullable");
        keyType.validate();
        valueType.validate();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof MapType
                && baseEquals((LogicalType) o)
                && keyType.equals(((MapType) o).keyType)
                && valueType.equals(((MapType) o).valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), keyType, valueType);
    }
}
