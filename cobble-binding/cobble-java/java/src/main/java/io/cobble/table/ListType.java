package io.cobble.table;

import java.util.Objects;

public final class ListType extends LogicalType {
    private static final long serialVersionUID = 1L;

    private final LogicalType elementType;

    ListType(LogicalType elementType, boolean nullable) {
        super(Kind.LIST, nullable);
        this.elementType = Objects.requireNonNull(elementType, "elementType");
        validate();
    }

    public LogicalType elementType() {
        return elementType;
    }

    @Override
    ListType withNullability(boolean nullable) {
        return new ListType(elementType, nullable);
    }

    @Override
    public ListType nullable() {
        return withNullability(true);
    }

    @Override
    public ListType notNull() {
        return withNullability(false);
    }

    @Override
    void validate() {
        elementType.validate();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ListType
                && baseEquals((LogicalType) o)
                && elementType.equals(((ListType) o).elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHash(), elementType);
    }
}
