package io.cobble.table;

abstract class PrimitiveType extends LogicalType {
    private static final long serialVersionUID = 1L;

    PrimitiveType(Kind kind, boolean nullable) {
        super(kind, nullable);
    }

    @Override
    public boolean equals(Object other) {
        return other != null && getClass() == other.getClass() && baseEquals((LogicalType) other);
    }

    @Override
    public int hashCode() {
        return baseHash();
    }
}
