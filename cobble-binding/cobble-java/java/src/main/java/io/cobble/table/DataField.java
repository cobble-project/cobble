package io.cobble.table;

import java.util.Objects;

/** Named logical field with a stable field id. */
public final class DataField {
    private final long id;
    private final String name;
    private final LogicalType logicalType;

    public DataField(long id, String name, LogicalType logicalType) {
        this.id = id;
        this.name = Objects.requireNonNull(name, "name");
        this.logicalType = Objects.requireNonNull(logicalType, "logicalType");
        if (id < 0 || id > 0xffffffffL || name.trim().isEmpty())
            throw new IllegalArgumentException("invalid field id or name");
        logicalType.validate();
    }

    public long id() {
        return id;
    }

    public String name() {
        return name;
    }

    public LogicalType logicalType() {
        return logicalType;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DataField)) return false;
        DataField that = (DataField) other;
        return id == that.id && name.equals(that.name) && logicalType.equals(that.logicalType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, logicalType);
    }
}
