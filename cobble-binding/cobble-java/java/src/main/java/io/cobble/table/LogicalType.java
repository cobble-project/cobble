package io.cobble.table;

import java.util.Objects;

/** Base class for Cobble's immutable cross-language logical types. */
public abstract class LogicalType {
    public enum Kind {
        BOOLEAN,
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT32,
        FLOAT64,
        DECIMAL,
        DATE,
        TIME,
        TIMESTAMP,
        STRING,
        BINARY,
        LIST,
        MAP,
        STRUCT,
        EXTENSION
    }

    private final Kind kind;
    private final boolean nullable;

    LogicalType(Kind kind, boolean nullable) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.nullable = nullable;
    }

    public final Kind kind() {
        return kind;
    }

    public final boolean isNullable() {
        return nullable;
    }

    abstract LogicalType withNullability(boolean nullable);

    public LogicalType nullable() {
        return withNullability(true);
    }

    public LogicalType notNull() {
        return withNullability(false);
    }

    void validate() {}

    final boolean isKeyCompatible() {
        if (nullable) return false;
        switch (kind) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case STRING:
            case BINARY:
                return true;
            default:
                return false;
        }
    }

    final boolean baseEquals(LogicalType other) {
        return other != null && kind == other.kind && nullable == other.nullable;
    }

    final int baseHash() {
        return Objects.hash(kind, nullable);
    }
}
