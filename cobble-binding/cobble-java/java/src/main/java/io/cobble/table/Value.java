package io.cobble.table;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Dynamic value used by the schema-directed table codec. */
public final class Value {
    public enum Kind {
        NULL,
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

    public static final class Decimal {
        public final int precision, scale;
        public final java.math.BigInteger unscaled;

        public Decimal(int precision, int scale, java.math.BigInteger unscaled) {
            this.precision = precision;
            this.scale = scale;
            this.unscaled = Objects.requireNonNull(unscaled, "unscaled");
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Decimal
                    && precision == ((Decimal) other).precision
                    && scale == ((Decimal) other).scale
                    && unscaled.equals(((Decimal) other).unscaled);
        }

        @Override
        public int hashCode() {
            return Objects.hash(precision, scale, unscaled);
        }
    }

    public static final class Timestamp {
        public final int precision;
        public final TimestampKind kind;
        public final long seconds;
        public final int nanos;

        public Timestamp(int precision, TimestampKind kind, long seconds, int nanos) {
            this.precision = precision;
            this.kind = Objects.requireNonNull(kind, "kind");
            this.seconds = seconds;
            this.nanos = nanos;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Timestamp
                    && precision == ((Timestamp) other).precision
                    && kind == ((Timestamp) other).kind
                    && seconds == ((Timestamp) other).seconds
                    && nanos == ((Timestamp) other).nanos;
        }

        @Override
        public int hashCode() {
            return Objects.hash(precision, kind, seconds, nanos);
        }
    }

    public static final class Extension {
        public final String typeId;
        public final Value value;

        public Extension(String typeId, Value value) {
            this.typeId = Objects.requireNonNull(typeId, "typeId");
            this.value = Objects.requireNonNull(value, "value");
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Extension
                    && typeId.equals(((Extension) other).typeId)
                    && value.equals(((Extension) other).value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(typeId, value);
        }
    }

    private final Kind kind;
    private final Object value;

    private Value(Kind kind, Object value) {
        this.kind = kind;
        this.value = value;
    }

    public static Value nullValue() {
        return new Value(Kind.NULL, null);
    }

    public static Value bool(boolean value) {
        return new Value(Kind.BOOLEAN, value);
    }

    public static Value int8(byte value) {
        return new Value(Kind.INT8, value);
    }

    public static Value int16(short value) {
        return new Value(Kind.INT16, value);
    }

    public static Value int32(int value) {
        return new Value(Kind.INT32, value);
    }

    public static Value int64(long value) {
        return new Value(Kind.INT64, value);
    }

    public static Value float32(float value) {
        return new Value(Kind.FLOAT32, value);
    }

    public static Value float64(double value) {
        return new Value(Kind.FLOAT64, value);
    }

    public static Value decimal(int precision, int scale, java.math.BigInteger unscaled) {
        return new Value(Kind.DECIMAL, new Decimal(precision, scale, unscaled));
    }

    public static Value date(int value) {
        return new Value(Kind.DATE, value);
    }

    public static Value time(long value) {
        return new Value(Kind.TIME, value);
    }

    public static Value timestamp(int precision, TimestampKind kind, long seconds, int nanos) {
        return new Value(Kind.TIMESTAMP, new Timestamp(precision, kind, seconds, nanos));
    }

    public static Value string(String value) {
        return new Value(Kind.STRING, Objects.requireNonNull(value, "value"));
    }

    /**
     * The buffer is retained as a read-only slice; its bytes are borrowed from its backing storage.
     */
    public static Value binary(ByteBuffer value) {
        return new Value(Kind.BINARY, value.asReadOnlyBuffer().slice());
    }

    public static Value binary(byte[] value) {
        return binary(ByteBuffer.wrap(java.util.Arrays.copyOf(value, value.length)));
    }

    static Value binaryOwned(byte[] value) {
        return new Value(Kind.BINARY, ByteBuffer.wrap(value).asReadOnlyBuffer());
    }

    public static Value list(List<Value> value) {
        return new Value(Kind.LIST, Collections.unmodifiableList(new ArrayList<Value>(value)));
    }

    public static Value map(List<Map.Entry<Value, Value>> value) {
        List<Map.Entry<Value, Value>> copy = new ArrayList<Map.Entry<Value, Value>>(value.size());
        for (Map.Entry<Value, Value> entry : value)
            copy.add(
                    new java.util.AbstractMap.SimpleImmutableEntry<Value, Value>(
                            entry.getKey(), entry.getValue()));
        return new Value(Kind.MAP, Collections.unmodifiableList(copy));
    }

    public static Value struct(List<Value> value) {
        return new Value(Kind.STRUCT, Collections.unmodifiableList(new ArrayList<Value>(value)));
    }

    public static Value extension(String typeId, Value value) {
        return new Value(Kind.EXTENSION, new Extension(typeId, value));
    }

    public Kind kind() {
        return kind;
    }

    public Object raw() {
        return kind == Kind.BINARY ? ((ByteBuffer) value).asReadOnlyBuffer() : value;
    }

    @SuppressWarnings("unchecked")
    <T> T cast() {
        return (T) value;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Value) || kind != ((Value) other).kind) return false;
        Object that = ((Value) other).value;
        return kind == Kind.BINARY
                ? equalBytes((ByteBuffer) value, (ByteBuffer) that)
                : Objects.equals(value, that);
    }

    @Override
    public int hashCode() {
        return kind == Kind.BINARY
                ? 31 * kind.hashCode() + bytesHash((ByteBuffer) value)
                : Objects.hash(kind, value);
    }

    private static boolean equalBytes(ByteBuffer left, ByteBuffer right) {
        return left.duplicate().equals(right.duplicate());
    }

    private static int bytesHash(ByteBuffer value) {
        int hash = 1;
        ByteBuffer duplicate = value.duplicate();
        while (duplicate.hasRemaining()) hash = 31 * hash + duplicate.get();
        return hash;
    }
}
