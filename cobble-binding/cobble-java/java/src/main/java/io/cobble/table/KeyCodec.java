package io.cobble.table;

import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Ordered composite key codec. Callers own the destination {@link ByteBuffer}. */
public final class KeyCodec {
    private KeyCodec() {}

    public static byte[] encode(List<LogicalType> types, List<Value> values) {
        ByteBuffer out = ByteBuffer.allocate(encodedSize(types, values));
        encodeTo(types, values, out);
        return out.array();
    }

    public static int encodedSize(List<LogicalType> types, List<Value> values) {
        if (types.size() != values.size())
            throw new IllegalArgumentException("key field count does not match value count");
        int size = 0;
        for (int i = 0; i < types.size(); i++)
            size = checkedAdd(size, encodedSize(types.get(i), values.get(i)));
        return size;
    }

    public static void encodeTo(List<LogicalType> types, List<Value> values, ByteBuffer out) {
        int start = out.position();
        try {
            if (types.size() != values.size())
                throw new IllegalArgumentException("key field count does not match value count");
            for (int i = 0; i < types.size(); i++) encodeTo(types.get(i), values.get(i), out);
        } catch (RuntimeException | Error error) {
            ((Buffer) out).position(start);
            throw error;
        }
    }

    static int encodeToWithPrefix(
            List<LogicalType> types, List<Value> values, int prefixFields, ByteBuffer out) {
        int start = out.position();
        try {
            if (types.size() != values.size())
                throw new IllegalArgumentException("key field count does not match value count");
            if (prefixFields < 0 || prefixFields > types.size())
                throw new IllegalArgumentException("invalid key prefix field count");
            int prefixEnd = start;
            for (int i = 0; i < types.size(); i++) {
                encodeTo(types.get(i), values.get(i), out);
                if (i + 1 == prefixFields) prefixEnd = out.position();
            }
            return prefixEnd;
        } catch (RuntimeException | Error error) {
            ((Buffer) out).position(start);
            throw error;
        }
    }

    static int encodedSizeFromPositions(List<LogicalType> types, List<Value> row, int[] positions) {
        assert types.size() == positions.length;
        int size = 0;
        for (int i = 0; i < positions.length; i++) {
            int position = positions[i];
            assert position >= 0 && position < row.size();
            size = checkedAdd(size, encodedSize(types.get(i), row.get(position)));
        }
        return size;
    }

    static int encodeFromPositionsToWithPrefix(
            List<LogicalType> types,
            List<Value> row,
            int[] positions,
            int prefixFields,
            ByteBuffer out) {
        int start = out.position();
        try {
            assert types.size() == positions.length;
            assert prefixFields >= 0 && prefixFields <= positions.length;
            int prefixEnd = start;
            for (int i = 0; i < positions.length; i++) {
                int position = positions[i];
                assert position >= 0 && position < row.size();
                encodeTo(types.get(i), row.get(position), out);
                if (i + 1 == prefixFields) prefixEnd = out.position();
            }
            return prefixEnd;
        } catch (RuntimeException | Error error) {
            ((Buffer) out).position(start);
            throw error;
        }
    }

    public static void encodeTo(LogicalType type, Value value, ByteBuffer out) {
        int start = out.position();
        try {
            encodeScalar(type, value, out);
        } catch (RuntimeException | Error error) {
            ((Buffer) out).position(start);
            throw error;
        }
    }

    private static void encodeScalar(LogicalType type, Value value, ByteBuffer out) {
        if (!type.isKeyCompatible())
            throw new IllegalArgumentException("type cannot be used as a key");
        switch (type.kind()) {
            case BOOLEAN:
                out.put((byte) (bool(value) ? 1 : 0));
                return;
            case INT8:
                putOrdered(out, (Byte) require(value, Value.Kind.INT8), 1);
                return;
            case INT16:
                putOrdered(out, (Short) require(value, Value.Kind.INT16), 2);
                return;
            case INT32:
                putOrdered(out, (Integer) require(value, Value.Kind.INT32), 4);
                return;
            case INT64:
                putOrdered(out, (Long) require(value, Value.Kind.INT64), 8);
                return;
            case DECIMAL:
                {
                    Value.Decimal decimal = (Value.Decimal) require(value, Value.Kind.DECIMAL);
                    validateDecimal(type, decimal);
                    putOrdered(out, decimal.unscaled, decimalWidth(decimal(type).precision()));
                    return;
                }
            case DATE:
                putOrdered(out, (Integer) require(value, Value.Kind.DATE), 4);
                return;
            case TIME:
                {
                    long nanos = (Long) require(value, Value.Kind.TIME);
                    validateTime(time(type).precision(), nanos);
                    putOrdered(out, nanos, 8);
                    return;
                }
            case TIMESTAMP:
                {
                    Value.Timestamp timestamp =
                            (Value.Timestamp) require(value, Value.Kind.TIMESTAMP);
                    validateTimestamp(type, timestamp);
                    putOrdered(out, timestamp.seconds, 8);
                    putU32Be(out, timestamp.nanos);
                    return;
                }
            case STRING:
                escaped(out, (String) require(value, Value.Kind.STRING));
                return;
            case BINARY:
                escaped(out, (ByteBuffer) require(value, Value.Kind.BINARY));
                return;
            default:
                throw new IllegalArgumentException("type cannot be used as a key");
        }
    }

    public static List<Value> decode(List<LogicalType> types, ByteBuffer input) {
        return decodeOwned(types, input);
    }

    // Key-compatible types are scalars. Escaped binary is decoded into a fresh array, so every
    // returned value remains valid after the input buffer is reused.
    static List<Value> decodeOwned(List<LogicalType> types, ByteBuffer input) {
        List<Value> values = new ArrayList<Value>(types.size());
        for (LogicalType type : types) values.add(decodeOne(type, input));
        if (input.hasRemaining()) throw new IllegalArgumentException("trailing key bytes");
        return values;
    }

    public static Value decodeOne(LogicalType type, ByteBuffer input) {
        if (!type.isKeyCompatible())
            throw new IllegalArgumentException("type cannot be used as a key");
        switch (type.kind()) {
            case BOOLEAN:
                return Value.bool(readBool(input));
            case INT8:
                return Value.int8((byte) readOrderedLong(input, 1));
            case INT16:
                return Value.int16((short) readOrderedLong(input, 2));
            case INT32:
                return Value.int32((int) readOrderedLong(input, 4));
            case INT64:
                return Value.int64(readOrderedLong(input, 8));
            case DECIMAL:
                {
                    DecimalType typeValue = decimal(type);
                    BigInteger value =
                            readOrderedBigInteger(input, decimalWidth(typeValue.precision()));
                    Value.Decimal decimal =
                            new Value.Decimal(typeValue.precision(), typeValue.scale(), value);
                    validateDecimal(type, decimal);
                    return Value.decimal(typeValue.precision(), typeValue.scale(), value);
                }
            case DATE:
                return Value.date((int) readOrderedLong(input, 4));
            case TIME:
                {
                    long value = readOrderedLong(input, 8);
                    validateTime(time(type).precision(), value);
                    return Value.time(value);
                }
            case TIMESTAMP:
                {
                    long seconds = readOrderedLong(input, 8);
                    int nanos = getU32Be(input);
                    Value.Timestamp value =
                            new Value.Timestamp(
                                    timestamp(type).precision(),
                                    timestamp(type).timestampKind(),
                                    seconds,
                                    nanos);
                    validateTimestamp(type, value);
                    return Value.timestamp(
                            timestamp(type).precision(),
                            timestamp(type).timestampKind(),
                            seconds,
                            nanos);
                }
            case STRING:
                return Value.string(decodeUtf8(readEscaped(input)));
            case BINARY:
                return Value.binaryOwned(readEscaped(input));
            default:
                throw new IllegalArgumentException("type cannot be used as a key");
        }
    }

    static int encodedSize(LogicalType type, Value value) {
        if (!type.isKeyCompatible())
            throw new IllegalArgumentException("type cannot be used as a key");
        switch (type.kind()) {
            case BOOLEAN:
                bool(value);
                return 1;
            case INT8:
                require(value, Value.Kind.INT8);
                return 1;
            case INT16:
                require(value, Value.Kind.INT16);
                return 2;
            case INT32:
                require(value, Value.Kind.INT32);
                return 4;
            case INT64:
                require(value, Value.Kind.INT64);
                return 8;
            case DATE:
                require(value, Value.Kind.DATE);
                return 4;
            case TIME:
                validateTime(time(type).precision(), (Long) require(value, Value.Kind.TIME));
                return 8;
            case TIMESTAMP:
                validateTimestamp(type, (Value.Timestamp) require(value, Value.Kind.TIMESTAMP));
                return 12;
            case DECIMAL:
                validateDecimal(type, (Value.Decimal) require(value, Value.Kind.DECIMAL));
                return decimalWidth(decimal(type).precision());
            case STRING:
                return escapedSize((String) require(value, Value.Kind.STRING));
            case BINARY:
                return escapedSize((ByteBuffer) require(value, Value.Kind.BINARY));
            default:
                throw new IllegalArgumentException("type cannot be used as a key");
        }
    }

    static Object require(Value value, Value.Kind expected) {
        if (value.kind() != expected)
            throw new IllegalArgumentException("value does not match logical type");
        return value.raw();
    }

    static boolean bool(Value value) {
        return (Boolean) require(value, Value.Kind.BOOLEAN);
    }

    static int decimalWidth(int precision) {
        return precision <= 9 ? 4 : precision <= 18 ? 8 : 16;
    }

    static void validateDecimal(LogicalType type, Value.Decimal value) {
        DecimalType decimal = decimal(type);
        if (value.precision != decimal.precision()
                || value.scale != decimal.scale()
                || value.unscaled.abs().compareTo(BigInteger.TEN.pow(decimal.precision())) >= 0)
            throw new IllegalArgumentException("invalid decimal value");
    }

    static void validateTime(int precision, long value) {
        if (precision < 0
                || precision > 9
                || value < 0
                || value >= 86400000000000L
                || value % pow10(9 - precision) != 0)
            throw new IllegalArgumentException("invalid time value");
    }

    static void validateTimestamp(LogicalType type, Value.Timestamp value) {
        TimestampType timestamp = timestamp(type);
        if (value.precision != timestamp.precision()
                || value.kind != timestamp.timestampKind()
                || value.nanos < 0
                || value.nanos >= 1000000000
                || value.nanos % pow10(9 - timestamp.precision()) != 0)
            throw new IllegalArgumentException("invalid timestamp value");
    }

    static int pow10(int exponent) {
        int result = 1;
        while (exponent-- > 0) result *= 10;
        return result;
    }

    static DecimalType decimal(LogicalType type) {
        return (DecimalType) type;
    }

    static TimeType time(LogicalType type) {
        return (TimeType) type;
    }

    static TimestampType timestamp(LogicalType type) {
        return (TimestampType) type;
    }

    static int checkedAdd(int left, int right) {
        if (right < 0 || left > Integer.MAX_VALUE - right)
            throw new IllegalArgumentException("encoded value exceeds addressable memory");
        return left + right;
    }

    private static int escapedSize(ByteBuffer value) {
        int size = 2;
        ByteBuffer input = value.duplicate();
        while (input.hasRemaining()) size = checkedAdd(size, input.get() == 0 ? 2 : 1);
        return size;
    }

    private static int escapedSize(String value) {
        final int[] size = {2};
        forEachUtf8(
                value,
                new ByteConsumer() {
                    public void accept(byte current) {
                        size[0] = checkedAdd(size[0], current == 0 ? 2 : 1);
                    }
                });
        return size[0];
    }

    interface ByteConsumer {
        void accept(byte value);
    }

    private static void escaped(ByteBuffer out, ByteBuffer value) {
        ByteBuffer input = value.duplicate();
        while (input.hasRemaining()) {
            byte current = input.get();
            out.put(current);
            if (current == 0) out.put((byte) 0xff);
        }
        out.put((byte) 0).put((byte) 0);
    }

    private static void escaped(ByteBuffer out, String value) {
        forEachUtf8(
                value,
                new ByteConsumer() {
                    public void accept(byte current) {
                        out.put(current);
                        if (current == 0) out.put((byte) 0xff);
                    }
                });
        out.put((byte) 0).put((byte) 0);
    }

    private static byte[] readEscaped(ByteBuffer input) {
        ByteBuffer scan = input.duplicate();
        int size = 0;
        boolean terminated = false;
        while (scan.hasRemaining()) {
            byte value = scan.get();
            if (value != 0) {
                size++;
                continue;
            }
            if (!scan.hasRemaining())
                throw new IllegalArgumentException("unterminated escaped key");
            int next = scan.get() & 0xff;
            if (next == 0) {
                terminated = true;
                break;
            }
            if (next != 0xff) throw new IllegalArgumentException("invalid key escape");
            size++;
        }
        if (!terminated) throw new IllegalArgumentException("unterminated escaped key");
        byte[] out = new byte[size];
        int index = 0;
        while (true) {
            byte value = input.get();
            if (value != 0) {
                out[index++] = value;
                continue;
            }
            int next = input.get() & 0xff;
            if (next == 0) return out;
            out[index++] = 0;
        }
    }

    static void forEachUtf8(String value, ByteConsumer consumer) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) consumer.accept((byte) c);
            else if (c < 0x800) {
                consumer.accept((byte) (0xc0 | (c >>> 6)));
                consumer.accept((byte) (0x80 | (c & 0x3f)));
            } else if (Character.isSurrogate(c)) {
                if (!Character.isHighSurrogate(c)
                        || i + 1 == value.length()
                        || !Character.isLowSurrogate(value.charAt(i + 1)))
                    throw new IllegalArgumentException("invalid UTF-16 string");
                int codePoint = Character.toCodePoint(c, value.charAt(++i));
                consumer.accept((byte) (0xf0 | (codePoint >>> 18)));
                consumer.accept((byte) (0x80 | ((codePoint >>> 12) & 0x3f)));
                consumer.accept((byte) (0x80 | ((codePoint >>> 6) & 0x3f)));
                consumer.accept((byte) (0x80 | (codePoint & 0x3f)));
            } else {
                consumer.accept((byte) (0xe0 | (c >>> 12)));
                consumer.accept((byte) (0x80 | ((c >>> 6) & 0x3f)));
                consumer.accept((byte) (0x80 | (c & 0x3f)));
            }
        }
    }

    static int utf8Size(String value) {
        final int[] size = {0};
        forEachUtf8(
                value,
                new ByteConsumer() {
                    public void accept(byte ignored) {
                        size[0]++;
                    }
                });
        return size[0];
    }

    static void putUtf8(ByteBuffer out, String value) {
        forEachUtf8(
                value,
                new ByteConsumer() {
                    public void accept(byte current) {
                        out.put(current);
                    }
                });
    }

    private static String decodeUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (java.nio.charset.CharacterCodingException error) {
            throw new IllegalArgumentException("invalid UTF-8 key", error);
        }
    }

    static void putOrdered(ByteBuffer out, long value, int width) {
        for (int shift = (width - 1) * 8; shift >= 0; shift -= 8) {
            byte current = (byte) (value >>> shift);
            out.put(shift == (width - 1) * 8 ? (byte) (current ^ 0x80) : current);
        }
    }

    static long readOrderedLong(ByteBuffer input, int width) {
        requireRemaining(input, width);
        long value = 0;
        for (int index = 0; index < width; index++) {
            int current = input.get() & 0xff;
            if (index == 0) current ^= 0x80;
            value = (value << 8) | current;
        }
        int shift = (8 - width) * 8;
        return value << shift >> shift;
    }

    static void putOrdered(ByteBuffer out, BigInteger value, int width) {
        byte[] raw = fixed(value, width);
        raw[0] ^= (byte) 0x80;
        out.put(raw);
    }

    static BigInteger readOrderedBigInteger(ByteBuffer input, int width) {
        requireRemaining(input, width);
        byte[] raw = new byte[width];
        input.get(raw);
        raw[0] ^= (byte) 0x80;
        return new BigInteger(raw);
    }

    private static byte[] fixed(BigInteger value, int width) {
        byte[] raw = value.toByteArray();
        if (raw.length > width
                && !(raw.length == width + 1 && raw[0] == (value.signum() < 0 ? (byte) 0xff : 0)))
            throw new IllegalArgumentException("integer does not fit logical width");
        byte[] fixed = new byte[width];
        java.util.Arrays.fill(fixed, value.signum() < 0 ? (byte) 0xff : 0);
        System.arraycopy(
                raw,
                Math.max(0, raw.length - width),
                fixed,
                Math.max(0, width - raw.length),
                Math.min(width, raw.length));
        return fixed;
    }

    private static boolean readBool(ByteBuffer input) {
        requireRemaining(input, 1);
        byte value = input.get();
        if (value != 0 && value != 1) throw new IllegalArgumentException("invalid boolean byte");
        return value == 1;
    }

    static void requireRemaining(ByteBuffer input, int length) {
        if (input.remaining() < length) throw new IllegalArgumentException("truncated value");
    }

    static void putU32Be(ByteBuffer out, int value) {
        out.put((byte) (value >>> 24))
                .put((byte) (value >>> 16))
                .put((byte) (value >>> 8))
                .put((byte) value);
    }

    static int getU32Be(ByteBuffer input) {
        requireRemaining(input, 4);
        return ((input.get() & 255) << 24)
                | ((input.get() & 255) << 16)
                | ((input.get() & 255) << 8)
                | (input.get() & 255);
    }
}
