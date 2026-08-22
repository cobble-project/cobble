package io.cobble.table;

import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Schema-directed value codec. Its primary API writes into caller-owned {@link ByteBuffer}s. */
public final class ValueCodec {
    private ValueCodec() {}

    public static byte[] encode(LogicalType type, Value value) {
        ByteBuffer out = ByteBuffer.allocate(encodedSize(type, value));
        encodeTo(type, value, out);
        return out.array();
    }

    public static void encodeTo(LogicalType type, Value value, ByteBuffer out) {
        int start = out.position();
        try {
            type.validate();
            encodeValue(type, value, out);
        } catch (RuntimeException | Error error) {
            ((Buffer) out).position(start);
            throw error;
        }
    }

    public static int encodedSize(LogicalType type, Value value) {
        type.validate();
        return sizeValue(type, value);
    }

    public static Value decode(LogicalType type, ByteBuffer input) {
        type.validate();
        Value value = decodeValue(type, input);
        if (input.hasRemaining()) throw new IllegalArgumentException("trailing value bytes");
        return value;
    }

    private static void encodeValue(LogicalType type, Value value, ByteBuffer out) {
        if (type.isNullable()) {
            if (value.kind() == Value.Kind.NULL) {
                out.put((byte) 0);
                return;
            }
            out.put((byte) 1);
        } else if (value.kind() == Value.Kind.NULL)
            throw new IllegalArgumentException("null does not match non-null type");
        encodeNonNull(type, value, out);
    }

    private static void encodeNonNull(LogicalType type, Value value, ByteBuffer out) {
        switch (type.kind()) {
            case BOOLEAN:
                out.put((byte) (KeyCodec.bool(value) ? 1 : 0));
                return;
            case INT8:
                out.put((Byte) KeyCodec.require(value, Value.Kind.INT8));
                return;
            case INT16:
                putI16Le(out, (Short) KeyCodec.require(value, Value.Kind.INT16));
                return;
            case INT32:
                putI32Le(out, (Integer) KeyCodec.require(value, Value.Kind.INT32));
                return;
            case INT64:
                putI64Le(out, (Long) KeyCodec.require(value, Value.Kind.INT64));
                return;
            case FLOAT32:
                putI32Le(
                        out,
                        Float.floatToRawIntBits(
                                (Float) KeyCodec.require(value, Value.Kind.FLOAT32)));
                return;
            case FLOAT64:
                putI64Le(
                        out,
                        Double.doubleToRawLongBits(
                                (Double) KeyCodec.require(value, Value.Kind.FLOAT64)));
                return;
            case DECIMAL:
                {
                    Value.Decimal decimal =
                            (Value.Decimal) KeyCodec.require(value, Value.Kind.DECIMAL);
                    KeyCodec.validateDecimal(type, decimal);
                    putFixedLe(
                            out,
                            decimal.unscaled,
                            KeyCodec.decimalWidth(decimal(type).precision()));
                    return;
                }
            case DATE:
                putI32Le(out, (Integer) KeyCodec.require(value, Value.Kind.DATE));
                return;
            case TIME:
                {
                    long nanos = (Long) KeyCodec.require(value, Value.Kind.TIME);
                    KeyCodec.validateTime(time(type).precision(), nanos);
                    putI64Le(out, nanos);
                    return;
                }
            case TIMESTAMP:
                {
                    Value.Timestamp timestamp =
                            (Value.Timestamp) KeyCodec.require(value, Value.Kind.TIMESTAMP);
                    KeyCodec.validateTimestamp(type, timestamp);
                    putI64Le(out, timestamp.seconds);
                    putI32Le(out, timestamp.nanos);
                    return;
                }
            case STRING:
                KeyCodec.putUtf8(out, (String) KeyCodec.require(value, Value.Kind.STRING));
                return;
            case BINARY:
                out.put(((ByteBuffer) KeyCodec.require(value, Value.Kind.BINARY)).duplicate());
                return;
            case LIST:
                {
                    requireKind(value, Value.Kind.LIST);
                    List<Value> values = value.cast();
                    putU32Le(out, values.size());
                    for (Value child : values) putFramed(list(type).elementType(), child, out);
                    return;
                }
            case MAP:
                {
                    requireKind(value, Value.Kind.MAP);
                    List<Map.Entry<Value, Value>> entries = value.cast();
                    putU32Le(out, entries.size());
                    for (Map.Entry<Value, Value> entry : entries) {
                        putFramed(map(type).keyType(), entry.getKey(), out);
                        putFramed(map(type).valueType(), entry.getValue(), out);
                    }
                    return;
                }
            case STRUCT:
                {
                    requireKind(value, Value.Kind.STRUCT);
                    List<Value> values = value.cast();
                    if (values.size() != struct(type).recordType().fields().size())
                        throw new IllegalArgumentException(
                                "struct field count does not match schema");
                    for (int i = 0; i < values.size(); i++)
                        putFramed(
                                struct(type).recordType().fields().get(i).logicalType(),
                                values.get(i),
                                out);
                    return;
                }
            case EXTENSION:
                {
                    requireKind(value, Value.Kind.EXTENSION);
                    Value.Extension extension = value.cast();
                    if (!extension(type).typeId().equals(extension.typeId))
                        throw new IllegalArgumentException(
                                "extension type id does not match schema");
                    encodeValue(extension(type).physicalType(), extension.value, out);
                    return;
                }
            default:
                throw new AssertionError(type.kind());
        }
    }

    private static Value decodeValue(LogicalType type, ByteBuffer input) {
        if (!type.isNullable()) return decodeNonNull(type, input);
        KeyCodec.requireRemaining(input, 1);
        byte marker = input.get();
        if (marker == 0) {
            if (input.hasRemaining()) throw new IllegalArgumentException("null has trailing bytes");
            return Value.nullValue();
        }
        if (marker != 1) throw new IllegalArgumentException("invalid nullable marker");
        return decodeNonNull(type, input);
    }

    private static Value decodeNonNull(LogicalType type, ByteBuffer input) {
        switch (type.kind()) {
            case BOOLEAN:
                return Value.bool(readBoolExact(input));
            case INT8:
                {
                    exact(input, 1);
                    return Value.int8(input.get());
                }
            case INT16:
                {
                    exact(input, 2);
                    return Value.int16(getI16Le(input));
                }
            case INT32:
                {
                    exact(input, 4);
                    return Value.int32(getI32Le(input));
                }
            case INT64:
                {
                    exact(input, 8);
                    return Value.int64(getI64Le(input));
                }
            case FLOAT32:
                {
                    exact(input, 4);
                    return Value.float32(Float.intBitsToFloat(getI32Le(input)));
                }
            case FLOAT64:
                {
                    exact(input, 8);
                    return Value.float64(Double.longBitsToDouble(getI64Le(input)));
                }
            case DECIMAL:
                {
                    DecimalType typeValue = decimal(type);
                    int width = KeyCodec.decimalWidth(typeValue.precision());
                    exact(input, width);
                    BigInteger unscaled = getFixedLe(input, width);
                    Value.Decimal value =
                            new Value.Decimal(typeValue.precision(), typeValue.scale(), unscaled);
                    KeyCodec.validateDecimal(type, value);
                    return Value.decimal(typeValue.precision(), typeValue.scale(), unscaled);
                }
            case DATE:
                {
                    exact(input, 4);
                    return Value.date(getI32Le(input));
                }
            case TIME:
                {
                    exact(input, 8);
                    long value = getI64Le(input);
                    KeyCodec.validateTime(time(type).precision(), value);
                    return Value.time(value);
                }
            case TIMESTAMP:
                {
                    exact(input, 12);
                    long seconds = getI64Le(input);
                    int nanos = getI32Le(input);
                    Value.Timestamp value =
                            new Value.Timestamp(
                                    timestamp(type).precision(),
                                    timestamp(type).timestampKind(),
                                    seconds,
                                    nanos);
                    KeyCodec.validateTimestamp(type, value);
                    return Value.timestamp(
                            timestamp(type).precision(),
                            timestamp(type).timestampKind(),
                            seconds,
                            nanos);
                }
            case STRING:
                return Value.string(readUtf8(input));
            case BINARY:
                {
                    ByteBuffer slice = input.asReadOnlyBuffer().slice();
                    ((Buffer) input).position(input.limit());
                    return Value.binary(slice);
                }
            case LIST:
                {
                    int count = getCount(input);
                    if (count > input.remaining() / 4)
                        throw new IllegalArgumentException("list count exceeds encoded frames");
                    List<Value> values = new ArrayList<Value>(count);
                    for (int i = 0; i < count; i++)
                        values.add(readFramed(list(type).elementType(), input));
                    return finish(input, Value.list(values));
                }
            case MAP:
                {
                    int count = getCount(input);
                    if (count > input.remaining() / 8)
                        throw new IllegalArgumentException("map count exceeds encoded frames");
                    List<Map.Entry<Value, Value>> entries =
                            new ArrayList<Map.Entry<Value, Value>>(count);
                    for (int i = 0; i < count; i++)
                        entries.add(
                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                        readFramed(map(type).keyType(), input),
                                        readFramed(map(type).valueType(), input)));
                    return finish(input, Value.map(entries));
                }
            case STRUCT:
                {
                    List<Value> values =
                            new ArrayList<Value>(struct(type).recordType().fields().size());
                    for (DataField field : struct(type).recordType().fields())
                        values.add(readFramed(field.logicalType(), input));
                    return finish(input, Value.struct(values));
                }
            case EXTENSION:
                return Value.extension(
                        extension(type).typeId(),
                        decodeValue(extension(type).physicalType(), input));
            default:
                throw new AssertionError(type.kind());
        }
    }

    private static Value finish(ByteBuffer input, Value value) {
        if (input.hasRemaining()) throw new IllegalArgumentException("trailing container bytes");
        return value;
    }

    private static void requireKind(Value value, Value.Kind expected) {
        if (value.kind() != expected)
            throw new IllegalArgumentException("value does not match logical type");
    }

    private static DecimalType decimal(LogicalType type) {
        return (DecimalType) type;
    }

    private static TimeType time(LogicalType type) {
        return (TimeType) type;
    }

    private static TimestampType timestamp(LogicalType type) {
        return (TimestampType) type;
    }

    private static ListType list(LogicalType type) {
        return (ListType) type;
    }

    private static MapType map(LogicalType type) {
        return (MapType) type;
    }

    private static StructType struct(LogicalType type) {
        return (StructType) type;
    }

    private static ExtensionType extension(LogicalType type) {
        return (ExtensionType) type;
    }

    private static int sizeValue(LogicalType type, Value value) {
        if (type.isNullable() && value.kind() == Value.Kind.NULL) return 1;
        if (!type.isNullable() && value.kind() == Value.Kind.NULL)
            throw new IllegalArgumentException("null does not match non-null type");
        int marker = type.isNullable() ? 1 : 0;
        switch (type.kind()) {
            case BOOLEAN:
            case INT8:
                KeyCodec.require(
                        value,
                        type.kind() == LogicalType.Kind.BOOLEAN
                                ? Value.Kind.BOOLEAN
                                : Value.Kind.INT8);
                return KeyCodec.checkedAdd(marker, 1);
            case INT16:
                KeyCodec.require(value, Value.Kind.INT16);
                return KeyCodec.checkedAdd(marker, 2);
            case INT32:
            case DATE:
                KeyCodec.require(
                        value,
                        type.kind() == LogicalType.Kind.INT32 ? Value.Kind.INT32 : Value.Kind.DATE);
                return KeyCodec.checkedAdd(marker, 4);
            case INT64:
                KeyCodec.require(value, Value.Kind.INT64);
                return KeyCodec.checkedAdd(marker, 8);
            case TIME:
                {
                    long nanos = (Long) KeyCodec.require(value, Value.Kind.TIME);
                    KeyCodec.validateTime(time(type).precision(), nanos);
                    return KeyCodec.checkedAdd(marker, 8);
                }
            case FLOAT32:
                KeyCodec.require(value, Value.Kind.FLOAT32);
                return KeyCodec.checkedAdd(marker, 4);
            case FLOAT64:
                KeyCodec.require(value, Value.Kind.FLOAT64);
                return KeyCodec.checkedAdd(marker, 8);
            case DECIMAL:
                {
                    Value.Decimal decimal =
                            (Value.Decimal) KeyCodec.require(value, Value.Kind.DECIMAL);
                    KeyCodec.validateDecimal(type, decimal);
                    return KeyCodec.checkedAdd(
                            marker, KeyCodec.decimalWidth(decimal(type).precision()));
                }
            case TIMESTAMP:
                {
                    Value.Timestamp timestamp =
                            (Value.Timestamp) KeyCodec.require(value, Value.Kind.TIMESTAMP);
                    KeyCodec.validateTimestamp(type, timestamp);
                    return KeyCodec.checkedAdd(marker, 12);
                }
            case STRING:
                return KeyCodec.checkedAdd(
                        marker,
                        KeyCodec.utf8Size((String) KeyCodec.require(value, Value.Kind.STRING)));
            case BINARY:
                return KeyCodec.checkedAdd(
                        marker,
                        ((ByteBuffer) KeyCodec.require(value, Value.Kind.BINARY)).remaining());
            case LIST:
                {
                    if (value.kind() != Value.Kind.LIST)
                        throw new IllegalArgumentException("value does not match logical type");
                    List<Value> values = value.cast();
                    int size = 4;
                    for (Value child : values)
                        size =
                                KeyCodec.checkedAdd(
                                        size,
                                        KeyCodec.checkedAdd(
                                                4, sizeValue(list(type).elementType(), child)));
                    return KeyCodec.checkedAdd(marker, size);
                }
            case MAP:
                {
                    if (value.kind() != Value.Kind.MAP)
                        throw new IllegalArgumentException("value does not match logical type");
                    List<Map.Entry<Value, Value>> entries = value.cast();
                    int size = 4;
                    for (Map.Entry<Value, Value> entry : entries)
                        size =
                                KeyCodec.checkedAdd(
                                        size,
                                        KeyCodec.checkedAdd(
                                                KeyCodec.checkedAdd(
                                                        8,
                                                        sizeValue(
                                                                map(type).keyType(),
                                                                entry.getKey())),
                                                sizeValue(
                                                        map(type).valueType(), entry.getValue())));
                    return KeyCodec.checkedAdd(marker, size);
                }
            case STRUCT:
                {
                    if (value.kind() != Value.Kind.STRUCT)
                        throw new IllegalArgumentException("value does not match logical type");
                    List<Value> values = value.cast();
                    if (values.size() != struct(type).recordType().fields().size())
                        throw new IllegalArgumentException(
                                "struct field count does not match schema");
                    int size = 0;
                    for (int i = 0; i < values.size(); i++)
                        size =
                                KeyCodec.checkedAdd(
                                        size,
                                        KeyCodec.checkedAdd(
                                                4,
                                                sizeValue(
                                                        struct(type)
                                                                .recordType()
                                                                .fields()
                                                                .get(i)
                                                                .logicalType(),
                                                        values.get(i))));
                    return KeyCodec.checkedAdd(marker, size);
                }
            case EXTENSION:
                {
                    if (value.kind() != Value.Kind.EXTENSION)
                        throw new IllegalArgumentException("value does not match logical type");
                    Value.Extension extension = value.cast();
                    if (!extension(type).typeId().equals(extension.typeId))
                        throw new IllegalArgumentException(
                                "extension type id does not match schema");
                    return KeyCodec.checkedAdd(
                            marker, sizeValue(extension(type).physicalType(), extension.value));
                }
            default:
                throw new AssertionError(type.kind());
        }
    }

    private static void putFramed(LogicalType type, Value value, ByteBuffer out) {
        int length = sizeValue(type, value);
        putU32Le(out, length);
        encodeValue(type, value, out);
    }

    private static Value readFramed(LogicalType type, ByteBuffer input) {
        int length = getU32Le(input);
        if (length < 0) throw new IllegalArgumentException("nested value length exceeds i32");
        KeyCodec.requireRemaining(input, length);
        ByteBuffer child = input.slice();
        ((Buffer) child).limit(length);
        ((Buffer) input).position(input.position() + length);
        return decode(type, child);
    }

    private static int getCount(ByteBuffer input) {
        int count = getU32Le(input);
        if (count < 0) throw new IllegalArgumentException("container count exceeds i32");
        return count;
    }

    private static void exact(ByteBuffer input, int length) {
        if (input.remaining() != length) throw new IllegalArgumentException("invalid value length");
    }

    private static boolean readBoolExact(ByteBuffer input) {
        exact(input, 1);
        byte value = input.get();
        if (value != 0 && value != 1) throw new IllegalArgumentException("invalid boolean byte");
        return value == 1;
    }

    private static void putU32Le(ByteBuffer out, int value) {
        putI32Le(out, value);
    }

    private static int getU32Le(ByteBuffer input) {
        return getI32Le(input);
    }

    private static void putI16Le(ByteBuffer out, short value) {
        out.put((byte) value).put((byte) (value >>> 8));
    }

    private static short getI16Le(ByteBuffer input) {
        KeyCodec.requireRemaining(input, 2);
        return (short) ((input.get() & 255) | ((input.get() & 255) << 8));
    }

    private static void putI32Le(ByteBuffer out, int value) {
        out.put((byte) value)
                .put((byte) (value >>> 8))
                .put((byte) (value >>> 16))
                .put((byte) (value >>> 24));
    }

    private static int getI32Le(ByteBuffer input) {
        KeyCodec.requireRemaining(input, 4);
        return (input.get() & 255)
                | ((input.get() & 255) << 8)
                | ((input.get() & 255) << 16)
                | ((input.get() & 255) << 24);
    }

    private static void putI64Le(ByteBuffer out, long value) {
        for (int i = 0; i < 8; i++) out.put((byte) (value >>> (8 * i)));
    }

    private static long getI64Le(ByteBuffer input) {
        KeyCodec.requireRemaining(input, 8);
        long value = 0;
        for (int i = 0; i < 8; i++) value |= (long) (input.get() & 255) << (8 * i);
        return value;
    }

    private static void putFixedLe(ByteBuffer out, BigInteger value, int width) {
        byte[] raw = value.toByteArray();
        for (int i = 0; i < width; i++) {
            int source = raw.length - 1 - i;
            out.put(source >= 0 ? raw[source] : (byte) (value.signum() < 0 ? 0xff : 0));
        }
    }

    private static BigInteger getFixedLe(ByteBuffer input, int width) {
        byte[] raw = new byte[width];
        input.get(raw);
        for (int i = 0; i < width / 2; i++) {
            byte swap = raw[i];
            raw[i] = raw[width - 1 - i];
            raw[width - 1 - i] = swap;
        }
        return new BigInteger(raw);
    }

    private static String readUtf8(ByteBuffer input) {
        try {
            return StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(input)
                    .toString();
        } catch (java.nio.charset.CharacterCodingException error) {
            throw new IllegalArgumentException("invalid UTF-8 value", error);
        }
    }
}
