package io.cobble.table;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableCodecTest {
    @Test
    void matches_shared_vectors_and_validates_the_contract() throws Exception {
        JsonObject fixture =
                JsonParser.parseString(
                                new String(
                                        Files.readAllBytes(
                                                Paths.get(
                                                        "..",
                                                        "..",
                                                        "spec",
                                                        "table",
                                                        "fixtures",
                                                        "cobble_table_v1_codec.json")),
                                        "UTF-8"))
                        .getAsJsonObject();
        List<LogicalType> keyTypes = keyTypes();
        List<Value> keyValues = keyValues();
        byte[] key = KeyCodec.encode(keyTypes, keyValues);
        assertEquals(fixture.get("key_hex").getAsString(), hex(key));
        assertEquals(keyValues, KeyCodec.decode(keyTypes, ByteBuffer.wrap(key)));
        assertEquals(Int32Type.class, LogicalTypes.int32().nullable().getClass());
        assertEquals(new DecimalType(20, 2, false), LogicalTypes.decimal(20, 2));
        Float64Type nullableFloat = LogicalTypes.float64().nullable();
        ListType nullableList = LogicalTypes.list(LogicalTypes.string()).nullable();
        assertEquals(true, nullableFloat.isNullable());
        assertEquals(LogicalTypes.list(LogicalTypes.string()).nullable(), nullableList);
        assertEquals(
                0,
                KeyCodec.encode(
                                Collections.singletonList(LogicalTypes.int32()),
                                Collections.singletonList(Value.int32(-1)))[0]
                        & 0x80);
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KeyCodec.encode(
                                Collections.singletonList(LogicalTypes.int32()),
                                Collections.singletonList(Value.nullValue())));

        LogicalType type = valueType();
        Value value = value();
        ByteBuffer direct = ByteBuffer.allocateDirect(ValueCodec.encodedSize(type, value));
        ValueCodec.encodeTo(type, value, direct);
        ((Buffer) direct).flip();
        byte[] encoded = new byte[direct.remaining()];
        direct.duplicate().get(encoded);
        assertEquals(fixture.get("value_hex").getAsString(), hex(encoded));
        Value decoded = ValueCodec.decode(type, direct);
        assertEquals(value, decoded);
        List<LogicalType> orderedTypes =
                Arrays.asList(
                        LogicalTypes.int8(),
                        LogicalTypes.int16(),
                        LogicalTypes.int32(),
                        LogicalTypes.int64(),
                        LogicalTypes.decimal(20, 2),
                        LogicalTypes.date(),
                        LogicalTypes.time(6),
                        LogicalTypes.timestamp(3, TimestampKind.WITHOUT_TIME_ZONE));
        List<Value> lowerValues =
                Arrays.asList(
                        Value.int8((byte) -1),
                        Value.int16((short) -2),
                        Value.int32(-3),
                        Value.int64(-4),
                        Value.decimal(20, 2, BigInteger.valueOf(-42)),
                        Value.date(-1),
                        Value.time(123000000L),
                        Value.timestamp(3, TimestampKind.WITHOUT_TIME_ZONE, -3, 123000000));
        List<Value> upperValues =
                Arrays.asList(
                        Value.int8((byte) 1),
                        Value.int16((short) 2),
                        Value.int32(3),
                        Value.int64(4),
                        Value.decimal(20, 2, BigInteger.valueOf(42)),
                        Value.date(1),
                        Value.time(124000000L),
                        Value.timestamp(3, TimestampKind.WITHOUT_TIME_ZONE, -2, 123000000));
        for (int index = 0; index < orderedTypes.size(); index++) {
            LogicalType orderedType = orderedTypes.get(index);
            byte[] lower = ValueCodec.encode(orderedType, lowerValues.get(index));
            byte[] upper = ValueCodec.encode(orderedType, upperValues.get(index));
            assertEquals(
                    hex(
                            KeyCodec.encode(
                                    Collections.singletonList(orderedType),
                                    Collections.singletonList(lowerValues.get(index)))),
                    hex(lower));
            assertEquals(
                    hex(
                            KeyCodec.encode(
                                    Collections.singletonList(orderedType),
                                    Collections.singletonList(upperValues.get(index)))),
                    hex(upper));
            assertEquals(true, compareUnsigned(lower, upper) < 0);
        }
        @SuppressWarnings("unchecked")
        List<Value> fields = (List<Value>) decoded.raw();
        assertEquals(true, ((ByteBuffer) fields.get(12).raw()).isDirect());
        ByteBuffer exposedBinary = (ByteBuffer) fields.get(12).raw();
        ((Buffer) exposedBinary).position(exposedBinary.limit());
        assertEquals(encoded.length, ValueCodec.encodedSize(type, decoded));
        BucketHash bucketHash = new BucketHash(fixture.get("bucket_count").getAsInt());
        int bucketPrefixLength = fixture.get("bucket_prefix_length").getAsInt();
        ByteBuffer heapBucketKey = ByteBuffer.wrap(key, 0, bucketPrefixLength);
        assertEquals(fixture.get("bucket").getAsInt(), bucketHash.bucket(heapBucketKey));
        assertEquals(0, heapBucketKey.position());
        ByteBuffer directBucketKey = ByteBuffer.allocateDirect(bucketPrefixLength);
        directBucketKey.put(key, 0, bucketPrefixLength);
        ((Buffer) directBucketKey).flip();
        assertEquals(fixture.get("bucket").getAsInt(), bucketHash.bucket(directBucketKey));
        assertEquals(0, directBucketKey.position());
        assertThrows(IllegalArgumentException.class, () -> new BucketHash(0));
        assertThrows(IllegalArgumentException.class, () -> new BucketHash(65537));
        assertEquals(0, new BucketHash(1).bucket(key));
        assertEquals(-28, BucketHash.hash(new byte[] {(byte) 0xc5}));
        assertEquals(65508, new BucketHash(65536).bucket(new byte[] {(byte) 0xc5}));
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueCodec.decode(LogicalTypes.bool(), ByteBuffer.wrap(new byte[] {2})));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueCodec.decode(
                                LogicalTypes.list(LogicalTypes.int32()),
                                ByteBuffer.wrap(
                                        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f})));

        String metadataJson =
                new String(
                        Files.readAllBytes(
                                Paths.get(
                                        "..",
                                        "..",
                                        "spec",
                                        "table",
                                        "fixtures",
                                        "table_metadata_v1.json")),
                        "UTF-8");
        TableMetadata metadata = TableMetadata.fromJson(metadataJson);
        assertEquals("cobble-table-v1", metadata.layout().codec());
        assertEquals(metadata, TableMetadata.fromJson(metadata.toJson()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueCodec.decode(
                                LogicalTypes.map(LogicalTypes.string(), LogicalTypes.int32()),
                                ByteBuffer.wrap(new byte[] {0, 0, 0, (byte) 0x80})));
        ByteBuffer rollback = ByteBuffer.allocate(32);
        rollback.put((byte) 7);
        int start = rollback.position();
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueCodec.encodeTo(LogicalTypes.int32(), Value.string("bad"), rollback));
        assertEquals(start, rollback.position());
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        KeyCodec.encodeTo(
                                Collections.singletonList(LogicalTypes.int32()),
                                Collections.singletonList(Value.nullValue()),
                                rollback));
        assertEquals(start, rollback.position());
    }

    private static List<LogicalType> keyTypes() {
        return Arrays.asList(
                LogicalTypes.bool(),
                LogicalTypes.int8(),
                LogicalTypes.int16(),
                LogicalTypes.int32(),
                LogicalTypes.int64(),
                LogicalTypes.decimal(20, 2),
                LogicalTypes.date(),
                LogicalTypes.time(6),
                LogicalTypes.timestamp(3, TimestampKind.WITHOUT_TIME_ZONE),
                LogicalTypes.string(),
                LogicalTypes.binary());
    }

    private static List<Value> keyValues() {
        return Arrays.asList(
                Value.bool(false),
                Value.int8((byte) -1),
                Value.int16((short) 0),
                Value.int32(1),
                Value.int64(-2),
                Value.decimal(20, 2, BigInteger.valueOf(-42)),
                Value.date(-1),
                Value.time(123000000L),
                Value.timestamp(3, TimestampKind.WITHOUT_TIME_ZONE, -3, 123000000),
                Value.string("x\0é"),
                Value.binary(new byte[] {0, (byte) 0xff}));
    }

    private static LogicalType valueType() {
        return LogicalTypes.struct(
                new RecordType(
                        Arrays.asList(
                                new DataField(1, "bool", LogicalTypes.bool()),
                                new DataField(2, "i8", LogicalTypes.int8()),
                                new DataField(3, "i16", LogicalTypes.int16()),
                                new DataField(4, "i32", LogicalTypes.int32()),
                                new DataField(5, "i64", LogicalTypes.int64()),
                                new DataField(6, "f32", LogicalTypes.float32()),
                                new DataField(7, "f64", LogicalTypes.float64()),
                                new DataField(8, "decimal", LogicalTypes.decimal(20, 2)),
                                new DataField(9, "date", LogicalTypes.date()),
                                new DataField(10, "time", LogicalTypes.time(6)),
                                new DataField(
                                        11,
                                        "timestamp",
                                        LogicalTypes.timestamp(
                                                3, TimestampKind.WITH_LOCAL_TIME_ZONE)),
                                new DataField(12, "string", LogicalTypes.string()),
                                new DataField(13, "binary", LogicalTypes.binary()),
                                new DataField(
                                        14,
                                        "list",
                                        LogicalTypes.list(LogicalTypes.binary().nullable())),
                                new DataField(
                                        15,
                                        "map",
                                        LogicalTypes.map(
                                                LogicalTypes.string(),
                                                LogicalTypes.int32().nullable())),
                                new DataField(
                                        16,
                                        "nested",
                                        LogicalTypes.struct(
                                                new RecordType(
                                                        Collections.singletonList(
                                                                new DataField(
                                                                        17,
                                                                        "n",
                                                                        LogicalTypes.string()
                                                                                .nullable()))))),
                                new DataField(
                                        18,
                                        "extension",
                                        LogicalTypes.extension(
                                                "test.extension", "null", LogicalTypes.int16())))));
    }

    private static Value value() {
        return Value.struct(
                Arrays.asList(
                        Value.bool(true),
                        Value.int8((byte) -1),
                        Value.int16((short) -2),
                        Value.int32(-3),
                        Value.int64(-4),
                        Value.float32(1.5f),
                        Value.float64(-2.5),
                        Value.decimal(20, 2, BigInteger.valueOf(-42)),
                        Value.date(-1),
                        Value.time(123000000L),
                        Value.timestamp(3, TimestampKind.WITH_LOCAL_TIME_ZONE, -3, 123000000),
                        Value.string("hi é"),
                        Value.binary(new byte[] {0, (byte) 0xff}),
                        Value.list(
                                Arrays.asList(Value.binary(new byte[] {'a'}), Value.nullValue())),
                        Value.map(
                                Collections.<java.util.Map.Entry<Value, Value>>singletonList(
                                        new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                Value.string("k"), Value.nullValue()))),
                        Value.struct(Collections.singletonList(Value.string("nested"))),
                        Value.extension("test.extension", Value.int16((short) 7))));
    }

    private static String hex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) builder.append(String.format("%02x", value & 0xff));
        return builder.toString();
    }

    private static int compareUnsigned(byte[] left, byte[] right) {
        for (int index = 0; index < Math.min(left.length, right.length); index++) {
            int comparison = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (comparison != 0) return comparison;
        }
        return Integer.compare(left.length, right.length);
    }
}
