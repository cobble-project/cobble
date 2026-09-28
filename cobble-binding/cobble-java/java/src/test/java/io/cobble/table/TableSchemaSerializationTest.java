package io.cobble.table;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableSchemaSerializationTest {
    @Test
    void serializesEveryValueKindWithOwnedNestedBinaryAndRawFloatBits() throws Exception {
        ByteBuffer direct = ByteBuffer.allocateDirect(6);
        direct.put(new byte[] {9, 0, (byte) 0xff, 3, 4, 8});
        ((Buffer) direct).position(1);
        ((Buffer) direct).limit(5);
        Value directBinary = Value.binary(direct);
        byte[] readOnlyBacking = new byte[] {7, 0, (byte) 0xff, 6};
        ByteBuffer readOnly = ByteBuffer.wrap(readOnlyBacking).asReadOnlyBuffer();
        ((Buffer) readOnly).position(1);
        ((Buffer) readOnly).limit(3);
        Value readOnlyBinary = Value.binary(readOnly);
        byte[] slicedBacking = new byte[] {5, 0, (byte) 0xff, 2, 6};
        Value slicedBinary = Value.binary(ByteBuffer.wrap(slicedBacking, 1, 3).slice());
        Value nested =
                Value.struct(
                        Arrays.asList(
                                Value.list(Arrays.asList(directBinary, Value.nullValue())),
                                Value.map(
                                        Arrays.asList(
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("read-only"), readOnlyBinary),
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("slice"), slicedBinary),
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("null"), Value.nullValue()))),
                                Value.extension("test.binary", directBinary)));
        List<Value> floats =
                Arrays.asList(
                        Value.float32(Float.intBitsToFloat(0x7fc01234)),
                        Value.float32(0.0f),
                        Value.float32(-0.0f),
                        Value.float32(Float.POSITIVE_INFINITY),
                        Value.float32(Float.NEGATIVE_INFINITY),
                        Value.float64(Double.longBitsToDouble(0x7ff8000000001234L)),
                        Value.float64(0.0d),
                        Value.float64(-0.0d),
                        Value.float64(Double.POSITIVE_INFINITY),
                        Value.float64(Double.NEGATIVE_INFINITY));
        List<Value> original = new ArrayList<Value>();
        original.addAll(
                Arrays.asList(
                        Value.nullValue(),
                        Value.bool(true),
                        Value.int8((byte) -8),
                        Value.int16((short) -16),
                        Value.int32(-32),
                        Value.int64(-64)));
        int floatStart = original.size();
        original.addAll(floats);
        original.addAll(
                Arrays.asList(
                        Value.decimal(
                                39, 0, new BigInteger("170141183460469231731687303715884105727")),
                        Value.decimal(
                                39, 0, new BigInteger("-170141183460469231731687303715884105728")),
                        Value.date(-1),
                        Value.time(123456789L),
                        Value.timestamp(9, TimestampKind.WITH_LOCAL_TIME_ZONE, -42L, 123456789),
                        Value.string("text\u0000\uffff"),
                        nested));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        ((Buffer) direct).clear();
        while (direct.hasRemaining()) direct.put((byte) 42);
        readOnlyBacking[1] = 42;
        slicedBacking[1] = 42;

        List<Value> restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            @SuppressWarnings("unchecked")
            List<Value> decoded = (List<Value>) input.readObject();
            restored = decoded;
        }
        Value expectedNested =
                Value.struct(
                        Arrays.asList(
                                Value.list(
                                        Arrays.asList(
                                                Value.binary(new byte[] {0, (byte) 0xff, 3, 4}),
                                                Value.nullValue())),
                                Value.map(
                                        Arrays.asList(
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("read-only"),
                                                        Value.binary(new byte[] {0, (byte) 0xff})),
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("slice"),
                                                        Value.binary(
                                                                new byte[] {0, (byte) 0xff, 2})),
                                                new AbstractMap.SimpleImmutableEntry<Value, Value>(
                                                        Value.string("null"), Value.nullValue()))),
                                Value.extension(
                                        "test.binary",
                                        Value.binary(new byte[] {0, (byte) 0xff, 3, 4}))));
        List<Value> expected = new ArrayList<Value>(original);
        expected.set(expected.size() - 1, expectedNested);
        assertEquals(expected, restored);
        for (int i = 0; i < floats.size(); i++) {
            Value before = floats.get(i);
            Value after = restored.get(floatStart + i);
            if (before.kind() == Value.Kind.FLOAT32)
                assertEquals(
                        Float.floatToRawIntBits((Float) before.raw()),
                        Float.floatToRawIntBits((Float) after.raw()));
            else
                assertEquals(
                        Double.doubleToRawLongBits((Double) before.raw()),
                        Double.doubleToRawLongBits((Double) after.raw()));
        }
        @SuppressWarnings("unchecked")
        List<Value> fields = (List<Value>) restored.get(restored.size() - 1).raw();
        @SuppressWarnings("unchecked")
        List<Value> nestedList = (List<Value>) fields.get(0).raw();
        ByteBuffer ownedBinary = (ByteBuffer) nestedList.get(0).raw();
        assertTrue(ownedBinary.isReadOnly());
        assertFalse(ownedBinary.isDirect());
        assertThrows(UnsupportedOperationException.class, () -> nestedList.add(Value.int32(1)));
    }

    @Test
    void serializesCompleteNestedLogicalTypeGraph() throws Exception {
        ExtensionType extension =
                LogicalTypes.extension(
                                "test.extension",
                                "{\"scale\":2,\"unit\":\"ms\"}",
                                LogicalTypes.int64())
                        .nullable();
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(0, "id", LogicalTypes.int64()),
                                new DataField(
                                        1,
                                        "payload",
                                        LogicalTypes.struct(
                                                        new RecordType(
                                                                Arrays.asList(
                                                                        new DataField(
                                                                                2,
                                                                                "items",
                                                                                LogicalTypes.list(
                                                                                        LogicalTypes
                                                                                                .int32()
                                                                                                .nullable())),
                                                                        new DataField(
                                                                                3,
                                                                                "attributes",
                                                                                LogicalTypes.map(
                                                                                        LogicalTypes
                                                                                                .string(),
                                                                                        extension)),
                                                                        new DataField(
                                                                                4,
                                                                                "amount",
                                                                                LogicalTypes
                                                                                        .decimal(
                                                                                                20,
                                                                                                2)),
                                                                        new DataField(
                                                                                5,
                                                                                "event_time",
                                                                                LogicalTypes
                                                                                        .timestamp(
                                                                                                6,
                                                                                                TimestampKind
                                                                                                        .WITH_LOCAL_TIME_ZONE)),
                                                                        new DataField(
                                                                                6,
                                                                                "local_time",
                                                                                LogicalTypes.time(
                                                                                        6)))))
                                                .nullable())),
                        Collections.singletonList(Long.valueOf(0)),
                        Collections.singletonList(Long.valueOf(0)));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(schema);
        }

        TableSchema restored;
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (TableSchema) input.readObject();
        }

        assertEquals(schema, restored);
        StructType payload = (StructType) restored.fields().get(1).logicalType();
        MapType attributes = (MapType) payload.recordType().fields().get(1).logicalType();
        assertEquals(
                "{\"scale\":2,\"unit\":\"ms\"}",
                ((ExtensionType) attributes.valueType()).parametersJson());
    }
}
