package io.cobble.table;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ValueProjectionTest {
    private static StructType struct(DataField... fields) {
        return LogicalTypes.struct(new RecordType(Arrays.asList(fields)));
    }

    @Test
    void selectsReorderedDeepStructsAndPreservesNullAndEmptyContainers() throws Exception {
        DataField keep = new DataField(11, "keep", LogicalTypes.int32());
        DataField unused = new DataField(12, "unused", LogicalTypes.binary());
        StructType element = struct(unused, keep).nullable();
        StructType selectedElement = struct(keep).nullable();
        StructType source =
                struct(
                                new DataField(1, "nested", element),
                                new DataField(2, "list", LogicalTypes.list(element)),
                                new DataField(
                                        3, "map", LogicalTypes.map(LogicalTypes.string(), element)))
                        .nullable();
        StructType output =
                struct(
                                new DataField(
                                        3,
                                        "map",
                                        LogicalTypes.map(LogicalTypes.string(), selectedElement)),
                                new DataField(1, "nested", selectedElement),
                                new DataField(2, "list", LogicalTypes.list(selectedElement)))
                        .nullable();
        Value item =
                Value.struct(
                        Arrays.asList(
                                Value.binary(ByteBuffer.wrap(new byte[] {1, 2})), Value.int32(7)));
        Value projectedItem = Value.struct(Collections.singletonList(Value.int32(7)));
        Value value =
                Value.struct(
                        Arrays.asList(
                                item,
                                Value.list(Arrays.asList(Value.nullValue(), item)),
                                Value.map(
                                        Arrays.asList(
                                                new AbstractMap.SimpleImmutableEntry<>(
                                                        Value.string("a"), item),
                                                new AbstractMap.SimpleImmutableEntry<>(
                                                        Value.string("b"), Value.nullValue())))));
        Value expected =
                Value.struct(
                        Arrays.asList(
                                Value.map(
                                        Arrays.asList(
                                                new AbstractMap.SimpleImmutableEntry<>(
                                                        Value.string("a"), projectedItem),
                                                new AbstractMap.SimpleImmutableEntry<>(
                                                        Value.string("b"), Value.nullValue()))),
                                projectedItem,
                                Value.list(Arrays.asList(Value.nullValue(), projectedItem))));
        ValueProjection projection = roundTrip(ValueProjection.compile(source, output));
        assertEquals(expected, projection.project(value));
        assertEquals(
                expected, projection.decode(ByteBuffer.wrap(ValueCodec.encode(source, value))));
        assertEquals(
                expected,
                projection.decodeOwned(ByteBuffer.wrap(ValueCodec.encode(source, value))));
        assertEquals(Value.nullValue(), projection.decodeOwned(ByteBuffer.wrap(new byte[] {0})));
        Value empty =
                Value.struct(
                        Arrays.asList(
                                Value.nullValue(),
                                Value.list(Collections.emptyList()),
                                Value.map(Collections.emptyList())));
        assertEquals(
                projection.project(empty),
                projection.decodeOwned(ByteBuffer.wrap(ValueCodec.encode(source, empty))));
        ValueProjection identity = ValueProjection.compile(output, output);
        assertSame(expected, identity.project(expected));
        assertEquals(
                expected,
                identity.decodeOwned(ByteBuffer.wrap(ValueCodec.encode(output, expected))));
    }

    @Test
    void skipsUnselectedScalarWithoutDecodingButValidatesItsFrame() {
        DataField invalid = new DataField(1, "invalid", LogicalTypes.bool());
        DataField selected = new DataField(2, "selected", LogicalTypes.int32());
        StructType source = struct(invalid, selected);
        Value value = Value.struct(Arrays.asList(Value.bool(true), Value.int32(42)));
        byte[] bytes = ValueCodec.encode(source, value);
        bytes[4] = 2; // Valid frame, invalid boolean payload.
        ValueProjection projection = ValueProjection.compile(source, struct(selected));
        assertEquals(
                Value.struct(Collections.singletonList(Value.int32(42))),
                projection.decodeOwned(ByteBuffer.wrap(bytes)));
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueCodec.decodeOwned(source, ByteBuffer.wrap(bytes)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueProjection.compile(source, struct(invalid))
                                .decodeOwned(ByteBuffer.wrap(bytes)));
        bytes[0] = 127;
        assertThrows(
                IllegalArgumentException.class,
                () -> projection.decodeOwned(ByteBuffer.wrap(bytes)));
    }

    @Test
    void selectedBinaryHonorsOwnedAndBorrowedLifetimes() {
        DataField binary = new DataField(1, "bytes", LogicalTypes.binary());
        StructType source = struct(binary, new DataField(2, "unused", LogicalTypes.string()));
        ValueProjection projection = ValueProjection.compile(source, struct(binary));
        byte[] bytes =
                ValueCodec.encode(
                        source,
                        Value.struct(
                                Arrays.asList(
                                        Value.binary(ByteBuffer.wrap(new byte[] {5, 6})),
                                        Value.string("skip"))));
        Value owned = projection.decodeOwned(ByteBuffer.wrap(bytes));
        Value borrowed = projection.decode(ByteBuffer.wrap(bytes));
        bytes[4] = 9;
        List<Value> ownedFields = owned.cast();
        List<Value> borrowedFields = borrowed.cast();
        assertEquals(5, ((ByteBuffer) ownedFields.get(0).raw()).get(0));
        assertEquals(9, ((ByteBuffer) borrowedFields.get(0).raw()).get(0));
    }

    @Test
    void supportsEmptyStructSelectionAndRejectsCastsUnknownIdsAndMapKeyChanges() {
        DataField field = new DataField(1, "a", LogicalTypes.int32());
        StructType source = struct(field);
        Value value = Value.struct(Collections.singletonList(Value.int32(1)));
        assertEquals(
                Value.struct(Collections.emptyList()),
                ValueProjection.compile(source, struct())
                        .decodeOwned(ByteBuffer.wrap(ValueCodec.encode(source, value))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueProjection.compile(
                                source, struct(new DataField(2, "a", LogicalTypes.int32()))));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueProjection.compile(
                                source, struct(new DataField(1, "a", LogicalTypes.int64()))));
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueProjection.compile(source, source.nullable()));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ValueProjection.compile(
                                LogicalTypes.map(source, source),
                                LogicalTypes.map(struct(), source)));
    }

    @Test
    void pluginFallbackPreservesPhysicalAccountingAndIdentityFastPath() throws Exception {
        DataField first =
                new DataField(
                        1,
                        "a",
                        struct(
                                new DataField(11, "n", LogicalTypes.int32()),
                                new DataField(12, "unused", LogicalTypes.string())));
        TableReadSchema source = new TableReadSchema(Collections.singletonList(first));
        TableReadSchema output =
                new TableReadSchema(
                        Collections.singletonList(
                                new DataField(
                                        1,
                                        "a",
                                        struct(new DataField(11, "n", LogicalTypes.int32())))));
        TableReadPosition position = new TableReadPosition(2, new byte[] {1}, 0);
        TableReadEntry<List<Value>> entry =
                new TableReadEntry<>(
                        position,
                        Collections.singletonList(
                                Value.struct(Arrays.asList(Value.int32(5), Value.string("skip")))),
                        123,
                        false);
        TableReadCursor<List<Value>> delegate =
                new TableReadCursor<List<Value>>() {
                    boolean emitted;

                    @Override
                    public TableReadEntry<List<Value>> next() {
                        if (emitted) return null;
                        emitted = true;
                        return entry;
                    }

                    @Override
                    public void close() {}
                };
        assertSame(
                delegate,
                TableReadProjection.cursor(delegate, new TableReadProjection(source, source)));
        try (TableReadCursor<List<Value>> cursor =
                TableReadProjection.cursor(delegate, new TableReadProjection(source, output))) {
            TableReadEntry<List<Value>> projected = cursor.next();
            assertSame(position, projected.position());
            assertEquals(123, projected.physicalBytes());
            assertFalse(projected.countsPhysicalEntry());
            assertEquals(
                    Collections.singletonList(
                            Value.struct(Collections.singletonList(Value.int32(5)))),
                    projected.value());
            assertNull(cursor.next());
        }
    }

    private static ValueProjection roundTrip(ValueProjection projection) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(projection);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ValueProjection) input.readObject();
        }
    }
}
