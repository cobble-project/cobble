package io.cobble.table;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TableSchemaSerializationTest {
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
