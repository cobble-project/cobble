package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.ShardSnapshot;
import io.cobble.WriteOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableWriterBuilderTest {

    @TempDir Path root;

    @Test
    void scansCompoundKeyPrefixesWithoutPostFiltering() {
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1L, "first", LogicalTypes.int8()),
                                new DataField(2L, "second", LogicalTypes.int8()),
                                new DataField(3L, "suffix", LogicalTypes.binary()),
                                new DataField(4L, "value", LogicalTypes.string())),
                        Arrays.asList(1L, 2L, 3L),
                        Arrays.asList(1L, 2L));
        Config config = new Config().addVolume(root.toUri().toString()).totalBuckets(8);
        List<Value> first =
                Arrays.asList(
                        Value.int8((byte) 1),
                        Value.int8((byte) 2),
                        Value.binary(new byte[] {0, (byte) 0xff}),
                        Value.string("first"));
        List<Value> second =
                Arrays.asList(
                        Value.int8((byte) 1),
                        Value.int8((byte) 2),
                        Value.binary(new byte[] {0, (byte) 0xff, 0}),
                        Value.string("second"));
        List<Value> other =
                Arrays.asList(
                        Value.int8((byte) 1),
                        Value.int8((byte) 3),
                        Value.binary(new byte[] {0, (byte) 0xff}),
                        Value.string("other"));
        List<Value> maximum =
                Arrays.asList(
                        Value.int8((byte) 127),
                        Value.int8((byte) 127),
                        Value.binary(new byte[] {1}),
                        Value.string("maximum"));
        try (Db db = Db.open(config);
                Table table = Table.create(db, "prefixes", schema)) {
            table.put(first);
            table.put(second);
            table.put(other);
            table.put(maximum);
            List<List<Value>> rows = new ArrayList<List<Value>>();
            try (TableScanCursor cursor = table.scanKeyPrefix(first.subList(0, 2))) {
                for (List<Value> row : cursor) rows.add(row);
            }
            assertEquals(Arrays.asList(first, second), rows);
            rows.clear();
            try (TableScanCursor cursor = table.scanKeyPrefix(first.subList(0, 3))) {
                for (List<Value> row : cursor) rows.add(row);
            }
            assertEquals(Collections.singletonList(first), rows);

            assertThrows(
                    IllegalArgumentException.class,
                    () -> table.scanKeyPrefix(Collections.emptyList()));
            assertThrows(
                    IllegalArgumentException.class, () -> table.scanKeyPrefix(first.subList(0, 1)));
            assertThrows(IllegalArgumentException.class, () -> table.scanKeyPrefix(first));
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            table.scanKeyPrefix(
                                    Arrays.asList(Value.int8((byte) 1), Value.string("bad"))));

            List<Value> maxPrefix = maximum.subList(0, 2);
            assertTrue(
                    Arrays.equals(
                            new byte[] {(byte) 0xff, (byte) 0xff},
                            KeyCodec.encode(
                                    Arrays.asList(LogicalTypes.int8(), LogicalTypes.int8()),
                                    maxPrefix)));
            rows.clear();
            try (TableScanCursor cursor = table.scanKeyPrefix(maxPrefix)) {
                for (List<Value> row : cursor) rows.add(row);
            }
            assertEquals(Collections.singletonList(maximum), rows);
        }
    }

    @Test
    void valueOnlyWritesAndReadsUseSchemaOrderAndOwnNestedBytes() {
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1L, "payload", LogicalTypes.binary()),
                                new DataField(2L, "tenant", LogicalTypes.string()),
                                new DataField(3L, "count", LogicalTypes.int32().nullable()),
                                new DataField(4L, "sequence", LogicalTypes.int64()),
                                new DataField(
                                        5L,
                                        "nested",
                                        LogicalTypes.list(LogicalTypes.binary().nullable()))),
                        Arrays.asList(2L, 4L),
                        Collections.singletonList(2L));
        Config config = new Config().addVolume(root.toUri().toString()).totalBuckets(1);
        try (Db db = Db.open(config);
                Table table = Table.create(db, "value_only", schema)) {
            TableKey first =
                    table.keyBuilder().push(Value.string("tenant")).push(Value.int64(1)).build();
            TableKey second =
                    table.keyBuilder().push(Value.string("tenant")).push(Value.int64(2)).build();
            TableKey missing =
                    table.keyBuilder().push(Value.string("tenant")).push(Value.int64(3)).build();
            byte[] payloadBytes = new byte[] {0, (byte) 0xff, 3};
            byte[] nestedBytes = new byte[] {7, 0, (byte) 0xff};
            List<Value> firstValues =
                    Arrays.asList(
                            Value.binary(ByteBuffer.wrap(payloadBytes)),
                            Value.nullValue(),
                            Value.list(
                                    Arrays.asList(
                                            Value.binary(ByteBuffer.wrap(nestedBytes)),
                                            Value.nullValue())));
            try (WriteOptions options =
                    new WriteOptions()
                            .ttlSeconds(3600)
                            .awaitDurable(true)
                            .columnFamily("default")) {
                table.putValues(first, firstValues, options);
            }
            payloadBytes[0] = 99;
            nestedBytes[0] = 99;
            List<Value> expectedFirst =
                    Arrays.asList(
                            Value.binary(new byte[] {0, (byte) 0xff, 3}),
                            Value.nullValue(),
                            Value.list(
                                    Arrays.asList(
                                            Value.binary(new byte[] {7, 0, (byte) 0xff}),
                                            Value.nullValue())));
            List<Value> secondValues =
                    Arrays.asList(
                            Value.binary(new byte[] {4}),
                            Value.int32(9),
                            Value.list(Collections.singletonList(Value.binary(new byte[] {5}))));
            table.putValues(second, secondValues);

            List<Value> firstRead = table.getValues(first);
            assertEquals(expectedFirst, firstRead);
            assertEquals(
                    Arrays.asList(
                            expectedFirst.get(0),
                            Value.string("tenant"),
                            expectedFirst.get(1),
                            Value.int64(1),
                            expectedFirst.get(2)),
                    table.get(first));
            assertNull(table.getValues(missing));
            List<List<Value>> batchRead =
                    table.multiGetValues(Arrays.asList(second, missing, first, second));
            assertEquals(Arrays.asList(secondValues, null, expectedFirst, secondValues), batchRead);
            table.getValues(second);
            assertEquals(expectedFirst, firstRead);
            assertEquals(expectedFirst, batchRead.get(2));
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> table.getValues(first).add(Value.int32(1)));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> table.putValues(first, Collections.singletonList(Value.int32(1))));
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            table.putValues(
                                    first,
                                    Arrays.asList(
                                            expectedFirst.get(0),
                                            Value.string("wrong type"),
                                            expectedFirst.get(2))));
            assertEquals(expectedFirst, table.getValues(first));
            assertNull(table.getValues(missing));
        }
    }

    @Test
    void bucketWriterUsesStableIdentityAndEmptyBaselineForAppendAndOverwrite() {
        TableSchema schema = schema();
        ShardSnapshot appended;
        ShardSnapshot laterUncommitted;
        long firstId;
        long secondId;
        long laterId;
        try (Table table = newWriter(0).create(schema)) {
            assertEquals("bucket-0", table.snapshot().dbId);
            firstId = keyInBucket(table, 0, 1L);
            secondId = keyInBucket(table, 0, firstId + 1L);
            table.put(Arrays.asList(Value.int64(firstId), Value.string("first")));
            appended = table.snapshot();
            laterId = keyInBucket(table, 0, secondId + 1L);
            table.put(Arrays.asList(Value.int64(laterId), Value.string("uncommitted")));
            laterUncommitted = table.snapshot();
            assertEquals("bucket-0", appended.dbId);
        }

        try (Table table = newWriter(0).resumeFromSnapshot(appended.snapshotId)) {
            assertTrue(table.get(table.keyBuilder().push(Value.int64(firstId)).build()) != null);
            assertFalse(table.get(table.keyBuilder().push(Value.int64(laterId)).build()) != null);
            table.put(Arrays.asList(Value.int64(secondId), Value.string("second")));
            assertTrue(table.snapshot().snapshotId > laterUncommitted.snapshotId);
        }

        // create() reopens retained empty snapshot 0, so overwrite starts without old rows.
        ShardSnapshot overwritten;
        try (Table table = newWriter(0).create(schema)) {
            assertFalse(table.get(table.keyBuilder().push(Value.int64(firstId)).build()) != null);
            assertFalse(table.get(table.keyBuilder().push(Value.int64(secondId)).build()) != null);
            table.put(Arrays.asList(Value.int64(firstId), Value.string("overwritten")));
            overwritten = table.snapshot();
        }

        assertTrue(overwritten.snapshotId > laterUncommitted.snapshotId);
        try (Table table = newWriter(0).resumeFromSnapshot(appended.snapshotId)) {
            assertEquals(
                    Arrays.asList(Value.int64(firstId), Value.string("first")),
                    table.get(table.keyBuilder().push(Value.int64(firstId)).build()));
            assertFalse(table.get(table.keyBuilder().push(Value.int64(secondId)).build()) != null);
            assertFalse(table.get(table.keyBuilder().push(Value.int64(laterId)).build()) != null);
        }
    }

    @Test
    void bucketWriterValidatesBaselineSchemaAndSelectedSnapshotBeforeWrites() {
        TableSchema schema = schema();
        newWriter(0).create(schema).close();

        TableSchema incompatible =
                new TableSchema(
                        Collections.singletonList(new DataField(1L, "id", LogicalTypes.int32())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        IllegalStateException wrongSchema =
                assertThrows(IllegalStateException.class, () -> newWriter(0).create(incompatible));
        assertTrue(wrongSchema.getMessage().contains("not this standalone table"));
        IllegalStateException missingSnapshot =
                assertThrows(
                        IllegalStateException.class, () -> newWriter(0).resumeFromSnapshot(999L));
        assertTrue(missingSnapshot.getMessage().contains("missing snapshot 999"));
    }

    @Test
    void writerRequiresOneValidBucketBeforeCreatingStorage() throws Exception {
        TableSchema schema = schema();
        assertThrows(
                IllegalStateException.class,
                () -> Table.writerBuilder(runtime()).tableName("events").create(schema));
        try (java.util.stream.Stream<Path> entries = Files.list(root)) {
            assertFalse(entries.findAny().isPresent());
        }
        assertThrows(
                IllegalStateException.class,
                () -> Table.writerBuilder(runtime()).tableName("events").bucket(2).create(schema));
        assertThrows(
                IllegalArgumentException.class,
                () -> Table.writerBuilder(runtime()).tableName("events").bucket(65536));
    }

    @Test
    void failedInitialBucketStateDoesNotGetSilentlyReinitialized() throws Exception {
        Files.createDirectories(root.resolve("bucket-1").resolve("data"));
        IllegalStateException failure =
                assertThrows(IllegalStateException.class, () -> newWriter(1).create(schema()));
        assertTrue(failure.getMessage().contains("persisted state but no empty baseline"));
    }

    @Test
    void bucketWriterRejectsNonemptyGenericSnapshotZeroAsItsBaseline() throws Exception {
        TableSchema schema = schema();
        String genericDbId;
        try (Db db = Db.open(runtime(), 1, 1);
                Table table = Table.create(db, "events", schema)) {
            long id = keyInBucket(table, 1, 1L);
            table.put(Arrays.asList(Value.int64(id), Value.string("not-a-baseline")));
            genericDbId = table.snapshot().dbId;
        }
        Files.move(root.resolve(genericDbId), root.resolve("bucket-1"));

        IllegalStateException failure =
                assertThrows(IllegalStateException.class, () -> newWriter(1).create(schema));
        assertTrue(failure.getMessage().contains("must not contain data"), failure::getMessage);
    }

    private TableWriterBuilder newWriter(int bucket) {
        return Table.writerBuilder(runtime()).tableName("events").bucket(bucket);
    }

    private Config runtime() {
        return new Config().addVolume(root.toUri().toString()).totalBuckets(2);
    }

    private static TableSchema schema() {
        return new TableSchema(
                Arrays.asList(
                        new DataField(1L, "id", LogicalTypes.int64()),
                        new DataField(2L, "value", LogicalTypes.string().nullable())),
                Collections.singletonList(1L),
                Collections.singletonList(1L));
    }

    private static long keyInBucket(Table table, int bucket, long firstCandidate) {
        for (long candidate = firstCandidate; ; candidate++) {
            if (table.keyBuilder().push(Value.int64(candidate)).build().bucket() == bucket)
                return candidate;
        }
    }
}
