package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableWriterBuilderTest {

    @TempDir Path root;

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
    void bucketWriterRejectsConcurrentHandleAndInvalidBaselineSchemaBeforeWrites() {
        TableSchema schema = schema();
        try (Table first = newWriter(0).create(schema)) {
            IllegalStateException concurrent =
                    assertThrows(IllegalStateException.class, () -> newWriter(0).create(schema));
            assertTrue(concurrent.getMessage().contains("already active"), concurrent::getMessage);
        }

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
