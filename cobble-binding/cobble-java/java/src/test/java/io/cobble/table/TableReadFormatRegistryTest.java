package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.GlobalSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableReadFormatRegistryTest {
    @TempDir Path dataDir;

    @Test
    void nativeFactoryPlansFixedSnapshotAndEmptyProjectionRetainsRows() throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "value", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        GlobalSnapshot global;
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema)) {
            table.put(Arrays.asList(Value.int64(1), Value.string("one")));
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                global = committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
            }
        }

        TableReadSnapshot snapshot = TableReadSnapshot.forGlobal(config, global, "data");
        assertEquals("cobble-table", snapshot.formatId());
        global.shardSnapshots.get(0).columnFamilies.clear();
        assertEquals("cobble-table", snapshot.formatId());

        TableScanPlan plan = roundTrip(TableReadFormatRegistry.plan(config, snapshot));
        assertEquals(Arrays.asList("id", "value"), names(plan.readSchema()));
        assertEquals(1, plan.splits().size());
        assertEquals(global.id, plan.snapshotId());
        assertEquals(1, plan.totalBuckets());
        assertEquals(
                plan.readSchema().fields(),
                TableScanPlan.forSnapshot(config, "data", global.id).readSchema().fields());
        assertEquals(global.id, TableScanPlan.forCurrentSnapshot(config, "data").snapshotId());
        TableScanSplit split = roundTrip(plan.splits().get(0));
        assertEquals("cobble-table", split.formatId());

        TableScanPlan countPlan = plan.project(Collections.<String>emptyList());
        assertEquals(schema, countPlan.schema());
        assertEquals(plan.snapshotId(), countPlan.forSplit(plan.splits().get(0)).snapshotId());
        assertThrows(
                IllegalArgumentException.class,
                () -> plan.project(Collections.singletonList("missing")));
        assertThrows(IllegalArgumentException.class, () -> plan.project(Arrays.asList("id", "id")));
        try (TableReadProvider<List<Value>, ?> provider = countPlan.open(config, split);
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
            assertEquals(Collections.emptyList(), cursor.next().value());
            assertEquals(null, cursor.next());
        }

        assertProjectedRow(
                config,
                plan.project(Collections.singletonList("value")),
                split,
                Collections.singletonList(Value.string("one")));
        assertProjectedRow(
                config,
                roundTrip(plan.project(Arrays.asList("value", "id"))),
                split,
                Arrays.asList(Value.string("one"), Value.int64(1)));
    }

    @Test
    void registryRejectsUnknownAndDuplicateFormatIds() {
        TableReadFormatRegistry.Registry registry =
                TableReadFormatRegistry.fromFactories(
                        Collections.singletonList(new StubFactory("one")));
        assertThrows(IllegalArgumentException.class, () -> registry.resolve("missing"));
        assertThrows(
                IllegalStateException.class,
                () ->
                        TableReadFormatRegistry.fromFactories(
                                Arrays.asList(new StubFactory("one"), new StubFactory("one"))));
    }

    @Test
    void closingSessionClosesUnclosedNativeCursor() throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Collections.singletonList(new DataField(1, "id", LogicalTypes.int64())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        GlobalSnapshot global;
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema)) {
            table.put(Collections.singletonList(Value.int64(1)));
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                global = committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
            }
        }
        TableScanPlan plan =
                TableReadFormatRegistry.plan(
                        config, TableReadSnapshot.forGlobal(config, global, "data"));
        try (TableReadProvider<List<Value>, ?> provider = plan.open(config, plan.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open()) {
            TableReadCursor<List<Value>> cursor =
                    session.scan(new TableReadRange(0, Integer.MAX_VALUE), null);
            session.close();
            assertThrows(IllegalStateException.class, cursor::next);
        }
    }

    @Test
    void directTraversalOwnsOverflowRowsAndClosesOnDecoderFailure() throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "payload", LogicalTypes.binary())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        byte[] payload = new byte[8 * 1024];
        Arrays.fill(payload, (byte) 7);
        GlobalSnapshot global;
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema)) {
            table.put(Arrays.asList(Value.int64(1), Value.binary(payload)));
            table.put(Arrays.asList(Value.int64(2), Value.binary(new byte[] {9})));
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                global = committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
            }
        }

        TableScanPlan plan = TableScanPlan.forSnapshot(config, "data", global.id);
        try (TableReadProvider<List<Value>, ?> provider = plan.open(config, plan.splits().get(0));
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
            TableReadEntry<List<Value>> first = cursor.next();
            assertTrue(first.countsPhysicalEntry());
            assertTrue(first.physicalBytes() >= payload.length);
            assertEquals(
                    Arrays.asList(Value.int64(2), Value.binary(new byte[] {9})),
                    cursor.next().value());
            cursor.close();
            assertEquals(Arrays.asList(Value.int64(1), Value.binary(payload)), first.value());
        }

        TableScanSplit split = plan.splits().get(0);
        java.util.List<String> names = Arrays.asList("id", "payload");
        try (io.cobble.DirectScanCursor direct = split.openDirectScanner(config, names, 0)) {
            TableDirectScanCursor<List<Value>> cursor =
                    TableDirectScanCursor.fixed(
                            direct,
                            (entry, key) -> {
                                throw new java.io.IOException("expected decoder failure");
                            });
            assertThrows(java.io.IOException.class, cursor::next);
            assertThrows(IllegalStateException.class, cursor::next);
            assertThrows(IllegalStateException.class, direct::nextEntry);
        }
    }

    @Test
    void directTraversalAllowsEmptyAssignmentButRejectsItsResumePosition() throws Exception {
        try (TableDirectScanCursor<List<Value>> cursor =
                TableDirectScanCursor.buckets(
                        Collections.<Integer>emptyList(),
                        null,
                        new byte[] {(byte) 0xff},
                        (bucket, start, end) -> {
                            throw new AssertionError("empty assignment must not open a cursor");
                        },
                        (entry, key) ->
                                Collections.singletonList(Collections.<Value>emptyList()))) {
            assertEquals(null, cursor.next());
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        TableDirectScanCursor.buckets(
                                Collections.<Integer>emptyList(),
                                new TableReadPosition(0, new byte[] {1}, 1),
                                new byte[] {(byte) 0xff},
                                (bucket, start, end) -> null,
                                (entry, key) ->
                                        Collections.singletonList(Collections.<Value>emptyList())));
    }

    private static List<String> names(TableReadSchema schema) {
        java.util.ArrayList<String> names = new java.util.ArrayList<String>();
        for (DataField field : schema.fields()) names.add(field.name());
        return names;
    }

    private static void assertProjectedRow(
            Config config, TableScanPlan plan, TableScanSplit split, List<Value> expected)
            throws Exception {
        try (TableReadProvider<List<Value>, ?> provider = plan.open(config, split);
                TableReadSession<List<Value>, ?> session = provider.open();
                TableReadCursor<List<Value>> cursor =
                        session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
            assertEquals(expected, cursor.next().value());
            assertEquals(null, cursor.next());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }

    private static final class StubFactory implements TableReadFormatFactory {
        private final String formatId;

        private StubFactory(String formatId) {
            this.formatId = formatId;
        }

        @Override
        public String formatId() {
            return formatId;
        }

        @Override
        public TableScanPlan plan(Config config, TableReadSnapshot snapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split) {
            throw new UnsupportedOperationException();
        }
    }
}
