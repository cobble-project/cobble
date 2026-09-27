package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.GlobalSnapshot;
import io.cobble.ScanCursor;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableScanPlanTest {

    @Test
    void scansShardManifestPathsOutsideTheRuntimeRoot() throws Exception {
        Path root = Files.createTempDirectory("cobble-java-table-scan-shards-");
        Config coordinatorConfig =
                new Config().addVolume(root.toString()).numColumns(1).totalBuckets(2);
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "value", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        List<ShardSnapshot> shards = new ArrayList<ShardSnapshot>();
        List<List<Value>> expected = new ArrayList<List<Value>>();
        for (int bucket = 0; bucket < 2; bucket++) {
            Config bucketConfig =
                    new Config()
                            .addVolume(root.resolve("bucket-" + bucket).toString())
                            .numColumns(1)
                            .totalBuckets(2);
            try (Db db = Db.open(bucketConfig, bucket, bucket);
                    Table table = Table.create(db, "data", schema)) {
                long id = bucket;
                while (table.keyBuilder().push(Value.int64(id)).build().bucket() != bucket) id++;
                List<Value> row = Arrays.asList(Value.int64(id), Value.string("bucket-" + bucket));
                table.put(row);
                expected.add(row);
                shards.add(table.snapshot());
            }
        }
        GlobalSnapshot global;
        try (TableSnapshotCommitter committer =
                TableSnapshotCommitter.open(coordinatorConfig, 2, 1)) {
            global = committer.commitBatch(1L, shards);
        }
        Path runtimeRoot = root.resolve("runtime-without-shards");
        Config runtimeConfig =
                new Config().addVolume(runtimeRoot.toString()).numColumns(1).totalBuckets(2);
        TableScanPlan plan;
        try (TableReader reader = TableReader.open(coordinatorConfig, "data", global.id)) {
            plan = roundTrip(reader.scanPlan());
        }
        List<List<Value>> rows = new ArrayList<List<Value>>();
        for (TableScanSplit split : plan.splits()) {
            try (TableScanCursor cursor = split.openTypedScanner(runtimeConfig, 4096)) {
                List<Value> row;
                while ((row = cursor.nextRow()) != null) rows.add(row);
            }
        }
        assertEquals(expected.size(), rows.size());
        assertTrue(rows.containsAll(expected));
        try (TableReader reader = TableReader.open(coordinatorConfig, "data", global.id)) {
            for (List<Value> row : expected) {
                assertEquals(row, reader.get(reader.keyBuilder().push(row.get(0)).build()));
            }
        }
        for (ShardSnapshot shard : shards) {
            assertFalse(
                    Files.exists(runtimeRoot.resolve(shard.dbId)),
                    "read-only scan must not create a runtime database directory");
        }
    }

    @Test
    void plansSerializableProjectedTableScan() throws Exception {
        Path dataDir = Files.createTempDirectory("cobble-java-table-scan-plan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        String large = String.join("", Collections.nCopies(8192, "x"));
        List<Value> largeRow =
                Arrays.asList(Value.int64(8), Value.string(large), Value.string("large"));
        List<Value> tailRow =
                Arrays.asList(Value.int64(9), Value.string("tail"), Value.string("end"));
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "first", LogicalTypes.string()),
                                new DataField(3, "second", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));

        GlobalSnapshot global;
        GlobalSnapshot latest;
        TableScanPlan plan;
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema)) {
            table.put(Arrays.asList(Value.int64(7), Value.string("first"), Value.string("second")));
            table.put(largeRow);
            table.put(tailRow);
            ShardSnapshot shard = db.snapshot();
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                global = committer.commitBatch(1L, Collections.singletonList(shard));
            }
            try (TableReader reader = TableReader.open(config, "data", global.id)) {
                plan = reader.scanPlan();
            }

            table.put(
                    Arrays.asList(
                            Value.int64(7), Value.string("new-first"), Value.string("new-second")));
            ShardSnapshot latestShard = db.snapshot();
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                latest = committer.commitBatch(2L, Collections.singletonList(latestShard));
            }
        }
        assertNotNull(global);
        assertNotNull(latest);

        assertEquals(schema, plan.schema());
        assertEquals(global.id, plan.snapshotId());
        assertEquals(1, plan.totalBuckets());
        try (TableReader reader = TableReader.openCurrent(config, "data")) {
            assertEquals(latest.id, reader.scanPlan().snapshotId());
        }

        plan = roundTrip(plan);

        TableScanPlan reordered =
                roundTrip(plan.project(Arrays.asList("second", "id", "first")))
                        .project(Arrays.asList("first", "id"));
        assertEquals(
                Arrays.asList(schema.fields().get(1), schema.fields().get(0)),
                reordered.readSchema().fields());
        assertEquals(
                Arrays.asList(
                        Arrays.asList(Value.string("first"), Value.int64(7)),
                        Arrays.asList(Value.string(large), Value.int64(8)),
                        Arrays.asList(Value.string("tail"), Value.int64(9))),
                readProjected(config, reordered));
        assertEquals(
                Arrays.asList(
                        Arrays.asList(
                                Value.string("second"), Value.string("first"), Value.int64(7)),
                        Arrays.asList(Value.string("large"), Value.string(large), Value.int64(8)),
                        Arrays.asList(Value.string("end"), Value.string("tail"), Value.int64(9))),
                readProjected(config, plan.project(Arrays.asList("second", "first", "id"))));
        assertTrue(
                physicalBytes(config, plan.project(Collections.singletonList("second"))) + 8192
                        < physicalBytes(config, plan));
        assertEquals(
                Arrays.asList(
                        Collections.singletonList(Value.int64(7)),
                        Collections.singletonList(Value.int64(8)),
                        Collections.singletonList(Value.int64(9))),
                readProjected(config, plan.project(Collections.singletonList("id"))));
        assertEquals(
                Arrays.asList(
                        Collections.<Value>emptyList(),
                        Collections.<Value>emptyList(),
                        Collections.<Value>emptyList()),
                readProjected(config, plan.project(Collections.<String>emptyList())));
        TableScanPlan projected = plan.project(Arrays.asList("second", "id"));
        byte[] boundary =
                KeyCodec.encode(
                        Collections.singletonList(LogicalTypes.int64()),
                        Collections.singletonList(Value.int64(8)));
        TableScanPlan.Partition partition =
                projected.partitionAfter(projected.splits().get(0), 0, boundary);
        assertThrows(
                IllegalArgumentException.class,
                () -> partition.after.forSplit(projected.splits().get(0)));
        assertThrows(
                IllegalArgumentException.class,
                () -> partition.after.open(config, projected.splits().get(0)));
        byte[] earlier =
                KeyCodec.encode(
                        Collections.singletonList(LogicalTypes.int64()),
                        Collections.singletonList(Value.int64(7)));
        byte[] later =
                KeyCodec.encode(
                        Collections.singletonList(LogicalTypes.int64()),
                        Collections.singletonList(Value.int64(9)));
        assertThrows(
                IllegalArgumentException.class,
                () -> partition.after.partitionAfter(partition.after.splits().get(0), 0, earlier));
        assertThrows(
                IllegalArgumentException.class,
                () -> partition.before.partitionAfter(partition.before.splits().get(0), 0, later));
        TableScanPlan.Partition atStart =
                partition.after.partitionAfter(partition.after.splits().get(0), 0, boundary);
        assertEquals(Collections.emptyList(), readProjected(config, atStart.before));
        assertEquals(
                Arrays.asList(
                        Arrays.asList(Value.string("second"), Value.int64(7)),
                        Arrays.asList(Value.string("large"), Value.int64(8))),
                readProjected(config, roundTrip(partition.before)));
        assertEquals(
                Collections.singletonList(Arrays.asList(Value.string("end"), Value.int64(9))),
                readProjected(config, roundTrip(partition.after)));
        assertThrows(
                IllegalArgumentException.class, () -> reordered.project(Arrays.asList("id", "id")));

        List<TableScanSplit> splits = plan.splits();
        assertEquals(1, splits.size());
        TableScanSplit split = roundTrip(splits.get(0));
        assertThrows(
                IllegalArgumentException.class,
                () -> split.openScanner(config, Arrays.asList("first", "first"), 4096));
        assertThrows(
                IllegalArgumentException.class,
                () -> split.openScanner(config, Collections.singletonList("missing"), 4096));

        try (ScanCursor cursor =
                split.openScanner(config, Arrays.asList("id", "second", "first"), 4096)) {
            ScanCursor.Entry entry = cursor.nextEntry();
            assertNotNull(entry);
            assertEquals(
                    Collections.singletonList(Value.int64(7)),
                    KeyCodec.decode(
                            Collections.singletonList(LogicalTypes.int64()),
                            ByteBuffer.wrap(entry.key)));
            assertEquals(2, entry.columns.length);
            assertEquals(
                    Value.string("second"),
                    ValueCodec.decodeOwned(
                            LogicalTypes.string(), ByteBuffer.wrap(entry.columns[0])));
            assertEquals(
                    Value.string("first"),
                    ValueCodec.decodeOwned(
                            LogicalTypes.string(), ByteBuffer.wrap(entry.columns[1])));
            assertNotNull(cursor.nextEntry());
            assertNotNull(cursor.nextEntry());
            assertNull(cursor.nextEntry());
        }
        try (ScanCursor cursor = split.openScanner(config, Collections.singletonList("id"), 4096)) {
            ScanCursor.Entry entry = cursor.nextEntry();
            assertNotNull(entry);
            assertEquals(1, entry.columns.length);
            assertEquals(
                    Value.string("first"),
                    ValueCodec.decodeOwned(
                            LogicalTypes.string(), ByteBuffer.wrap(entry.columns[0])));
        }

        TableScanCursor cursor = split.openTypedScanner(config, 4096);
        List<Value> retained;
        try {
            assertEquals(
                    Arrays.asList(Value.int64(7), Value.string("first"), Value.string("second")),
                    cursor.nextRow());
            // Exceed the pooled direct buffer, then reuse it for a small row.
            retained = cursor.nextRow();
            assertEquals(largeRow, retained);
            assertEquals(tailRow, cursor.nextRow());
            assertNull(cursor.nextRow());
        } finally {
            cursor.close();
        }
        assertEquals(largeRow, retained);
        cursor.close();
        assertThrows(IllegalStateException.class, cursor::nextRow);
    }

    private static List<List<Value>> readProjected(Config config, TableScanPlan plan)
            throws Exception {
        List<List<Value>> rows = new ArrayList<List<Value>>();
        for (TableScanSplit split : plan.splits()) {
            try (TableReadProvider<List<Value>, ?> provider = plan.open(config, split);
                    TableReadSession<List<Value>, ?> session = provider.open();
                    TableReadCursor<List<Value>> cursor =
                            session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
                assertEquals(plan.readSchema().fields(), session.schema().fields());
                TableReadEntry<List<Value>> entry;
                while ((entry = cursor.next()) != null) {
                    assertTrue(entry.countsPhysicalEntry());
                    rows.add(entry.value());
                }
            }
        }
        return rows;
    }

    private static long physicalBytes(Config config, TableScanPlan plan) throws Exception {
        long bytes = 0;
        for (TableScanSplit split : plan.splits()) {
            try (TableReadProvider<List<Value>, ?> provider = plan.open(config, split);
                    TableReadSession<List<Value>, ?> session = provider.open();
                    TableReadCursor<List<Value>> cursor =
                            session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
                TableReadEntry<List<Value>> entry;
                while ((entry = cursor.next()) != null) bytes += entry.physicalBytes();
            }
        }
        return bytes;
    }

    @Test
    void projectedScanReadsNullableColumnAddedAfterExistingRows() throws Exception {
        Path dataDir = Files.createTempDirectory("cobble-java-table-scan-evolution-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema initialSchema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "old", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        List<String> namespace = Collections.singletonList("test");
        TableIdentifier identifier = new TableIdentifier(namespace, "data");
        GlobalSnapshot snapshot;
        String physicalName;
        try (FileCatalog catalog = FileCatalog.open(config, "scan-evolution");
                Db db = Db.open(config)) {
            catalog.createNamespace(namespace);
            try (CatalogTable initial = catalog.createTable(identifier, initialSchema);
                    Table writer = initial.materializeTable(db)) {
                physicalName = writer.name();
                writer.put(Arrays.asList(Value.int64(1), Value.string("existing")));
                try (CatalogTable evolved =
                        catalog.evolveSchema(
                                identifier,
                                Collections.singletonList(
                                        TableSchemaChange.addField(
                                                "added", LogicalTypes.string().nullable())))) {
                    assertTrue(evolved.refreshWriter(writer));
                    try (TableSnapshotCommitter committer =
                            TableSnapshotCommitter.open(config, 1, 1)) {
                        snapshot =
                                committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
                    }
                }
            }
        }
        TableScanPlan plan;
        try (TableReader reader = TableReader.open(config, physicalName, snapshot.id)) {
            plan = roundTrip(reader.scanPlan());
        }
        assertEquals(
                Collections.singletonList(Collections.singletonList(Value.nullValue())),
                readProjected(config, plan.project(Collections.singletonList("added"))));
        assertEquals(
                Collections.singletonList(Arrays.asList(Value.nullValue(), Value.int64(1))),
                readProjected(config, plan.project(Arrays.asList("added", "id"))));
    }

    @Test
    void nativeReaderUsesFixedTableSnapshotsAndUnifiedCapabilities() throws Exception {
        Path dataDir = Files.createTempDirectory("cobble-java-native-read-provider-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "id", LogicalTypes.int64()),
                                new DataField(2, "value", LogicalTypes.string())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        List<Value> expected = Arrays.asList(Value.int64(7), Value.string("seven"));
        GlobalSnapshot snapshot;
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema)) {
            table.put(expected);
            try (TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
                snapshot = committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
            }
        }

        TableReader reader = TableReader.open(config, "data", snapshot.id);
        TableScanSplit split = reader.scanPlan().splits().get(0);
        List<Value> key = Collections.singletonList(Value.int64(7));
        assertEquals(expected, reader.lookup(key).iterator().next().value());
        try (TableReadCursor<List<Value>> cursor =
                reader.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
            assertEquals(expected, cursor.next().value());
        }
        reader.close();
        assertThrows(IllegalStateException.class, () -> reader.lookup(key));

        try (NativeTableScanReadProvider provider = new NativeTableScanReadProvider(config, split);
                TableReadSession<List<Value>, Void> session = provider.open()) {
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> session.scan(new TableReadRange(0, 0), null));
            assertThrows(
                    UnsupportedOperationException.class,
                    () ->
                            session.scan(
                                    new TableReadRange(0, Integer.MAX_VALUE),
                                    new TableReadPosition(0, new byte[] {1}, 0)));
            try (TableReadCursor<List<Value>> cursor =
                    session.scan(new TableReadRange(0, Integer.MAX_VALUE), null)) {
                TableReadEntry<List<Value>> entry = cursor.next();
                assertNotNull(entry);
                assertEquals(expected, entry.value());
                assertTrue(entry.countsPhysicalEntry());
                assertNull(cursor.next());
            }
            session.close();
            assertThrows(
                    IllegalStateException.class,
                    () -> session.scan(new TableReadRange(0, Integer.MAX_VALUE), null));
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
}
