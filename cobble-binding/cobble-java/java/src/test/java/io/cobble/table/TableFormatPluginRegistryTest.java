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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableFormatPluginRegistryTest {
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

        TableScanPlan plan;
        try (TableReader reader = TableReader.open(config, "data", global.id)) {
            plan = roundTrip(reader.scanPlan());
        }
        assertEquals(Arrays.asList("id", "value"), names(plan.readSchema()));
        assertEquals(1, plan.splits().size());
        assertEquals(global.id, plan.snapshotId());
        assertEquals(1, plan.totalBuckets());
        try (TableReader reader = TableReader.open(config, "data", global.id)) {
            assertEquals(plan.readSchema().fields(), reader.scanPlan().readSchema().fields());
        }
        try (TableReader reader = TableReader.openCurrent(config, "data")) {
            assertEquals(global.id, reader.scanPlan().snapshotId());
        }
        TableScanSplit split = roundTrip(plan.splits().get(0));
        assertEquals("cobble-table", split.formatId());

        TableScanPlan countPlan = plan.project(Collections.<String>emptyList());
        assertEquals(schema, countPlan.schema());
        assertEquals(plan.snapshotId(), countPlan.forSplit(plan.splits().get(0)).snapshotId());
        assertThrows(
                IllegalArgumentException.class,
                () -> plan.project(Collections.singletonList("missing")));
        assertThrows(IllegalArgumentException.class, () -> plan.project(Arrays.asList("id", "id")));
        io.cobble.ShardSnapshot foreignShard = split.shardSnapshot();
        foreignShard.snapshotId++;
        TableScanSplit foreign =
                new TableScanSplit(
                        split.formatId(), split.columnFamily(), foreignShard, split.metadataJson());
        assertThrows(IllegalArgumentException.class, () -> countPlan.open(config, foreign));
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
        TableFormatPluginRegistry.Registry registry =
                TableFormatPluginRegistry.fromPlugins(
                        Collections.singletonList(new StubFactory("one")));
        assertThrows(IllegalArgumentException.class, () -> registry.resolve("missing"));
        assertThrows(
                IllegalStateException.class,
                () ->
                        TableFormatPluginRegistry.fromPlugins(
                                Arrays.asList(new StubFactory("one"), new StubFactory("one"))));
    }

    @Test
    void pathResolutionTreatsPlainAndFileUriAsTheSameNativeRoot() throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Collections.singletonList(new DataField(1, "id", LogicalTypes.int64())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        try (Db db = Db.open(config);
                Table table = Table.create(db, "data", schema);
                TableSnapshotCommitter committer = TableSnapshotCommitter.open(config, 1, 1)) {
            table.put(Collections.singletonList(Value.int64(1)));
            committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
        }

        try (TableReader reader =
                TableReader.open(
                        config,
                        new TablePathRequest(
                                dataDir.toUri().toString(),
                                "data",
                                null,
                                Collections.<String, String>emptyMap()))) {
            assertEquals("cobble-table", reader.scanPlan().formatId());
            assertEquals(
                    Collections.singletonList(Value.int64(1)),
                    reader.get(reader.keyBuilder().push(Value.int64(1)).build()));
        }

        Config empty = new Config().addVolume(dataDir.resolve("empty").toString());
        assertThrows(
                TablePathMissingSnapshotException.class,
                () ->
                        TableReader.open(
                                empty,
                                new TablePathRequest(
                                        dataDir.resolve("empty").toUri().toString(),
                                        "data",
                                        null,
                                        Collections.<String, String>emptyMap())));
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
        TableScanPlan plan;
        try (TableReader reader = TableReader.open(config, "data", global.id)) {
            plan = reader.scanPlan();
        }
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

        TableScanPlan plan;
        try (TableReader reader = TableReader.open(config, "data", global.id)) {
            plan = reader.scanPlan();
        }
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
        byte[][] decoderKeyBeforeMutation = new byte[1][];
        try (io.cobble.DirectScanCursor direct = split.openDirectScanner(config, names, 0);
                TableDirectScanCursor<List<Value>> cursor =
                        TableDirectScanCursor.fixed(
                                direct,
                                (entry, key) -> {
                                    decoderKeyBeforeMutation[0] = Arrays.copyOf(key, key.length);
                                    key[0] ^= 1;
                                    return Arrays.asList(
                                            Collections.singletonList(Value.int64(10)),
                                            Collections.singletonList(Value.int64(20)));
                                })) {
            TableReadEntry<List<Value>> first = cursor.next();
            TableReadEntry<List<Value>> second = cursor.next();
            byte[] firstPublicKey = first.position().physicalKey();
            assertArrayEquals(decoderKeyBeforeMutation[0], firstPublicKey);
            assertArrayEquals(second.position().physicalKey(), firstPublicKey);
            firstPublicKey[0] ^= 1;
            assertArrayEquals(second.position().physicalKey(), first.position().physicalKey());
        }
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

    @Test
    void physicalRangeTraversalSortsAndDeduplicatesReorderedShardRanges() {
        io.cobble.ShardSnapshot.Range late = new io.cobble.ShardSnapshot.Range();
        late.start = 2;
        late.end = 3;
        io.cobble.ShardSnapshot.Range early = new io.cobble.ShardSnapshot.Range();
        early.start = 0;
        early.end = 2;
        assertEquals(
                Arrays.asList(0, 1, 2, 3),
                PhysicalTableReadSession.selectedBuckets(
                        4, new TableReadRange(0, Integer.MAX_VALUE), Arrays.asList(late, early)));
    }

    @Test
    void physicalConfigPreservesRealVolumesAndDerivesOnlyMissingSnapshotRoot() throws Exception {
        Config storage = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        TableSchema schema =
                new TableSchema(
                        Collections.singletonList(new DataField(1, "id", LogicalTypes.int64())),
                        Collections.singletonList(1L),
                        Collections.singletonList(1L));
        GlobalSnapshot global;
        try (Db db = Db.open(storage);
                Table table = Table.create(db, "data", schema);
                TableSnapshotCommitter committer = TableSnapshotCommitter.open(storage, 1, 1)) {
            table.put(Collections.singletonList(Value.int64(1)));
            global = committer.commitBatch(1L, Collections.singletonList(db.snapshot()));
        }
        TableReadSnapshot snapshot = TableReadSnapshot.forGlobal(storage, global, "data");

        Config preserved = storage.copy();
        Config.VolumeDescriptor unrelated =
                Config.VolumeDescriptor.singleVolume(dataDir.resolve("other-volume").toString());
        unrelated.accessId = "other-access";
        unrelated.secretKey = "other-secret";
        preserved.addVolume(unrelated);
        Config unchanged = PhysicalTableReadSession.physicalConfig(preserved, snapshot);
        assertEquals(2, unchanged.volumes.size());
        assertEquals("other-access", unchanged.volumes.get(1).accessId);

        Config leafInput = new Config();
        Config.VolumeDescriptor leaf =
                Config.VolumeDescriptor.singleVolume(dataDir.resolve("_metadata").toString());
        leaf.accessId = "checkpoint-access";
        leaf.secretKey = "checkpoint-secret";
        leafInput.addVolume(leaf);
        Config derived = PhysicalTableReadSession.physicalConfig(leafInput, snapshot);
        assertEquals(2, derived.volumes.size());
        Config.VolumeDescriptor derivedRoot = derived.volumes.get(1);
        assertEquals(
                dataDir.toAbsolutePath().normalize(),
                Path.of(derivedRoot.baseDir).toAbsolutePath().normalize());
        assertEquals("checkpoint-access", derivedRoot.accessId);
        assertEquals("checkpoint-secret", derivedRoot.secretKey);

        Config manifestInput = new Config();
        Config.VolumeDescriptor manifestVolume =
                Config.VolumeDescriptor.singleVolume(global.shardSnapshots.get(0).manifestPath);
        manifestVolume.accessId = "manifest-access";
        manifestVolume.secretKey = "manifest-secret";
        manifestInput.addVolume(manifestVolume);
        Config repaired = PhysicalTableReadSession.physicalConfig(manifestInput, snapshot);
        assertEquals(1, repaired.volumes.size());
        Config.VolumeDescriptor repairedRoot = repaired.volumes.get(0);
        assertEquals(
                dataDir.toAbsolutePath().normalize(),
                Path.of(repairedRoot.baseDir).toAbsolutePath().normalize());
        assertEquals("manifest-access", repairedRoot.accessId);
        assertEquals("manifest-secret", repairedRoot.secretKey);
        try (io.cobble.Reader view = io.cobble.Reader.open(repaired, global);
                io.cobble.ScanOptions scan = new io.cobble.ScanOptions().columnFamily("data");
                io.cobble.DirectScanCursor cursor =
                        view.scanDirectWithOptions(0, null, null, scan)) {
            assertTrue(cursor.nextEntry() != null, "derived root must open the shard manifest");
        }
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

    private static final class StubFactory implements TableFormatPlugin {
        private final String formatId;

        private StubFactory(String formatId) {
            this.formatId = formatId;
        }

        @Override
        public String formatId() {
            return formatId;
        }

        @Override
        public TableFormatBinding bind(TableReadSnapshot snapshot) {
            throw new UnsupportedOperationException();
        }
    }
}
