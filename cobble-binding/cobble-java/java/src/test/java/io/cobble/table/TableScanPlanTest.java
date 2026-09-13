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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TableScanPlanTest {

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
            plan = TableScanPlan.forSnapshot(config, "data", global.id);

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
        assertEquals(latest.id, TableScanPlan.forCurrentSnapshot(config, "data").snapshotId());

        plan = roundTrip(plan);

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
