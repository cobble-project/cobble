package io.cobble.structured;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StructuredDbTest {

    @Test
    void typedBytesAndListPutGetRoundTrip() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-typed-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.of(100, ListRetainMode.LAST)).commit();
            Schema schema = db.currentSchema();
            assertTrue(schema.getColumnType(0) instanceof Schema.ColumnType.Bytes);
            Schema.ColumnType col1Type = schema.getColumnType(1);
            assertTrue(col1Type instanceof Schema.ColumnType.List);
            ListConfig col1Cfg = ((Schema.ColumnType.List) col1Type).getConfig();
            assertEquals(100, col1Cfg.getMaxElements().intValue());
            assertEquals(ListRetainMode.LAST, col1Cfg.getRetainMode());
            byte[] key = "row-1".getBytes(StandardCharsets.UTF_8);

            // Write bytes to column 0
            db.put(0, key, 0, ColumnValue.ofBytes("hello".getBytes(StandardCharsets.UTF_8)));

            // Write list to column 1
            byte[][] listElements =
                    new byte[][] {
                        "elem-a".getBytes(StandardCharsets.UTF_8),
                        "elem-b".getBytes(StandardCharsets.UTF_8),
                    };
            db.put(0, key, 1, ColumnValue.ofList(listElements));

            // Read back as typed Row
            Row row = db.get(0, key);
            assertNotNull(row);
            assertArrayEquals(key, row.getKey());
            assertEquals(2, row.getColumnCount());

            // Column 0: bytes
            ColumnValue col0 = row.getColumnValue(0);
            assertNotNull(col0);
            assertTrue(col0.isBytes());
            assertFalse(col0.isList());
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), col0.asBytes());
            assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), row.getBytes(0));

            // Column 1: list
            ColumnValue col1 = row.getColumnValue(1);
            assertNotNull(col1);
            assertTrue(col1.isList());
            assertFalse(col1.isBytes());
            byte[][] list = col1.asList();
            assertEquals(2, list.length);
            assertArrayEquals("elem-a".getBytes(StandardCharsets.UTF_8), list[0]);
            assertArrayEquals("elem-b".getBytes(StandardCharsets.UTF_8), list[1]);
            assertArrayEquals(list, row.getList(1));
        }
    }

    @Test
    void columnFamilyTypedRoundTripAndScan() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-cf-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema()
                    .addListColumn("metrics", 0, ListConfig.of(10, ListRetainMode.LAST))
                    .addBytesColumn("metrics", 1)
                    .commit();
            Schema schema = db.currentSchema();
            assertTrue(schema.columnFamilies().containsKey("default"));
            assertTrue(schema.columnFamilies().containsKey("metrics"));
            assertTrue(schema.getColumnType(1, 0) instanceof Schema.ColumnType.List);
            assertTrue(schema.getColumnType(1, 1) instanceof Schema.ColumnType.Bytes);

            byte[] key = "metrics-row".getBytes(StandardCharsets.UTF_8);
            db.put(
                    0,
                    key,
                    "metrics",
                    0,
                    ColumnValue.ofList(
                            new byte[][] {
                                "a".getBytes(StandardCharsets.UTF_8),
                            }));
            db.put(0, key, "metrics", 1, "m1".getBytes(StandardCharsets.UTF_8));
            db.merge(
                    0,
                    key,
                    "metrics",
                    0,
                    ColumnValue.ofList(
                            new byte[][] {
                                "b".getBytes(StandardCharsets.UTF_8),
                            }));

            Row row = db.get(0, key, "metrics");
            assertNotNull(row);
            assertEquals(2, row.getColumnCount());
            assertArrayEquals("m1".getBytes(StandardCharsets.UTF_8), row.getBytes(1));
            assertArrayEquals(
                    new byte[][] {
                        "a".getBytes(StandardCharsets.UTF_8), "b".getBytes(StandardCharsets.UTF_8),
                    },
                    row.getList(0));

            try (ReadOptions options = ReadOptions.forColumnInFamily("metrics", 0)) {
                Row projected = db.getWithOptions(0, key, options);
                assertNotNull(projected);
                assertEquals(1, projected.getColumnCount());
                assertArrayEquals(
                        new byte[][] {
                            "a".getBytes(StandardCharsets.UTF_8),
                            "b".getBytes(StandardCharsets.UTF_8),
                        },
                        projected.getList(0));
            }

            List<Row> rows = new ArrayList<Row>();
            try (ScanCursor cursor =
                    db.scan(
                            0,
                            "metrics-".getBytes(StandardCharsets.UTF_8),
                            "metrics.~".getBytes(StandardCharsets.UTF_8),
                            "metrics")) {
                ScanBatch batch = cursor.nextBatch();
                for (int i = 0; i < batch.size(); i++) {
                    rows.add(batch.getRow(i));
                }
                while (batch.hasMore) {
                    batch = cursor.nextBatch();
                    for (int i = 0; i < batch.size(); i++) {
                        rows.add(batch.getRow(i));
                    }
                }
            }
            assertEquals(1, rows.size());
            assertArrayEquals("m1".getBytes(StandardCharsets.UTF_8), rows.get(0).getBytes(1));

            db.delete(0, key, "metrics", 1);
            Row deleted = db.get(0, key, "metrics");
            assertNotNull(deleted);
            assertNull(deleted.getColumnValue(1));
        }
    }

    @Test
    void singleDbColumnFamilyTypedRoundTrip() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-single-cf-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (SingleDb db = SingleDb.open(config)) {
            db.updateSchema()
                    .addListColumn("metrics", 0, ListConfig.of(10, ListRetainMode.LAST))
                    .addBytesColumn("metrics", 1)
                    .commit();

            byte[] key = "single-cf".getBytes(StandardCharsets.UTF_8);
            try (WriteOptions options = WriteOptions.withColumnFamily("metrics")) {
                db.putWithOptions(
                        0,
                        key,
                        0,
                        ColumnValue.ofList(
                                new byte[][] {
                                    "x".getBytes(StandardCharsets.UTF_8),
                                }),
                        options);
                db.putWithOptions(
                        0,
                        key,
                        1,
                        ColumnValue.ofBytes("y".getBytes(StandardCharsets.UTF_8)),
                        options);
            }

            Row row = db.get(0, key, "metrics");
            assertNotNull(row);
            assertEquals(2, row.getColumnCount());
            assertArrayEquals("y".getBytes(StandardCharsets.UTF_8), row.getBytes(1));
            assertArrayEquals(
                    new byte[][] {
                        "x".getBytes(StandardCharsets.UTF_8),
                    },
                    row.getList(0));

            db.delete(0, key, "metrics", 1);
            Row deleted = db.get(0, key, "metrics");
            assertNotNull(deleted);
            assertNull(deleted.getColumnValue(1));
        }
    }

    @Test
    void listMergeAppendsElements() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-merge-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.of(100, ListRetainMode.LAST)).commit();
            byte[] key = "merge-key".getBytes(StandardCharsets.UTF_8);

            // Initial list put
            db.put(
                    0,
                    key,
                    1,
                    ColumnValue.ofList(
                            new byte[][] {
                                "first".getBytes(StandardCharsets.UTF_8),
                            }));

            // Merge appends
            db.merge(
                    0,
                    key,
                    1,
                    ColumnValue.ofList(
                            new byte[][] {
                                "second".getBytes(StandardCharsets.UTF_8),
                                "third".getBytes(StandardCharsets.UTF_8),
                            }));

            Row row = db.get(0, key);
            assertNotNull(row);
            byte[][] list = row.getList(1);
            assertEquals(3, list.length);
            assertArrayEquals("first".getBytes(StandardCharsets.UTF_8), list[0]);
            assertArrayEquals("second".getBytes(StandardCharsets.UTF_8), list[1]);
            assertArrayEquals("third".getBytes(StandardCharsets.UTF_8), list[2]);
        }
    }

    @Test
    void deleteAndAbsentColumnReturnsNull() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-delete-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.defaults()).commit();
            byte[] key = "del-key".getBytes(StandardCharsets.UTF_8);

            // Put only column 0
            db.put(0, key, 0, ColumnValue.ofBytes("val".getBytes(StandardCharsets.UTF_8)));

            Row row = db.get(0, key);
            assertNotNull(row);
            assertNotNull(row.getBytes(0));
            assertNull(row.getColumnValue(1)); // column 1 not written

            // Delete column 0
            db.delete(0, key, 0);

            Row afterDelete = db.get(0, key);
            // After deleting the only non-null column, row may still exist but be empty or null
            if (afterDelete != null) {
                assertNull(afterDelete.getBytes(0));
            }

            // Non-existent key returns null
            assertNull(db.get(0, "no-such-key".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void typedScanCursorYieldsTypedRows() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.of(50, ListRetainMode.LAST)).commit();
            int count = 50;
            for (int i = 0; i < count; i++) {
                byte[] key = String.format("scan-%04d", i).getBytes(StandardCharsets.UTF_8);
                db.put(
                        0,
                        key,
                        0,
                        ColumnValue.ofBytes(("bytes-" + i).getBytes(StandardCharsets.UTF_8)));
                // Use merge for column 1 so it does not overwrite column 0
                db.merge(
                        0,
                        key,
                        1,
                        ColumnValue.ofList(
                                new byte[][] {
                                    ("el-" + i).getBytes(StandardCharsets.UTF_8),
                                }));
            }

            // Sanity check: verify get returns both columns for first key
            Row getRow = db.get(0, String.format("scan-%04d", 0).getBytes(StandardCharsets.UTF_8));
            assertNotNull(getRow, "get returned null for scan-0000");
            assertNotNull(
                    getRow.getColumnValue(0),
                    "get: column 0 null; colcount=" + getRow.getColumnCount());
            assertNotNull(
                    getRow.getColumnValue(1),
                    "get: column 1 null; colcount=" + getRow.getColumnCount());

            // Full range scan
            byte[] start = "scan-".getBytes(StandardCharsets.UTF_8);
            byte[] end = "scan-~".getBytes(StandardCharsets.UTF_8);

            List<Row> allRows = new ArrayList<Row>();
            try (ScanCursor cursor = db.scan(0, start, end)) {
                ScanBatch batch = cursor.nextBatch();
                // Check raw batch structure
                assertTrue(batch.size() > 0, "batch should have at least one row");
                assertEquals(
                        2,
                        batch.rawColumns[0].length,
                        "raw columns for first row should have 2 entries");
                for (int i = 0; i < batch.size(); i++) {
                    allRows.add(batch.getRow(i));
                }
                while (batch.hasMore) {
                    batch = cursor.nextBatch();
                    for (int i = 0; i < batch.size(); i++) {
                        allRows.add(batch.getRow(i));
                    }
                }
            }

            assertEquals(count, allRows.size());

            // Debug: check first row details
            Row firstRow = allRows.get(0);
            int colCount = firstRow.getColumnCount();
            assertEquals(2, colCount, "first scan row should have 2 columns");

            // Verify typed content
            for (int i = 0; i < count; i++) {
                Row row = allRows.get(i);
                String expectedKey = String.format("scan-%04d", i);
                assertArrayEquals(expectedKey.getBytes(StandardCharsets.UTF_8), row.getKey());

                // Column 0: bytes
                assertNotNull(
                        row.getColumnValue(0),
                        "column 0 null for key " + expectedKey + " at index " + i);
                assertTrue(row.getColumnValue(0).isBytes());
                assertArrayEquals(("bytes-" + i).getBytes(StandardCharsets.UTF_8), row.getBytes(0));

                // Column 1: list
                assertNotNull(
                        row.getColumnValue(1),
                        "column 1 null for key "
                                + expectedKey
                                + " at index "
                                + i
                                + " colCount="
                                + row.getColumnCount()
                                + " col0="
                                + row.getColumnValue(0));
                assertTrue(row.getColumnValue(1).isList());
                byte[][] list = row.getList(1);
                assertEquals(1, list.length);
                assertArrayEquals(("el-" + i).getBytes(StandardCharsets.UTF_8), list[0]);
            }
        }
    }

    @Test
    void mixedSchemaWithDefaultBytesColumns() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-default-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(2, ListConfig.of(10, ListRetainMode.FIRST)).commit();
            byte[] key = "mixed".getBytes(StandardCharsets.UTF_8);

            // Write all three columns
            db.put(0, key, 0, "col0".getBytes(StandardCharsets.UTF_8));
            db.put(0, key, 1, "col1".getBytes(StandardCharsets.UTF_8));
            db.put(
                    0,
                    key,
                    2,
                    ColumnValue.ofList(
                            new byte[][] {
                                "x".getBytes(StandardCharsets.UTF_8),
                                "y".getBytes(StandardCharsets.UTF_8),
                            }));

            Row row = db.get(0, key);
            assertNotNull(row);
            assertEquals(3, row.getColumnCount());

            // Columns 0 and 1: bytes (default schema)
            assertArrayEquals("col0".getBytes(StandardCharsets.UTF_8), row.getBytes(0));
            assertArrayEquals("col1".getBytes(StandardCharsets.UTF_8), row.getBytes(1));

            // Column 2: list
            byte[][] list = row.getList(2);
            assertEquals(2, list.length);
            assertArrayEquals("x".getBytes(StandardCharsets.UTF_8), list[0]);
            assertArrayEquals("y".getBytes(StandardCharsets.UTF_8), list[1]);
        }
    }

    @Test
    void shrinkAndExpandBucketRoundTripKeepsStructuredData() throws IOException {
        Path sharedDir = Files.createTempDirectory("cobble-structured-rescale-shared-");
        Config config = new Config().addVolume(sharedDir.toString()).numColumns(1).totalBuckets(4);

        try (Db source = Db.open(config, 2, 3)) {
            source.updateSchema().addBytesColumn("value-state", 0).commit();
            source.put(
                    2,
                    "bucket-2".getBytes(StandardCharsets.UTF_8),
                    "value-state",
                    0,
                    "value-2".getBytes(StandardCharsets.UTF_8));
            ShardSnapshot snapshot = source.snapshot();
            assertTrue(source.retainSnapshot(snapshot.snapshotId));

            try (Db target = Db.open(config, 0, 1)) {
                target.updateSchema().addBytesColumn("value-state", 0).commit();
                target.put(
                        0,
                        "bucket-0".getBytes(StandardCharsets.UTF_8),
                        "value-state",
                        0,
                        "value-0".getBytes(StandardCharsets.UTF_8));

                assertTrue(
                        target.expandBucket(
                                        source.id(),
                                        snapshot.snapshotId,
                                        new int[] {2},
                                        new int[] {3})
                                >= 0L);
                Row expanded =
                        target.get(2, "bucket-2".getBytes(StandardCharsets.UTF_8), "value-state");
                assertNotNull(expanded);
                assertArrayEquals("value-2".getBytes(StandardCharsets.UTF_8), expanded.getBytes(0));

                assertTrue(target.shrinkBucket(new int[] {2}, new int[] {3}) >= 0L);
                assertNotNull(
                        target.get(0, "bucket-0".getBytes(StandardCharsets.UTF_8), "value-state"));
                assertNull(
                        target.get(2, "bucket-2".getBytes(StandardCharsets.UTF_8), "value-state"));
            }
        }
    }

    @Test
    void openWithBucketRangeLimitsOwnedBuckets() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-open-range-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(4);

        try (Db db = Db.open(config, 1, 2)) {
            db.updateSchema().addBytesColumn("value-state", 0).commit();
            db.put(
                    1,
                    "bucket-1".getBytes(StandardCharsets.UTF_8),
                    "value-state",
                    0,
                    "value-1".getBytes(StandardCharsets.UTF_8));
            ShardSnapshot snapshot = db.snapshot();
            assertEquals(1, snapshot.ranges.size());
            assertEquals(1, snapshot.ranges.get(0).start);
            assertEquals(2, snapshot.ranges.get(0).end);
        }
    }

    @Test
    void listRetainLastCapEnforced() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-cap-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(0).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(0, ListConfig.of(3, ListRetainMode.LAST)).commit();
            byte[] key = "cap-key".getBytes(StandardCharsets.UTF_8);

            // Put initial list of 2
            db.put(
                    0,
                    key,
                    0,
                    ColumnValue.ofList(
                            new byte[][] {
                                "a".getBytes(StandardCharsets.UTF_8),
                                "b".getBytes(StandardCharsets.UTF_8),
                            }));

            // Merge 3 more (total 5, should be capped to last 3)
            db.merge(
                    0,
                    key,
                    0,
                    ColumnValue.ofList(
                            new byte[][] {
                                "c".getBytes(StandardCharsets.UTF_8),
                                "d".getBytes(StandardCharsets.UTF_8),
                                "e".getBytes(StandardCharsets.UTF_8),
                            }));

            Row row = db.get(0, key);
            assertNotNull(row);
            byte[][] list = row.getList(0);
            assertEquals(3, list.length);
            // Retain last: should keep c, d, e
            assertArrayEquals("c".getBytes(StandardCharsets.UTF_8), list[0]);
            assertArrayEquals("d".getBytes(StandardCharsets.UTF_8), list[1]);
            assertArrayEquals("e".getBytes(StandardCharsets.UTF_8), list[2]);
        }
    }

    @Test
    void structuredDistributedScanPlanSplitCursorRoundTrip() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-dist-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.of(50, ListRetainMode.LAST)).commit();
            int count = 40;
            for (int i = 0; i < count; i++) {
                byte[] key = String.format("dscan-%04d", i).getBytes(StandardCharsets.UTF_8);
                db.put(
                        0,
                        key,
                        0,
                        ColumnValue.ofBytes(("bytes-" + i).getBytes(StandardCharsets.UTF_8)));
                db.merge(
                        0,
                        key,
                        1,
                        ColumnValue.ofList(
                                new byte[][] {
                                    ("el-" + i).getBytes(StandardCharsets.UTF_8),
                                }));
            }

            ShardSnapshot shardSnapshot = db.snapshot();
            assertNotNull(shardSnapshot);
            assertNotNull(shardSnapshot.columnFamilyIds);
            assertEquals(Integer.valueOf(0), shardSnapshot.columnFamilyIds.get("default"));
            assertTrue(db.retainSnapshot(shardSnapshot.snapshotId));

            GlobalSnapshot globalSnapshot;
            try (DbCoordinator coordinator = DbCoordinator.open(config)) {
                globalSnapshot =
                        coordinator.materializeGlobalSnapshot(
                                1,
                                shardSnapshot.snapshotId,
                                java.util.Collections.singletonList(shardSnapshot));
            }
            assertNotNull(globalSnapshot);
            assertNotNull(globalSnapshot.columnFamilyIds);
            assertEquals(Integer.valueOf(0), globalSnapshot.columnFamilyIds.get("default"));

            StructuredScanPlan plan =
                    StructuredScanPlan.fromGlobalSnapshot(globalSnapshot)
                            .withStart("dscan-0010".getBytes(StandardCharsets.UTF_8))
                            .withEnd("dscan-0030".getBytes(StandardCharsets.UTF_8));
            java.util.List<StructuredScanSplit> splits = plan.splits();
            assertEquals(1, splits.size());

            StructuredScanSplit split = StructuredScanSplit.fromJson(splits.get(0).toJson());
            try (ScanOptions options = new ScanOptions().batchSize(7).columns(1);
                    ScanCursor cursor = split.openScannerWithOptions(config, options)) {
                java.util.List<Row> rows = new ArrayList<Row>();
                for (Row row : cursor) {
                    rows.add(row);
                }
                assertEquals(20, rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    int expected = 10 + i;
                    Row row = rows.get(i);
                    assertArrayEquals(
                            String.format("dscan-%04d", expected).getBytes(StandardCharsets.UTF_8),
                            row.getKey());
                    assertEquals(1, row.getColumnCount());
                    assertNotNull(row.getColumnValue(0));
                    assertTrue(row.getColumnValue(0).isList());
                    byte[][] list = row.getList(0);
                    assertEquals(1, list.length);
                    assertArrayEquals(("el-" + expected).getBytes(StandardCharsets.UTF_8), list[0]);
                }
            }
        }
    }

    @Test
    void structuredDirectBufferApisHandleFitAndOverflow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-direct-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        byte[] smallKey = "direct-small".getBytes(StandardCharsets.UTF_8);
        byte[] largeKey = "direct-large".getBytes(StandardCharsets.UTF_8);
        byte[] smallValue = "small-structured-value".getBytes(StandardCharsets.UTF_8);
        byte[] largeValue = new byte[4096];
        for (int i = 0; i < largeValue.length; i++) {
            largeValue[i] = (byte) ('a' + (i % 26));
        }

        try (Db db = Db.open(config)) {
            db.put(0, smallKey, 0, ColumnValue.ofBytes(smallValue));
            db.put(0, largeKey, 0, ColumnValue.ofBytes(largeValue));

            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow small = db.getDirectWithOptions(0, smallKey, options)) {
                assertNotNull(small);
                assertArrayEquals(smallValue, readDirectBytes(small.getBytes(0)));
            }

            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow large = db.getDirectWithOptions(0, largeKey, options)) {
                assertNotNull(large);
                assertArrayEquals(largeValue, readDirectBytes(large.getBytes(0)));
            }

            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(largeKey.length);
            ((Buffer) keyBuffer).clear();
            keyBuffer.put(largeKey);
            ((Buffer) keyBuffer).flip();
            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow large = db.getDirectWithOptions(0, keyBuffer, options)) {
                assertNotNull(large);
                assertArrayEquals(largeValue, readDirectBytes(large.getBytes(0)));
            }

            ByteBuffer paddedKeyBuffer = ByteBuffer.allocateDirect(largeKey.length + 32);
            ((Buffer) paddedKeyBuffer).clear();
            paddedKeyBuffer.put(largeKey);
            paddedKeyBuffer.put((byte) 'x');
            ((Buffer) paddedKeyBuffer).position(paddedKeyBuffer.capacity());
            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow large =
                            db.getDirectWithOptions(0, paddedKeyBuffer, largeKey.length, options)) {
                assertNotNull(large);
                assertArrayEquals(largeValue, readDirectBytes(large.getBytes(0)));
            }
        }
    }

    @Test
    void structuredDirectEncodedListWriteAndMergeHandleFitAndOverflow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-direct-list-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        byte[] key = "direct-list".getBytes(StandardCharsets.UTF_8);
        byte[] smallElement = "direct-list-small".getBytes(StandardCharsets.UTF_8);
        byte[] largeElement = largeValueBytes("direct-list-large", 0, 4096);
        byte[] tailElement = "direct-list-tail".getBytes(StandardCharsets.UTF_8);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(0, ListConfig.of(10, ListRetainMode.LAST)).commit();

            ByteBuffer keyBuffer = directBufferOf(key);
            DirectListValueBuilder builder = new DirectListValueBuilder(256);
            builder.beginElement();
            builder.outputStream().write(smallElement);
            builder.finishElement();
            db.putEncodedListDirectWithOptions(
                    0, keyBuffer, key.length, 0, builder.buffer(), builder.length(), null);

            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow row = db.getDirectWithOptions(0, keyBuffer, key.length, options)) {
                assertNotNull(row);
                assertFalse(row.isNull(0));
                assertEquals(1, row.getListElementCount(0));
                assertArrayEquals(smallElement, readDirectBytes(row.getListElement(0, 0)));
            }

            builder.clear();
            builder.append(directBufferOf(largeElement), largeElement.length);
            builder.append(tailElement);
            db.mergeEncodedListDirectWithOptions(
                    0, keyBuffer, key.length, 0, builder.buffer(), builder.length(), null);

            Row typed = db.get(0, key);
            assertNotNull(typed);
            byte[][] list = typed.getList(0);
            assertEquals(3, list.length);
            assertArrayEquals(smallElement, list[0]);
            assertArrayEquals(largeElement, list[1]);
            assertArrayEquals(tailElement, list[2]);

            try (ReadOptions options = ReadOptions.forColumns(0);
                    DirectRow row = db.getDirectWithOptions(0, keyBuffer, key.length, options)) {
                assertNotNull(row);
                assertFalse(row.isNull(0));
                assertEquals(3, row.getListElementCount(0));
                assertArrayEquals(smallElement, readDirectBytes(row.getListElement(0, 0)));
                assertArrayEquals(largeElement, readDirectBytes(row.getListElement(0, 1)));
                assertArrayEquals(tailElement, readDirectBytes(row.getListElement(0, 2)));
            }
        }
    }

    @Test
    void structuredDirectEncodedListWriteRejectsBytesColumn() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-direct-list-column-check-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        byte[] key = "direct-list-bytes-column".getBytes(StandardCharsets.UTF_8);
        try (Db db = Db.open(config)) {
            ByteBuffer keyBuffer = directBufferOf(key);
            DirectListValueBuilder builder = new DirectListValueBuilder(64);
            builder.append("value".getBytes(StandardCharsets.UTF_8));

            IllegalStateException putError =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    db.putEncodedListDirectWithOptions(
                                            0,
                                            keyBuffer,
                                            key.length,
                                            0,
                                            builder.buffer(),
                                            builder.length(),
                                            null));
            assertTrue(putError.getMessage().contains("not a LIST column"));

            IllegalStateException mergeError =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    db.mergeEncodedListDirectWithOptions(
                                            0,
                                            keyBuffer,
                                            key.length,
                                            0,
                                            builder.buffer(),
                                            builder.length(),
                                            null));
            assertTrue(mergeError.getMessage().contains("not a LIST column"));
        }
    }

    @Test
    void structuredDirectScanCursorHandlesFitAndOverflow() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-direct-scan-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            for (int i = 0; i < 8; i++) {
                byte[] key = String.format("direct-scan-%02d", i).getBytes(StandardCharsets.UTF_8);
                byte[] value =
                        i == 4
                                ? largeValueBytes("direct-scan-large", i, 4096)
                                : ("direct-scan-value-" + i).getBytes(StandardCharsets.UTF_8);
                db.put(0, key, 0, ColumnValue.ofBytes(value));
            }

            byte[] start = "direct-scan-02".getBytes(StandardCharsets.UTF_8);
            byte[] end = "direct-scan-07".getBytes(StandardCharsets.UTF_8);
            ByteBuffer startBuffer = ByteBuffer.allocateDirect(start.length + 8);
            ByteBuffer endBuffer = ByteBuffer.allocateDirect(end.length + 8);
            ((Buffer) startBuffer).clear();
            startBuffer.put(start);
            ((Buffer) endBuffer).clear();
            endBuffer.put(end);

            try (ScanOptions options = new ScanOptions().batchSize(2).columns(0);
                    DirectScanCursor cursor =
                            db.scanDirectWithOptions(
                                    0, startBuffer, start.length, endBuffer, end.length, options)) {
                java.util.List<byte[]> seenKeys = new ArrayList<>();
                java.util.List<byte[]> seenValues = new ArrayList<>();
                for (DirectScanRow row : cursor) {
                    seenKeys.add(readDirectBytes(row.getKey()));
                    seenValues.add(readDirectBytes(row.getBytes(0)));
                }

                assertEquals(5, seenKeys.size());
                for (int i = 0; i < seenKeys.size(); i++) {
                    int expected = i + 2;
                    assertArrayEquals(
                            String.format("direct-scan-%02d", expected)
                                    .getBytes(StandardCharsets.UTF_8),
                            seenKeys.get(i));
                    byte[] expectedValue =
                            expected == 4
                                    ? largeValueBytes("direct-scan-large", expected, 4096)
                                    : ("direct-scan-value-" + expected)
                                            .getBytes(StandardCharsets.UTF_8);
                    assertArrayEquals(expectedValue, seenValues.get(i));
                }
            }
        }
    }

    @Test
    void structuredDirectScanSupportsNullOptionsAndBatchTraversal() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-structured-direct-scan-null-opt-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config)) {
            for (int i = 0; i < 3; i++) {
                byte[] key =
                        String.format("st-direct-null-%02d", i).getBytes(StandardCharsets.UTF_8);
                db.put(
                        0,
                        key,
                        0,
                        ColumnValue.ofBytes(("st-value-" + i).getBytes(StandardCharsets.UTF_8)));
            }

            byte[] start = "st-direct-null-00".getBytes(StandardCharsets.UTF_8);
            byte[] end = "st-direct-null-03".getBytes(StandardCharsets.UTF_8);
            ByteBuffer startBuffer = ByteBuffer.allocateDirect(start.length + 8);
            ByteBuffer endBuffer = ByteBuffer.allocateDirect(end.length + 8);
            ((Buffer) startBuffer).clear();
            startBuffer.put(start);
            ((Buffer) endBuffer).clear();
            endBuffer.put(end);

            try (DirectScanCursor cursor =
                    db.scanDirectWithOptions(
                            0, startBuffer, start.length, endBuffer, end.length, null)) {
                DirectScanBatch batch1 = cursor.nextBatch();
                assertEquals(3, batch1.size());
                assertFalse(batch1.hasMore);
                assertArrayEquals(start, readDirectBytes(batch1.getRow(0).getKey()));
                assertArrayEquals(
                        "st-value-2".getBytes(StandardCharsets.UTF_8),
                        readDirectBytes(batch1.getRow(2).getBytes(0)));

                DirectScanBatch batch2 = cursor.nextBatch();
                assertEquals(0, batch2.size());
                assertFalse(batch2.hasMore);
            }
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static byte[] readDirectBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static ByteBuffer directBufferOf(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        ((Buffer) buffer).clear();
        buffer.put(bytes);
        ((Buffer) buffer).flip();
        return buffer;
    }

    private static byte[] largeValueBytes(String prefix, int seed, int length) {
        byte[] bytes = new byte[length];
        byte[] prefixBytes = (prefix + "-" + seed + "-").getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = prefixBytes[i % prefixBytes.length];
        }
        return bytes;
    }
}
