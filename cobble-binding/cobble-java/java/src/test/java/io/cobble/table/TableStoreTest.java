package io.cobble.table;

import io.cobble.Config;
import io.cobble.ImportStorageMode;
import io.cobble.RescaleOptions;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableStoreTest {
    private static final List<String> TABLES = Arrays.asList("left", "right");
    private static final TableSchema SCHEMA =
            new TableSchema(
                    Arrays.asList(
                            new DataField(1L, "id", LogicalTypes.int64()),
                            new DataField(2L, "value", LogicalTypes.string())),
                    Collections.singletonList(1L),
                    Collections.singletonList(1L));

    @TempDir Path root;

    @Test
    void rescalesAllTablesWithExclusiveHandlesAndOptionalSnapshots() {
        Config config = new Config().addVolume(root.toUri().toString()).totalBuckets(4);
        config.snapshotOnFlush = false;
        config.snapshotRetention = 128;
        config.walEnabled = false;
        try (TableStore full = TableStore.open(config)) {
            assertRange(full.snapshot(), 0, 3);
        }

        int[] movedStarts = {2};
        int[] movedEnds = {3};
        try (TableStore source = TableStore.open(config, 2, 3);
                TableStore target =
                        TableStore.open(config, new int[] {0}, new int[] {1}, "target")) {
            assertEquals("target", target.id());
            assertNotEquals(target.id(), source.id());
            TableKey moved = writeRows(source, 2, "source:");
            TableKey kept = writeRows(target, 0, "target:");
            ShardSnapshot sourceSnapshot = source.snapshot();
            assertEquals(source.id(), sourceSnapshot.dbId);
            assertRange(sourceSnapshot, 2, 3);

            int[] all = {};
            LongSupplier expandDefault =
                    () ->
                            target.expandBucketFromManifest(
                                    source.id(),
                                    sourceSnapshot.manifestPath,
                                    all,
                                    all,
                                    ImportStorageMode.ADOPT_ASYNC);
            try (Table table = target.openTable("left")) {
                assertHandlesLive(expandDefault::getAsLong);
                try (TableProjection projection =
                        table.projectByNames(Collections.singletonList("value"))) {
                    table.close();
                    assertHandlesLive(expandDefault::getAsLong);
                    try (TableScanCursor scanner = projection.scan(0)) {
                        projection.close();
                        assertHandlesLive(expandDefault::getAsLong);
                    }
                }
            }

            assertEquals(sourceSnapshot.snapshotId, expandDefault.getAsLong());
            target.waitForImportAdoption(10_000L);
            assertRows(target, kept, moved, true);
            try (Table table = target.openTable("left")) {
                assertHandlesLive(() -> target.shrinkBucket(movedStarts, movedEnds));
            }
            long defaultPreShrink = target.shrinkBucket(movedStarts, movedEnds);
            assertRange(target.getShardSnapshot(defaultPreShrink), 0, 3);
            assertRange(target.getShardSnapshot(defaultPreShrink + 1), 0, 1);
            assertRows(target, kept, moved, false);

            for (int mask = 0; mask < 4; mask++) {
                RescaleOptions options = new RescaleOptions();
                options.snapshotBefore = (mask & 1) != 0;
                options.snapshotAfter = (mask & 2) != 0;
                int immediateSnapshots =
                        (options.snapshotBefore ? 1 : 0) + (options.snapshotAfter ? 1 : 0);
                long before = target.snapshot().snapshotId;
                assertEquals(
                        sourceSnapshot.snapshotId,
                        target.expandBucketFromManifest(
                                source.id(),
                                sourceSnapshot.manifestPath,
                                movedStarts,
                                movedEnds,
                                ImportStorageMode.REFERENCE_PERSISTENT,
                                options));
                assertRows(target, kept, moved, true);
                ShardSnapshot expanded = target.snapshot();
                assertEquals(before + immediateSnapshots + 1, expanded.snapshotId);
                assertRange(expanded, 0, 3);
                if (options.snapshotBefore) {
                    assertRange(target.getShardSnapshot(before + 1), 0, 1);
                }
                if (options.snapshotAfter) {
                    assertRange(target.getShardSnapshot(expanded.snapshotId - 1), 0, 3);
                }

                long preShrink = target.shrinkBucket(movedStarts, movedEnds, options);
                assertEquals(options.snapshotBefore ? expanded.snapshotId + 1 : -1L, preShrink);
                if (options.snapshotBefore) {
                    assertRange(target.getShardSnapshot(preShrink), 0, 3);
                }
                assertRows(target, kept, moved, false);
                ShardSnapshot shrunk = target.snapshot();
                assertEquals(expanded.snapshotId + immediateSnapshots + 1, shrunk.snapshotId);
                assertRange(shrunk, 0, 1);
                if (options.snapshotAfter) {
                    assertRange(target.getShardSnapshot(shrunk.snapshotId - 1), 0, 1);
                }
            }
            target.close();
            assertThrows(IllegalStateException.class, target::id);
        }
    }

    private static TableKey writeRows(TableStore store, int bucket, String prefix) {
        TableKey key = null;
        for (String name : TABLES) {
            try (Table table = store.createTable(name, SCHEMA)) {
                if (key == null) {
                    for (long id = 0; ; id++) {
                        TableKey candidate = table.keyBuilder().push(Value.int64(id)).build();
                        if (candidate.bucket() == bucket) {
                            key = candidate;
                            break;
                        }
                    }
                }
                table.putValues(key, Collections.singletonList(Value.string(prefix + name)));
            }
        }
        return key;
    }

    private static void assertRows(
            TableStore store, TableKey kept, TableKey moved, boolean expanded) {
        for (String name : TABLES) {
            try (Table table = store.openTable(name)) {
                assertEquals(
                        Collections.singletonList(Value.string("target:" + name)),
                        table.getValues(kept));
                assertEquals(
                        expanded ? Collections.singletonList(Value.string("source:" + name)) : null,
                        table.getValues(moved));
            }
        }
    }

    private static void assertRange(ShardSnapshot snapshot, int start, int end) {
        assertEquals(1, snapshot.ranges.size());
        assertEquals(start, snapshot.ranges.get(0).start);
        assertEquals(end, snapshot.ranges.get(0).end);
    }

    private static void assertHandlesLive(Executable operation) {
        IllegalStateException error = assertThrows(IllegalStateException.class, operation);
        assertTrue(error.getMessage().contains("release all"), error::getMessage);
    }
}
