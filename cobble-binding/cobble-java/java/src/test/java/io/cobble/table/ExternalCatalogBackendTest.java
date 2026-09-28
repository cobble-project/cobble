package io.cobble.table;

import io.cobble.Config;
import io.cobble.Db;
import io.cobble.ShardSnapshot;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExternalCatalogBackendTest {
    @Test
    void externalStoreConnectsMaterializesRefreshesAndDetachesPlan(@TempDir Path dataDir)
            throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        config.l0FileLimit = Integer.MAX_VALUE;
        TableIdentifier identifier =
                new TableIdentifier(Collections.singletonList("analytics"), "scores");
        TableSchema schema =
                new TableSchema(
                        Arrays.asList(
                                new DataField(1, "tenant", LogicalTypes.string()),
                                new DataField(2, "id", LogicalTypes.int64()),
                                new DataField(3, "score", LogicalTypes.int8())),
                        Arrays.asList(1L, 2L),
                        Collections.singletonList(1L));
        MemorySchemaStore store = new MemorySchemaStore();
        CatalogSchemaVersion initial = CatalogSchemaVersion.initial(41L, schema);
        store.publish(initial);
        assertEquals(0L, initial.catalogSchemaId());
        assertEquals(schema, roundTrip(initial).schema());

        List<Value> oldRow =
                Arrays.asList(Value.string("tenant-a"), Value.int64(7L), Value.int8((byte) 42));
        List<Value> widenedRow =
                Arrays.asList(Value.string("tenant-a"), Value.int64(7L), Value.int64(42L));
        ShardSnapshot oldSnapshot;
        CatalogSchemaVersion widened =
                initial.evolve(
                        Collections.singletonList(
                                TableSchemaChange.alterFieldType("score", LogicalTypes.int64())));
        assertEquals(1L, widened.catalogSchemaId());
        assertEquals(initial.tableId(), widened.tableId());
        assertEquals(widened.schema(), roundTrip(widened).schema());

        TableWritePlan detached;
        try (CatalogTable first =
                        CatalogTable.connect(identifier, initial, config, "external", store);
                Db db = Db.open(config)) {
            store.failNextMapping = true;
            IllegalStateException mappingFailure =
                    assertThrows(IllegalStateException.class, () -> first.materializeTable(db));
            assertTrue(mappingFailure.getMessage().contains("mapping callback rejected"));
            try (Table connected = first.materializeTable(db);
                    Table worker =
                            first.newWriteBuilder()
                                    .build()
                                    .writerBuilder(config)
                                    .bucket(0)
                                    .open()) {
                assertEquals(1, store.mappings.size());
                connected.put(oldRow);
                worker.put(oldRow);
                oldSnapshot = worker.snapshot();
                store.publish(widened);
                try (CatalogTable second =
                        CatalogTable.connect(identifier, widened, config, "external", store)) {
                    assertTrue(second.refreshWriter(connected));
                    assertFalse(second.refreshWriter(connected));
                    assertEquals(2, store.mappings.size());
                    assertEquals(
                            widenedRow,
                            connected.get(
                                    connected
                                            .keyBuilder()
                                            .push(oldRow.get(0))
                                            .push(oldRow.get(1))
                                            .build()));
                    store.failNextLoad = true;
                    IllegalStateException loadFailure =
                            assertThrows(
                                    IllegalStateException.class,
                                    () -> second.newWriteBuilder().build());
                    assertTrue(loadFailure.getMessage().contains("load callback rejected"));
                    detached = roundTrip(second.newWriteBuilder().build());
                }
            }
        }

        store.closed = true;
        try (Table resumed =
                detached.writerBuilder(config)
                        .bucket(0)
                        .resumeFromSnapshot(oldSnapshot.snapshotId)) {
            TableKey oldKey = resumed.keyBuilder().push(oldRow.get(0)).push(oldRow.get(1)).build();
            assertEquals(widenedRow, resumed.get(oldKey));
            List<Value> newRow =
                    Arrays.asList(Value.string("tenant-a"), Value.int64(8L), Value.int64(43L));
            resumed.put(newRow);
            assertEquals(
                    newRow,
                    resumed.get(
                            resumed.keyBuilder().push(newRow.get(0)).push(newRow.get(1)).build()));
            assertTrue(resumed.snapshot().snapshotId > oldSnapshot.snapshotId);
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

    private static final class MemorySchemaStore implements CatalogSchemaStore {
        private final Map<Long, CatalogSchemaVersion> versions =
                new HashMap<Long, CatalogSchemaVersion>();
        private final Map<String, ShardSchemaMapping> mappings =
                new HashMap<String, ShardSchemaMapping>();
        private volatile boolean failNextLoad;
        private volatile boolean failNextMapping;
        private volatile boolean closed;

        synchronized void publish(CatalogSchemaVersion version) {
            versions.put(version.catalogSchemaId(), version);
        }

        @Override
        public synchronized CatalogSchemaVersion loadSchemaVersion(long tableId, long schemaId) {
            if (closed) throw new IllegalStateException("store closed");
            if (failNextLoad) {
                failNextLoad = false;
                throw new IllegalStateException("load callback rejected");
            }
            CatalogSchemaVersion version = versions.get(schemaId);
            if (version == null || version.tableId() != tableId)
                throw new IllegalArgumentException("schema version not found");
            return version;
        }

        @Override
        public synchronized void recordShardSchemaMapping(ShardSchemaMapping mapping) {
            if (closed) throw new IllegalStateException("store closed");
            if (failNextMapping) {
                failNextMapping = false;
                throw new IllegalStateException("mapping callback rejected");
            }
            mappings.put(
                    mapping.tableId() + "/" + mapping.dbId() + "/" + mapping.catalogSchemaId(),
                    mapping);
        }
    }
}
