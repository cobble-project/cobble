package io.cobble;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnFamilyMetadataTest {
    @TempDir Path dataDir;

    @Test
    void capturesObjectMetadataInShardSnapshot() throws Exception {
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        try (Db db = Db.open(config);
                SchemaBuilder builder = db.updateSchema()) {
            builder.setColumnFamilyOptions(
                    "state",
                    ColumnFamilyOptions.defaults()
                            .metadata("{\"format\":\"flink-state\",\"version\":1}"));
            builder.addColumn("state", 0, null, null);
            builder.commit();

            ShardSnapshot snapshot = db.snapshot();
            String metadata = snapshot.columnFamilies.get("state").options.metadata;
            JsonObject object = JsonParser.parseString(metadata).getAsJsonObject();
            assertEquals("flink-state", object.get("format").getAsString());
            assertEquals(1, object.get("version").getAsInt());
            assertTrue(JsonParser.parseString(metadata).isJsonObject());

            ShardSnapshot loaded =
                    SnapshotTools.loadShardSnapshot(config, snapshot.dbId, snapshot.manifestPath);
            assertEquals(metadata, loaded.columnFamilies.get("state").options.metadata);
            assertTrue(
                    JsonParser.parseString(loaded.columnFamilies.get("state").options.metadata)
                            .isJsonObject());
        }
    }
}
