package io.cobble;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DbBindingTest {
    @Test
    void dbBulkReadWriteWithConfigJson() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-db-json-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (Db db = Db.open(config)) {
            int count = 320;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] value = valueBytes("db-json-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] expected = valueBytes("db-json-v", i);
                assertArrayEquals(expected, db.get(0, key, 0));
            }

            for (int i = 0; i < count; i += 3) {
                byte[] key = keyBytes("db-json", i);
                db.delete(0, key, 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("db-json", i);
                byte[] value = db.get(0, key, 0);
                if (i % 3 == 0) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("db-json-v", i), value);
                }
            }
        }
    }

    @Test
    void singleDbBulkReadWriteWithConfigPath() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-single-path-");
        Path configPath =
                writeConfigFile(
                        dataDir,
                        new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1));

        try (SingleDb db = SingleDb.open(configPath.toString())) {
            int count = 280;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                byte[] value = valueBytes("single-path-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                assertArrayEquals(valueBytes("single-path-v", i), db.get(0, key, 0));
            }

            for (int i = 1; i < count; i += 4) {
                db.delete(0, keyBytes("single-path", i), 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("single-path", i);
                byte[] value = db.get(0, key, 0);
                if (i % 4 == 1) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("single-path-v", i), value);
                }
            }
        }
    }

    @Test
    void structuredDbBulkReadWriteWithConfigJson() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-java-structured-json-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(2).totalBuckets(1);

        try (StructuredDb db = StructuredDb.open(config)) {
            int count = 300;
            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                byte[] value = valueBytes("structured-json-v", i);
                db.put(0, key, 0, value);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                assertArrayEquals(valueBytes("structured-json-v", i), db.get(0, key, 0));
            }

            for (int i = 2; i < count; i += 5) {
                db.delete(0, keyBytes("structured-json", i), 0);
            }

            for (int i = 0; i < count; i++) {
                byte[] key = keyBytes("structured-json", i);
                byte[] value = db.get(0, key, 0);
                if (i % 5 == 2) {
                    assertNull(value);
                } else {
                    assertArrayEquals(valueBytes("structured-json-v", i), value);
                }
            }
        }
    }

    private static Path writeConfigFile(Path dataDir, Config config) throws IOException {
        Files.createDirectories(dataDir);
        Path configPath = Files.createTempFile("cobble-java-config-", ".json");
        Files.write(configPath, config.toJson().getBytes(StandardCharsets.UTF_8));
        return configPath;
    }

    private static byte[] keyBytes(String prefix, int i) {
        return (prefix + "-k-" + i).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] valueBytes(String prefix, int i) {
        return (prefix + "-value-" + i + "-payload").getBytes(StandardCharsets.UTF_8);
    }
}
