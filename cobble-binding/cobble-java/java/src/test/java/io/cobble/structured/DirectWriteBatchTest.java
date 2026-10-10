package io.cobble.structured;

import io.cobble.Config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DirectWriteBatchTest {

    @Test
    void preservesDefaultAndExplicitColumnFamilyAndTtlOptions() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-direct-batch-options-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        config.ttlEnabled = true;
        config.timeProvider = Config.TimeProviderKind.MANUAL;
        DirectWriteBatch batch = new DirectWriteBatch(16);

        try (Db db = Db.open(config);
                WriteOptions options = WriteOptions.withColumnFamily("timed").ttlSeconds(10)) {
            db.updateSchema().addBytesColumn("timed", 0).commit();
            append(batch, "key", "default");
            db.putDirectBatchWithOptions(0, 0, batch, null);
            batch.clear();
            append(batch, "key", "expires");
            db.putDirectBatchWithOptions(0, 0, batch, options);

            assertArrayEquals(bytes("default"), db.get(0, bytes("key")).getBytes(0));
            assertArrayEquals(bytes("expires"), db.get(0, bytes("key"), "timed").getBytes(0));
            db.setTime(11);
            assertNull(db.get(0, bytes("key"), "timed"));
            assertArrayEquals(bytes("default"), db.get(0, bytes("key")).getBytes(0));
        }
    }

    @Test
    void writesAndReusesDirectBatch() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-direct-write-batch-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        DirectWriteBatch batch = new DirectWriteBatch(1);

        try (Db db = Db.open(config);
                WriteOptions options = new WriteOptions()) {
            append(batch, "k1", "v1");
            append(batch, "k2", "value-two");
            assertEquals(2, batch.size());
            db.putDirectBatchWithOptions(0, 0, batch, options);

            assertArrayEquals(bytes("v1"), db.get(0, bytes("k1")).getBytes(0));
            assertArrayEquals(bytes("value-two"), db.get(0, bytes("k2")).getBytes(0));

            batch.clear();
            append(batch, "k3", "v3");
            db.putDirectBatchWithOptions(0, 0, batch, options);
            assertArrayEquals(bytes("v3"), db.get(0, bytes("k3")).getBytes(0));
        }
    }

    @Test
    void writesByteArraysAndSlicesAlongsideDirectBuffers() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-byte-array-write-batch-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        DirectWriteBatch batch = new DirectWriteBatch(1);

        try (Db db = Db.open(config);
                WriteOptions options = new WriteOptions()) {
            batch.put(bytes("k1"), bytes("v1"));
            byte[] key = bytes("_k2_");
            byte[] value = bytes("_value-two_");
            batch.put(key, 1, 2, value, 1, 9);
            append(batch, "k3", "v3");
            db.putDirectBatchWithOptions(0, 0, batch, options);

            assertArrayEquals(bytes("v1"), db.get(0, bytes("k1")).getBytes(0));
            assertArrayEquals(bytes("value-two"), db.get(0, bytes("k2")).getBytes(0));
            assertArrayEquals(bytes("v3"), db.get(0, bytes("k3")).getBytes(0));
        }
    }

    @Test
    void validatesDirectInputSlices() {
        DirectWriteBatch batch = new DirectWriteBatch(16);
        ByteBuffer direct = direct("key");

        assertThrows(
                IllegalArgumentException.class,
                () -> batch.put(ByteBuffer.wrap(bytes("heap")), 4, direct, 3));
        assertThrows(IllegalArgumentException.class, () -> batch.put(direct, 4, direct, 3));
    }

    @Test
    void validatesByteArrayInputSlices() {
        DirectWriteBatch batch = new DirectWriteBatch(16);
        byte[] bytes = bytes("data");

        assertThrows(IllegalArgumentException.class, () -> batch.put(null, bytes));
        assertThrows(IllegalArgumentException.class, () -> batch.put(bytes, null));
        assertThrows(IllegalArgumentException.class, () -> batch.put(bytes, -1, 1, bytes, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> batch.put(bytes, 3, 2, bytes, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> batch.put(bytes, 0, 1, bytes, 4, 1));
    }

    private static void append(DirectWriteBatch batch, String key, String value) {
        ByteBuffer keyBuffer = direct(key);
        ByteBuffer valueBuffer = direct(value);
        batch.put(keyBuffer, keyBuffer.position(), valueBuffer, valueBuffer.position());
    }

    private static ByteBuffer direct(String value) {
        byte[] bytes = bytes(value);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        return buffer;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
