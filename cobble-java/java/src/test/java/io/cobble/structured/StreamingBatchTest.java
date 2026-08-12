package io.cobble.structured;

import io.cobble.Config;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamingBatchTest {

    @Test
    void streamsWritesAcrossChunksAndFlushesAtElementBoundaries() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-streaming-write-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        byte[] key = bytes("key-crosses-chunks");
        byte[] value = repeatedBytes(37, 91);

        try (Db db = Db.open(config);
                WriteOptions options = new WriteOptions()) {
            try (StreamingWriteBatch batch = db.streamingWriteBatch(0, 0, options, 8)) {
                DataOutputStream output = batch.output();
                output.write(key);
                batch.finishKey();
                output.write(value);
                batch.finishElement();

                assertArrayEquals(value, db.get(0, key).getBytes(0));

                output.write(bytes("second"));
                batch.finishKey();
                output.writeInt(123456);
                batch.finishElement();
            }
            assertArrayEquals(intBytes(123456), db.get(0, bytes("second")).getBytes(0));
        }
    }

    @Test
    void streamsMultiGetKeysAndValuesWithoutByteArrays() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-streaming-read-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);
        byte[] presentKey = bytes("present-key-crosses-chunks");
        byte[] missingKey = bytes("missing-key-crosses-chunks");
        byte[] emptyKey = bytes("empty-value");
        byte[] value = repeatedBytes(41, 17);

        try (Db db = Db.open(config);
                ReadOptions options = ReadOptions.defaults()) {
            db.put(0, presentKey, 0, value);
            db.put(0, emptyKey, 0, new byte[0]);

            try (StreamingMultiGet batch = db.streamingMultiGet(options, 4)) {
                DataOutputStream keys = batch.keyOutput();
                keys.write(presentKey);
                batch.finishKey(0);
                keys.write(missingKey);
                batch.finishKey(0);
                keys.write(emptyKey);
                batch.finishKey(0);

                try (StreamingMultiGetResult result = batch.execute()) {
                    DataInputStream first = result.nextValue();
                    byte[] actual = new byte[value.length];
                    first.readFully(actual);
                    assertArrayEquals(value, actual);
                    assertEquals(-1, first.read());

                    assertNull(result.nextValue());

                    DataInputStream empty = result.nextValue();
                    assertEquals(-1, empty.read());
                    assertEquals(false, result.hasNext());
                }

                keys.write(presentKey);
                batch.finishKey(0);
                try (StreamingMultiGetResult result = batch.execute()) {
                    byte[] actual = new byte[value.length];
                    result.nextValue().readFully(actual);
                    assertArrayEquals(value, actual);
                }
            }
        }
    }

    @Test
    void requiresCurrentValueToBeConsumedBeforeAdvancing() throws IOException {
        Path dataDir = Files.createTempDirectory("cobble-streaming-read-boundary-");
        Config config = new Config().addVolume(dataDir.toString()).numColumns(1).totalBuckets(1);

        try (Db db = Db.open(config);
                ReadOptions options = ReadOptions.defaults()) {
            db.put(0, bytes("a"), 0, bytes("first"));
            db.put(0, bytes("b"), 0, bytes("second"));
            try (StreamingMultiGet batch = db.streamingMultiGet(options, 2)) {
                batch.keyOutput().writeByte('a');
                batch.finishKey(0);
                batch.keyOutput().writeByte('b');
                batch.finishKey(0);
                try (StreamingMultiGetResult result = batch.execute()) {
                    result.nextValue().readByte();
                    assertThrows(IllegalStateException.class, result::nextValue);
                    result.skipRemainingValue();
                    byte[] second = new byte[6];
                    result.nextValue().readFully(second);
                    assertArrayEquals(bytes("second"), second);
                }
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeatedBytes(int length, int value) {
        byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] intBytes(int value) {
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value
        };
    }
}
