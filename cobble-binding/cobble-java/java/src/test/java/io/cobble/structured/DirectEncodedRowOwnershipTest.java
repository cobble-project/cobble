package io.cobble.structured;

import io.cobble.Config;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DirectEncodedRowOwnershipTest {
    @Test
    void overflowRowsRetainOwnersDuringDecodingAndSubsequentReads() throws Exception {
        Config config =
                new Config()
                        .addVolume(Files.createTempDirectory("cobble-direct-owner-").toString())
                        .numColumns(3)
                        .totalBuckets(1);
        byte[] key = "owner-row".getBytes(StandardCharsets.UTF_8);
        byte[] otherKey = "other-row".getBytes(StandardCharsets.UTF_8);
        byte[] bytes = new byte[4096];
        Arrays.fill(bytes, (byte) 'b');
        byte[] element = new byte[4096];
        Arrays.fill(element, (byte) 'l');
        byte[] tail = new byte[] {1, 2, 3};
        ByteBuffer directKey = directBufferOf(key);

        try (Db db = Db.open(config)) {
            db.updateSchema().addListColumn(1, ListConfig.of(10, ListRetainMode.LAST)).commit();
            db.put(0, key, 0, ColumnValue.ofBytes(bytes));
            db.put(0, key, 1, ColumnValue.ofList(new byte[][] {element, tail}));
            db.put(0, otherKey, 0, ColumnValue.ofBytes(element));

            try (ReadOptions options = ReadOptions.forColumns(0, 1, 2)) {
                for (int overload = 0; overload < 3; overload++) {
                    try (DirectEncodedRow row = readRow(db, key, directKey, options, overload)) {
                        assertNotNull(row);
                        assertTrue(row.length() > 2048);
                        WeakReference<ByteBuffer> owner = ownerReference(row);
                        assertNotNull(owner.get());
                        assertTrue(owner.get().isDirect());

                        // Exercise the reusable JNI overflow scratch and pooled buffer while
                        // keeping the first row's separate, Java-owned overflow copy open.
                        try (DirectEncodedRow other =
                                db.getDirectEncodedRowWithOptions(0, otherKey, options)) {
                            assertArrayEquals(element, other.decodeBytesColumn(0, this::readAll));
                        }
                        System.gc();
                        assertNotNull(owner.get(), "open row must retain its direct buffer");
                        assertEquals(3, row.columnCount());
                        assertFalse(row.isNull(0));
                        assertTrue(row.isNull(2));
                        assertArrayEquals(
                                bytes,
                                row.decodeBytesColumn(0, input -> readWithOwner(input, owner)));
                        List<byte[]> list =
                                row.decodeListColumn(1, input -> readWithOwner(input, owner));
                        assertEquals(2, list.size());
                        assertArrayEquals(element, list.get(0));
                        assertArrayEquals(tail, list.get(1));
                        assertNull(row.decodeBytesColumn(2, this::readAll));
                        assertNull(row.decodeListColumn(2, this::readAll));
                        assertThrows(
                                IllegalStateException.class,
                                () -> row.decodeBytesColumn(1, this::readAll));
                        assertThrows(
                                IllegalStateException.class,
                                () -> row.decodeListColumn(0, this::readAll));

                        IOException failure = new IOException("decoder failure");
                        assertSame(
                                failure,
                                assertThrows(
                                        IOException.class,
                                        () ->
                                                row.decodeBytesColumn(
                                                        0,
                                                        input -> {
                                                            readWithOwner(input, owner);
                                                            throw failure;
                                                        })));
                        assertFalse(Thread.holdsLock(owner.get()));
                        assertArrayEquals(bytes, row.decodeBytesColumn(0, this::readAll));
                        ByteBuffer scratch = Db.getLastDirectOverflowBuffer();
                        assertNotSame(scratch, owner.get(), "structured rows keep stable copies");
                        row.close();
                        row.close();
                        assertSame(
                                scratch,
                                Db.getLastDirectOverflowBuffer(),
                                "closing a copy must not replace the occupied native scratch slot");
                    }
                }
            }
        }
    }

    @Test
    void scanRowPassesItsEncodedBufferOwnerToTheDecoder() throws Exception {
        ByteBuffer encoded = ByteBuffer.allocateDirect(4 + 1 + 4 + 4 + 1 + 4 + 1);
        encoded.putInt(1).put((byte) 'k').putInt(10);
        encoded.putInt(1).put((byte) 1).putInt(1).put((byte) 'v');
        DirectScanRow scan = DirectScanRow.decode(encoded, encoded.capacity());
        Field rowField = DirectScanRow.class.getDeclaredField("row");
        rowField.setAccessible(true);
        WeakReference<ByteBuffer> owner = ownerReference((DirectEncodedRow) rowField.get(scan));
        assertSame(encoded, owner.get());
        assertEquals(1, scan.columnCount());
        assertArrayEquals(
                new byte[] {'v'}, scan.decodeBytesColumn(0, input -> readWithOwner(input, owner)));

        Config config =
                new Config()
                        .addVolume(
                                Files.createTempDirectory("cobble-scan-overflow-owner-").toString())
                        .numColumns(1)
                        .totalBuckets(1);
        byte[] value = new byte[4096];
        Arrays.fill(value, (byte) 'a');
        byte[] otherValue = new byte[8192];
        Arrays.fill(otherValue, (byte) 'b');
        try (Db db = Db.open(config)) {
            db.put(0, new byte[] {1}, 0, ColumnValue.ofBytes(value));
            db.put(0, new byte[] {2}, 0, ColumnValue.ofBytes(otherValue));
            // Another cursor and point lookup must not overwrite either a row or a lazy batch.
            for (boolean batched : new boolean[] {false, true}) {
                try (DirectScanCursor cursor = db.scanDirectWithOptions(0, null, 0, null, 0, null);
                        DirectScanCursor other =
                                db.scanDirectWithOptions(0, null, 0, null, 0, null)) {
                    DirectScanBatch batch = batched ? cursor.nextBatch(1) : null;
                    DirectScanRow first = batched ? null : cursor.nextRow();
                    assertNotNull(other.nextRow());
                    assertArrayEquals(
                            otherValue, other.nextRow().decodeBytesColumn(0, this::readAll));
                    try (DirectEncodedRow point =
                            db.getDirectEncodedRowWithOptions(0, new byte[] {2}, null)) {
                        assertArrayEquals(otherValue, point.decodeBytesColumn(0, this::readAll));
                    }
                    System.gc();
                    DirectScanRow retained = batched ? batch.get(0) : first;
                    assertArrayEquals(value, retained.decodeBytesColumn(0, this::readAll));
                    assertArrayEquals(
                            otherValue, cursor.nextRow().decodeBytesColumn(0, this::readAll));
                    assertNull(cursor.nextRow());
                }
            }
        }
    }

    private static DirectEncodedRow readRow(
            Db db, byte[] key, ByteBuffer directKey, ReadOptions options, int overload) {
        switch (overload) {
            case 0:
                return db.getDirectEncodedRowWithOptions(0, key, options);
            case 1:
                return db.getDirectEncodedRowWithOptions(0, directKey, options);
            default:
                return db.getDirectEncodedRowWithOptions(0, directKey, key.length, options);
        }
    }

    private static WeakReference<ByteBuffer> ownerReference(DirectEncodedRow row) throws Exception {
        Field owner = DirectEncodedRow.class.getDeclaredField("owner");
        owner.setAccessible(true);
        return new WeakReference<>((ByteBuffer) owner.get(row));
    }

    private byte[] readWithOwner(InputStream input, WeakReference<ByteBuffer> owner)
            throws IOException {
        assertTrue(Thread.holdsLock(owner.get()), "decoder must keep the owner monitor locked");
        System.gc();
        assertNotNull(owner.get(), "owner must stay live throughout the callback");
        return readAll(input);
    }

    private byte[] readAll(InputStream input) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        int first = input.read();
        if (first >= 0) {
            result.write(first);
        }
        byte[] buffer = new byte[256];
        int read;
        while ((read = input.read(buffer)) != -1) {
            result.write(buffer, 0, read);
        }
        return result.toByteArray();
    }

    private static ByteBuffer directBufferOf(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        ((Buffer) buffer).flip();
        return buffer;
    }
}
