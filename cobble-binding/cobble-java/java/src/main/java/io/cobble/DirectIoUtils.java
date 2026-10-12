package io.cobble;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

/** Shared helpers for direct-memory Cobble Java APIs. */
public final class DirectIoUtils {
    private DirectIoUtils() {}

    public static long directAddress(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            throw new IllegalArgumentException("buffer must be a direct ByteBuffer");
        }
        return UnsafeAccess.directAddress(buffer);
    }

    public static ByteBuffer resolveEncodedBuffer(
            ByteBuffer ioBuffer, int encodedLength, Supplier<ByteBuffer> overflowBufferSupplier) {
        if (encodedLength > 0) {
            return ioBuffer;
        }
        ByteBuffer overflowBuffer = overflowBufferSupplier.get();
        if (overflowBuffer == null) {
            throw new IllegalStateException("missing overflow direct buffer from JNI");
        }
        return overflowBuffer;
    }

    public static void copyKey(ByteBuffer target, byte[] key) {
        synchronized (target) {
            UnsafeAccess.copyMemory(key, 0, directAddress(target), key.length);
        }
    }

    public static void copyKey(ByteBuffer target, ByteBuffer keyBuffer, int keyLength) {
        ByteBuffer source = prefixView(keyBuffer, keyLength);
        ByteBuffer destination = prefixView(target, keyLength);
        destination.put(source);
    }

    /** Returns a stable direct copy of {@code source[0..length)}. */
    public static ByteBuffer copyDirectPrefix(ByteBuffer source, int length) {
        if (source == null || !source.isDirect()) {
            throw new IllegalArgumentException("source must be a direct ByteBuffer");
        }
        if (length < 0 || length > source.capacity()) {
            throw new IllegalArgumentException("length out of range: " + length);
        }
        ByteBuffer copy = ByteBuffer.allocateDirect(length);
        copy.put(prefixView(source, length));
        ((Buffer) copy).clear();
        return copy;
    }

    /**
     * Returns an exclusively owned, completed lease to the closing thread's single overflow slot.
     * Internal use only: caller-owned buffers and live views must never be returned.
     */
    public static void returnOverflowBuffer(ByteBuffer buffer) {
        if (buffer == null) return;
        ((Buffer) buffer).clear();
        buffer.order(ByteOrder.BIG_ENDIAN);
        returnOverflowBufferNative(buffer);
    }

    private static native void returnOverflowBufferNative(ByteBuffer buffer);

    // Bulk ByteBuffer copies retain both owners without locking two caller-provided buffers.
    // A prefix needs only a duplicate, not the extra slice required for an arbitrary offset.
    private static ByteBuffer prefixView(ByteBuffer source, int length) {
        ByteBuffer view = source.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).limit(length);
        return view;
    }

    public static void ensureRemaining(int totalLength, int offset, int size, String payloadName) {
        if (size < 0 || offset < 0 || offset > totalLength - size) {
            throw new IllegalStateException("malformed " + payloadName);
        }
    }

    public static int readLength(
            long address, int totalLength, int offset, String fieldName, String payloadName) {
        ensureRemaining(totalLength, offset, Integer.BYTES, payloadName);
        int value = UnsafeAccess.getIntBigEndian(address + offset);
        if (value < 0) {
            throw new IllegalStateException("invalid " + fieldName + ": " + value);
        }
        return value;
    }

    public static byte readByte(long address, int totalLength, int offset, String payloadName) {
        ensureRemaining(totalLength, offset, 1, payloadName);
        return UnsafeAccess.getByte(address + offset);
    }

    public static int readUnsignedByte(
            long address, int totalLength, int offset, String payloadName) {
        return readByte(address, totalLength, offset, payloadName) & 0xFF;
    }

    public static ByteBuffer slice(ByteBuffer source, int offset, int length, String payloadName) {
        ensureRemaining(source.capacity(), offset, length, payloadName);
        ByteBuffer view = source.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).position(offset);
        ((Buffer) view).limit(offset + length);
        return view.slice();
    }

    public static ByteBuffer[] decodeDirectColumnsCopy(long address, int encodedLength) {
        if (encodedLength <= 0) {
            throw new IllegalStateException("invalid direct payload length: " + encodedLength);
        }
        int offset = 0;
        int columnCount =
                readLength(address, encodedLength, offset, "column count", "direct payload");
        offset += Integer.BYTES;
        ByteBuffer[] columns = new ByteBuffer[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int present = readUnsignedByte(address, encodedLength, offset, "direct payload");
            offset += 1;
            if (present == 0) {
                columns[i] = null;
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct payload: bad presence flag "
                                + present
                                + " for column "
                                + i);
            }
            int length =
                    readLength(address, encodedLength, offset, "column length", "direct payload");
            offset += Integer.BYTES;
            ensureRemaining(encodedLength, offset, length, "direct payload");
            ByteBuffer column = ByteBuffer.allocateDirect(length);
            synchronized (column) {
                UnsafeAccess.copyMemory(address + offset, directAddress(column), length);
            }
            columns[i] = column;
            offset += length;
        }
        if (offset != encodedLength) {
            throw new IllegalStateException("invalid direct payload: trailing bytes");
        }
        return columns;
    }
}
