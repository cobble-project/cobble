package io.cobble;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encoded direct-buffer-backed raw row returned by {@link Db} direct read APIs.
 *
 * <p>The payload stores selected columns as presence flag + length + bytes. Callers can decode a
 * column with an {@link InputStream}-based mapper without materializing intermediate heap arrays.
 */
public final class DirectEncodedRow implements AutoCloseable {
    private static final String PAYLOAD_NAME = "encoded direct row payload";

    private static final Runnable NO_OP =
            new Runnable() {
                @Override
                public void run() {}
            };

    private final long address;
    private final int length;
    private final Runnable releaser;
    private final AtomicBoolean closed;

    DirectEncodedRow(long address, int length, Runnable releaser) {
        if (address == 0L) {
            throw new IllegalArgumentException("address must not be 0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        this.address = address;
        this.length = length;
        this.releaser = releaser == null ? NO_OP : releaser;
        this.closed = new AtomicBoolean(false);
    }

    /** Returns encoded payload length in bytes. */
    public int length() {
        return length;
    }

    /** Returns the selected column count in this encoded row payload. */
    public int size() {
        return DirectIoUtils.readLength(address, length, 0, "column count", PAYLOAD_NAME);
    }

    /** Returns whether the target selected column is missing/null. */
    public boolean isNull(int column) {
        int offset = 0;
        int columnCount =
                DirectIoUtils.readLength(address, length, offset, "column count", PAYLOAD_NAME);
        offset += Integer.BYTES;
        ensureColumnInRange(column, columnCount);
        for (int i = 0; i < columnCount; i++) {
            int present = DirectIoUtils.readUnsignedByte(address, length, offset, PAYLOAD_NAME);
            offset += 1;
            if (present == 0) {
                if (i == column) {
                    return true;
                }
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct encoded row payload: bad presence flag " + present);
            }
            int valueLength =
                    DirectIoUtils.readLength(
                            address, length, offset, "column length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            if (i == column) {
                return false;
            }
            offset = skipBytes(offset, valueLength);
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one selected column through a caller-provided stream decoder.
     *
     * @return mapped value, or {@code null} when the selected column is missing/null
     */
    public <T> T decodeColumn(int column, ColumnDecoder<T> decoder) throws IOException {
        if (decoder == null) {
            throw new NullPointerException("decoder");
        }
        int offset = 0;
        int columnCount =
                DirectIoUtils.readLength(address, length, offset, "column count", PAYLOAD_NAME);
        offset += Integer.BYTES;
        ensureColumnInRange(column, columnCount);
        ReusableDirectAddressInputStream inputStream = new ReusableDirectAddressInputStream();
        for (int i = 0; i < columnCount; i++) {
            int present = DirectIoUtils.readUnsignedByte(address, length, offset, PAYLOAD_NAME);
            offset += 1;
            if (present == 0) {
                if (i == column) {
                    return null;
                }
                continue;
            }
            if (present != 1) {
                throw new IllegalStateException(
                        "invalid direct encoded row payload: bad presence flag " + present);
            }
            int valueLength =
                    DirectIoUtils.readLength(
                            address, length, offset, "column length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            if (i != column) {
                offset = skipBytes(offset, valueLength);
                continue;
            }
            DirectIoUtils.ensureRemaining(length, offset, valueLength, PAYLOAD_NAME);
            inputStream.reset(address + offset, valueLength);
            return decoder.decode(inputStream);
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    Runnable takeReleaser() {
        if (closed.compareAndSet(false, true)) {
            return releaser;
        }
        return NO_OP;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            releaser.run();
        }
    }

    private int skipBytes(int offset, int bytesLength) {
        DirectIoUtils.ensureRemaining(length, offset, bytesLength, PAYLOAD_NAME);
        return offset + bytesLength;
    }

    private static void ensureColumnInRange(int column, int columnCount) {
        if (column < 0 || column >= columnCount) {
            throw new IllegalArgumentException("column out of range: " + column);
        }
    }

    /** Decoder function that maps one column payload stream to a typed value. */
    @FunctionalInterface
    public interface ColumnDecoder<T> {
        T decode(InputStream input) throws IOException;
    }

    private static final class ReusableDirectAddressInputStream extends InputStream {
        private long currentAddress;
        private int remaining;

        private void reset(long startAddress, int length) {
            this.currentAddress = startAddress;
            this.remaining = length;
        }

        @Override
        public int read() {
            if (remaining <= 0) {
                return -1;
            }
            int value = UnsafeAccess.getByte(currentAddress) & 0xFF;
            currentAddress += 1;
            remaining -= 1;
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if ((offset | length | (offset + length) | (bytes.length - (offset + length))) < 0) {
                throw new IndexOutOfBoundsException("invalid byte[] range");
            }
            if (length == 0) {
                return 0;
            }
            if (remaining <= 0) {
                return -1;
            }
            int readable = Math.min(length, remaining);
            UnsafeAccess.copyMemory(currentAddress, bytes, offset, readable);
            currentAddress += readable;
            remaining -= readable;
            return readable;
        }
    }
}
