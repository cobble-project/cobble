package io.cobble.structured;

import io.cobble.DirectIoUtils;
import io.cobble.UnsafeAccess;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encoded direct-buffer-backed structured row returned by {@link Db} direct read APIs.
 *
 * <p>The payload keeps Cobble's internal structured row encoding and lets callers decode a target
 * column through an {@link InputStream}-based mapper without materializing intermediate heap
 * arrays.
 */
public final class DirectEncodedRow implements AutoCloseable {
    private static final String PAYLOAD_NAME = "encoded structured direct row payload";
    private static final byte TAG_NULL = 0;
    private static final byte TAG_BYTES = 1;
    private static final byte TAG_LIST = 2;

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

    /** Returns the number of columns encoded in this row. */
    public int columnCount() {
        return DirectIoUtils.readLength(address, length, 0, "column count", PAYLOAD_NAME);
    }

    /** Returns whether the target column is NULL. */
    public boolean isNull(int column) {
        int offset = 0;
        int columnCount =
                DirectIoUtils.readLength(address, length, offset, "column count", PAYLOAD_NAME);
        offset += Integer.BYTES;
        ensureColumnInRange(column, columnCount);
        for (int i = 0; i < columnCount; i++) {
            byte tag = DirectIoUtils.readByte(address, length, offset, PAYLOAD_NAME);
            offset += 1;
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return true;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength =
                            DirectIoUtils.readLength(
                                    address, length, offset, "bytes length", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i == column) {
                        return false;
                    }
                    offset = skipBytes(offset, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount =
                            DirectIoUtils.readLength(
                                    address, length, offset, "list element count", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i == column) {
                        return false;
                    }
                    offset = skipList(offset, elementCount);
                    break;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one BYTES column with a caller-provided {@link InputStream} decoder.
     *
     * @return mapped value, or {@code null} if the target column is NULL
     */
    public <T> T decodeBytesColumn(int column, ColumnDecoder<T> decoder) throws IOException {
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
            byte tag = DirectIoUtils.readByte(address, length, offset, PAYLOAD_NAME);
            offset += 1;
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return null;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength =
                            DirectIoUtils.readLength(
                                    address, length, offset, "bytes length", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i == column) {
                        DirectIoUtils.ensureRemaining(length, offset, bytesLength, PAYLOAD_NAME);
                        inputStream.reset(address + offset, bytesLength);
                        return decoder.decode(inputStream);
                    }
                    offset = skipBytes(offset, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount =
                            DirectIoUtils.readLength(
                                    address, length, offset, "list element count", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i == column) {
                        throw new IllegalStateException(
                                "expected BYTES column in direct row but found LIST");
                    }
                    offset = skipList(offset, elementCount);
                    break;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
        }
        throw new IllegalStateException("missing column in encoded direct row: " + column);
    }

    /**
     * Decodes one LIST column with a caller-provided element decoder.
     *
     * <p>The decoder is invoked once for each list element and receives an {@link InputStream} view
     * of that element payload.
     *
     * @return decoded list, or {@code null} if the target column is NULL
     */
    public <T> List<T> decodeListColumn(int column, ColumnDecoder<T> decoder) throws IOException {
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
            byte tag = DirectIoUtils.readByte(address, length, offset, PAYLOAD_NAME);
            offset += 1;
            switch (tag) {
                case TAG_NULL:
                    if (i == column) {
                        return null;
                    }
                    break;
                case TAG_BYTES:
                    int bytesLength =
                            DirectIoUtils.readLength(
                                    address, length, offset, "bytes length", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i == column) {
                        throw new IllegalStateException(
                                "expected LIST column in direct row but found BYTES");
                    }
                    offset = skipBytes(offset, bytesLength);
                    break;
                case TAG_LIST:
                    int elementCount =
                            DirectIoUtils.readLength(
                                    address, length, offset, "list element count", PAYLOAD_NAME);
                    offset += Integer.BYTES;
                    if (i != column) {
                        offset = skipList(offset, elementCount);
                        break;
                    }
                    List<T> decoded = new ArrayList<>(elementCount);
                    for (int j = 0; j < elementCount; j++) {
                        int elementLength =
                                DirectIoUtils.readLength(
                                        address,
                                        length,
                                        offset,
                                        "list element length",
                                        PAYLOAD_NAME);
                        offset += Integer.BYTES;
                        DirectIoUtils.ensureRemaining(length, offset, elementLength, PAYLOAD_NAME);
                        inputStream.reset(address + offset, elementLength);
                        decoded.add(decoder.decode(inputStream));
                        offset += elementLength;
                    }
                    return decoded;
                default:
                    throw new IllegalStateException("unknown direct row column tag: " + tag);
            }
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

    private int skipList(int offset, int elementCount) {
        for (int i = 0; i < elementCount; i++) {
            int elementLength =
                    DirectIoUtils.readLength(
                            address, length, offset, "list element length", PAYLOAD_NAME);
            offset += Integer.BYTES;
            DirectIoUtils.ensureRemaining(length, offset, elementLength, PAYLOAD_NAME);
            offset += elementLength;
        }
        return offset;
    }

    private static void ensureColumnInRange(int column, int columnCount) {
        if (column < 0 || column >= columnCount) {
            throw new IllegalArgumentException("column out of range: " + column);
        }
    }

    /** Decoder function that maps one column payload stream to a value. */
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
