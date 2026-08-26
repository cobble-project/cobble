package io.cobble.structured;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/** Ordered values returned by {@link StreamingMultiGet}. */
public final class StreamingMultiGetResult implements AutoCloseable {
    private final StreamingMultiGet owner;
    private final ByteBuffer encoded;
    private final BoundedByteBufferInputStream valueStream;
    private final DataInputStream valueInput;
    private final int count;
    private int index;
    private boolean closed;

    StreamingMultiGetResult(
            StreamingMultiGet owner, ByteBuffer encoded, int encodedLength, int expectedCount) {
        this.owner = owner;
        this.encoded = encoded.duplicate();
        this.encoded.position(0);
        this.encoded.limit(encodedLength);
        this.valueStream = new BoundedByteBufferInputStream();
        this.valueInput = new DataInputStream(valueStream);
        this.count = readInt("result count");
        if (count != expectedCount) {
            throw new IllegalStateException(
                    "Cobble streaming multi-get returned "
                            + count
                            + " values for "
                            + expectedCount
                            + " keys");
        }
    }

    /** Returns whether another result is available. Missing keys still occupy one result. */
    public boolean hasNext() {
        ensureOpen();
        return index < count;
    }

    /**
     * Returns a bounded stream for the next value, or {@code null} when the key was not found. The
     * previous value must be consumed or skipped before advancing. The returned stream is reused
     * and remains valid only until the next call to this method.
     */
    public DataInputStream nextValue() {
        ensureOpen();
        if (!hasNext()) {
            throw new NoSuchElementException("no more streaming multi-get values");
        }
        if (valueStream.remaining() != 0) {
            throw new IllegalStateException("the previous streaming value was not fully consumed");
        }
        int length = readInt("value length");
        index++;
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > encoded.remaining()) {
            throw new IllegalStateException("invalid streaming value length: " + length);
        }
        valueStream.reset(encoded, length);
        return valueInput;
    }

    /** Skips the unread remainder of the current value. */
    public void skipRemainingValue() {
        ensureOpen();
        valueStream.skipRemaining();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            valueStream.skipRemaining();
            owner.releaseResult();
        }
    }

    private int readInt(String field) {
        if (encoded.remaining() < Integer.BYTES) {
            throw new IllegalStateException("truncated streaming multi-get " + field);
        }
        return encoded.getInt();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("streaming multi-get result is closed");
        }
    }

    private static final class BoundedByteBufferInputStream extends InputStream {
        private ByteBuffer source;
        private int remaining;

        private void reset(ByteBuffer source, int length) {
            this.source = source;
            this.remaining = length;
        }

        private int remaining() {
            return remaining;
        }

        private void skipRemaining() {
            if (remaining > 0) {
                source.position(source.position() + remaining);
                remaining = 0;
            }
        }

        @Override
        public int read() {
            if (remaining == 0) {
                return -1;
            }
            remaining--;
            return source.get() & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || offset > bytes.length - length) {
                throw new IndexOutOfBoundsException("invalid offset/length");
            }
            if (remaining == 0) {
                return -1;
            }
            int read = Math.min(length, remaining);
            source.get(bytes, offset, read);
            remaining -= read;
            return read;
        }

        @Override
        public long skip(long length) {
            int skipped = (int) Math.min(Math.max(0L, length), remaining);
            source.position(source.position() + skipped);
            remaining -= skipped;
            return skipped;
        }

        @Override
        public int available() {
            return remaining;
        }

        @Override
        public void close() throws IOException {
            skipRemaining();
        }
    }
}
