package io.cobble;

import java.nio.ByteBuffer;

/** Java-side random access file contract bridged into Cobble core through JNI. */
public interface CustomRandomAccessFile extends AutoCloseable {
    /**
     * Reads up to {@code size} bytes from {@code offset} and returns the payload as heap bytes.
     *
     * <p>If this implementation supports direct-buffer reads, native callers will prefer {@link
     * #readAtDirect(long, int)}.
     */
    byte[] readAt(long offset, int size);

    /**
     * Whether this implementation supports direct-buffer reads through {@link #readAtDirect(long,
     * int)}.
     */
    default boolean supportDirect() {
        return false;
    }

    /**
     * Reads up to {@code size} bytes from {@code offset} into a direct {@link ByteBuffer}.
     *
     * <p>Implementations should return a direct buffer with position {@code 0} and limit set to the
     * number of readable bytes.
     */
    default ByteBuffer readAtDirect(long offset, int size) {
        throw new UnsupportedOperationException("Direct read is not supported.");
    }

    /** File size in bytes. */
    long size();

    @Override
    void close();
}
