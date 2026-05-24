package io.cobble;

import java.nio.ByteBuffer;

/** Java-side sequential write file contract bridged into Cobble core through JNI. */
public interface CustomSequentialWriteFile extends AutoCloseable {
    /**
     * Appends bytes to the file from heap memory.
     *
     * <p>If this implementation supports direct-buffer writes, native callers will prefer {@link
     * #writeDirect(ByteBuffer, int)}.
     */
    int write(byte[] data);

    /**
     * Whether this implementation supports direct-buffer writes through {@link
     * #writeDirect(ByteBuffer, int)}.
     */
    default boolean supportDirect() {
        return false;
    }

    /**
     * Appends bytes to the file from a native-managed direct buffer.
     *
     * <p>The direct buffer lifecycle is owned by the native layer; implementations must not retain
     * the buffer reference after this method returns.
     */
    default int writeDirect(ByteBuffer data, int length) {
        throw new UnsupportedOperationException("Direct write is not supported.");
    }

    /** Number of bytes written to the file. */
    long size();

    @Override
    void close();
}
