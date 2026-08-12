package io.cobble.structured;

import io.cobble.DirectIoUtils;

import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Fixed-size direct chunks used by the streaming batch APIs. */
final class DirectChunkedOutputStream extends OutputStream {
    static final int MAX_CHUNK_SIZE_BYTES = 64 * 1024;
    private static final int MAX_RETAINED_CHUNKS = 8;

    private final int chunkSize;
    private final List<ByteBuffer> chunks;
    private int currentIndex;
    private int size;

    DirectChunkedOutputStream(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be > 0");
        }
        this.chunkSize = chunkSize;
        this.chunks = new ArrayList<>();
        this.chunks.add(ByteBuffer.allocateDirect(chunkSize));
    }

    @Override
    public void write(int value) {
        ByteBuffer current = current();
        if (!current.hasRemaining()) {
            current = advanceChunk();
        }
        current.put((byte) value);
        size = Math.addExact(size, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IndexOutOfBoundsException("invalid offset/length");
        }
        int remaining = length;
        int sourceOffset = offset;
        while (remaining > 0) {
            ByteBuffer current = current();
            if (!current.hasRemaining()) {
                current = advanceChunk();
            }
            int copied = Math.min(current.remaining(), remaining);
            current.put(bytes, sourceOffset, copied);
            sourceOffset += copied;
            remaining -= copied;
            size = Math.addExact(size, copied);
        }
    }

    int size() {
        return size;
    }

    long[] addresses() {
        long[] addresses = new long[activeChunkCount()];
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = DirectIoUtils.directAddress(chunks.get(i));
        }
        return addresses;
    }

    int[] lengths() {
        int[] lengths = new int[activeChunkCount()];
        for (int i = 0; i < lengths.length; i++) {
            lengths[i] = chunks.get(i).position();
        }
        return lengths;
    }

    void reset() {
        int retained = Math.min(chunks.size(), MAX_RETAINED_CHUNKS);
        while (chunks.size() > retained) {
            chunks.remove(chunks.size() - 1);
        }
        for (ByteBuffer chunk : chunks) {
            ((Buffer) chunk).clear();
        }
        currentIndex = 0;
        size = 0;
    }

    private int activeChunkCount() {
        return size == 0 ? 0 : currentIndex + 1;
    }

    private ByteBuffer current() {
        return chunks.get(currentIndex);
    }

    private ByteBuffer advanceChunk() {
        currentIndex++;
        if (currentIndex == chunks.size()) {
            chunks.add(ByteBuffer.allocateDirect(chunkSize));
        }
        ByteBuffer next = chunks.get(currentIndex);
        ((Buffer) next).clear();
        return next;
    }
}
