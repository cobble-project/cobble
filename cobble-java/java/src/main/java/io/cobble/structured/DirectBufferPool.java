package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

final class DirectBufferPool {
    static final int DEFAULT_BUFFER_SIZE_BYTES = 2 * 1024;
    static final int DEFAULT_MAX_POOL_SIZE = 64;

    private final int bufferSizeBytes;
    private final int maxPoolSize;
    private final ConcurrentLinkedQueue<ByteBuffer> buffers;
    private final AtomicInteger pooledCount;

    DirectBufferPool(int bufferSizeBytes, int maxPoolSize) {
        if (bufferSizeBytes <= 0) {
            throw new IllegalArgumentException("bufferSizeBytes must be > 0");
        }
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("maxPoolSize must be > 0");
        }
        this.bufferSizeBytes = bufferSizeBytes;
        this.maxPoolSize = maxPoolSize;
        this.buffers = new ConcurrentLinkedQueue<>();
        this.pooledCount = new AtomicInteger(0);
    }

    static DirectBufferPool defaults() {
        return new DirectBufferPool(DEFAULT_BUFFER_SIZE_BYTES, DEFAULT_MAX_POOL_SIZE);
    }

    int bufferSizeBytes() {
        return bufferSizeBytes;
    }

    int maxPoolSize() {
        return maxPoolSize;
    }

    ByteBuffer acquire() {
        ByteBuffer pooled = buffers.poll();
        if (pooled != null) {
            pooledCount.decrementAndGet();
            ((Buffer) pooled).clear();
            return pooled;
        }
        return ByteBuffer.allocateDirect(bufferSizeBytes);
    }

    void release(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() != bufferSizeBytes) {
            return;
        }
        int next = pooledCount.incrementAndGet();
        if (next > maxPoolSize) {
            pooledCount.decrementAndGet();
            return;
        }
        ((Buffer) buffer).clear();
        buffers.offer(buffer);
    }
}
