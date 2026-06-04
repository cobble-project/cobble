package io.cobble.structured;

import io.cobble.DirectIoUtils;
import io.cobble.NativeObject;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Column-family-scoped priority queue for structured Java bindings.
 *
 * <p>Each queue owns one dedicated column family and stores one bytes column. Items are ordered by
 * key within each bucket. Re-offering the same key uses Cobble's bytes merge semantics, so values
 * append in offer order.
 *
 * <p>Each instance owns a native priority-queue handle. The handle caches the resolved Rust-side
 * queue metadata, including the column-family id used by cursor operations, so hot-path queue
 * operations do not route back through {@link Db} or {@link SingleDb} JNI bridge methods. The
 * parent object is retained only to keep the backing DB handle reachable for this non-owning native
 * queue handle; close queues before closing their parent DB.
 */
public final class PriorityQueue extends NativeObject {
    private final NativeObject owner;
    private final String columnFamily;
    private final DirectBufferPool directBufferPool;

    PriorityQueue(NativeObject owner, long nativeHandle) {
        super(nativeHandle);
        if (owner == null) {
            throw new NullPointerException("owner");
        }
        this.owner = owner;
        this.columnFamily = columnFamily(nativeHandle);
        int[] directPoolConfig = directBufferPoolConfig(nativeHandle);
        if (directPoolConfig == null || directPoolConfig.length != 2) {
            throw new IllegalStateException("failed to load direct buffer pool config from queue");
        }
        this.directBufferPool = new DirectBufferPool(directPoolConfig[0], directPoolConfig[1]);
    }

    /** Returns the normalized backing column-family name for this queue. */
    public String columnFamily() {
        return columnFamily;
    }

    /** Upsert one queue item. Re-offering the same key merges bytes values. */
    public void offer(int bucket, byte[] key, byte[] value) {
        priorityQueueOffer(nativeHandle, bucket, key, value);
    }

    /**
     * Upsert one queue item with caller-owned direct key/value buffers.
     *
     * <p>Re-offering the same key merges bytes values. The bytes in {@code [0, limit)} are consumed
     * from each direct buffer.
     */
    public void offerDirect(int bucket, ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        if (valueBuffer == null || !valueBuffer.isDirect()) {
            throw new IllegalArgumentException("valueBuffer must be a direct ByteBuffer");
        }
        offerDirect(
                bucket,
                keyBuffer,
                ((Buffer) keyBuffer).limit(),
                valueBuffer,
                ((Buffer) valueBuffer).limit());
    }

    /**
     * Upsert one queue item with caller-owned direct key/value buffers.
     *
     * <p>The bytes in {@code keyBuffer[0..keyLength)} and {@code valueBuffer[0..valueLength)} are
     * consumed directly without JNI heap-array materialization.
     */
    public void offerDirect(
            int bucket,
            ByteBuffer keyBuffer,
            int keyLength,
            ByteBuffer valueBuffer,
            int valueLength) {
        validateDirectSlice("keyBuffer", keyBuffer, keyLength);
        validateDirectSlice("valueBuffer", valueBuffer, valueLength);
        priorityQueueOfferDirect(
                nativeHandle,
                bucket,
                DirectIoUtils.directAddress(keyBuffer),
                keyBuffer.capacity(),
                keyLength,
                DirectIoUtils.directAddress(valueBuffer),
                valueBuffer.capacity(),
                valueLength);
    }

    /** Delete one queue item if it exists. */
    public void delete(int bucket, byte[] key) {
        priorityQueueDelete(nativeHandle, bucket, key);
    }

    /** Return the smallest key in the queue, or {@code null} if the queue is empty. */
    public Entry peek(int bucket) {
        byte[][] pair = priorityQueuePeek(nativeHandle, bucket);
        if (pair == null) {
            return null;
        }
        if (pair.length != 2) {
            throw new IllegalStateException(
                    "priority queue peek returned malformed pair length " + pair.length);
        }
        return new Entry(pair[0], pair[1]);
    }

    /** Return and remove the smallest key in the queue, or {@code null} if the queue is empty. */
    public Entry poll(int bucket) {
        byte[][] pair = priorityQueuePoll(nativeHandle, bucket);
        if (pair == null) {
            return null;
        }
        if (pair.length != 2) {
            throw new IllegalStateException(
                    "priority queue poll returned malformed pair length " + pair.length);
        }
        return new Entry(pair[0], pair[1]);
    }

    /**
     * Return the smallest key in the queue through direct buffers.
     *
     * <p>The returned key/value views stay valid until the entry is closed.
     */
    public DirectPriorityQueueEntry peekDirect(int bucket) {
        return peekBatchDirect(bucket, 1).takeSingleEntryOrNull();
    }

    /**
     * Return and remove the smallest key in the queue through direct buffers.
     *
     * <p>The returned key/value views stay valid until the entry is closed.
     */
    public DirectPriorityQueueEntry pollDirect(int bucket) {
        return pollBatchDirect(bucket, 1).takeSingleEntryOrNull();
    }

    /**
     * Return and remove up to {@code batchSize} smallest keys.
     *
     * <p>Passing {@code 0} returns an empty list immediately.
     */
    public List<Entry> pollBatch(int bucket, int batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize must be >= 0");
        }
        if (batchSize == 0) {
            return Collections.emptyList();
        }
        return decodeBatch(priorityQueuePollBatch(nativeHandle, bucket, batchSize));
    }

    /**
     * Return up to {@code batchSize} smallest keys without advancing the queue cursor.
     *
     * <p>Passing {@code 0} returns an empty list immediately.
     */
    public List<Entry> peekBatch(int bucket, int batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize must be >= 0");
        }
        if (batchSize == 0) {
            return Collections.emptyList();
        }
        return decodeBatch(priorityQueuePeekBatch(nativeHandle, bucket, batchSize));
    }

    /**
     * Return and remove up to {@code batchSize} smallest keys through direct buffers.
     *
     * <p>The returned entry views share one native payload, so callers should close the batch when
     * finished.
     */
    public DirectPriorityQueueBatch pollBatchDirect(int bucket, int batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize must be >= 0");
        }
        if (batchSize == 0) {
            return DirectPriorityQueueBatch.empty();
        }
        EncodedDirectResult encoded = readPriorityQueueDirectResult(bucket, batchSize, false);
        if (encoded == null) {
            return DirectPriorityQueueBatch.empty();
        }
        return DirectPriorityQueueBatch.decode(encoded.buffer, encoded.length, encoded.releaser);
    }

    /**
     * Return up to {@code batchSize} smallest keys through direct buffers without advancing the
     * queue cursor.
     *
     * <p>The returned entry views share one native payload, so callers should close the batch when
     * finished.
     */
    public DirectPriorityQueueBatch peekBatchDirect(int bucket, int batchSize) {
        if (batchSize < 0) {
            throw new IllegalArgumentException("batchSize must be >= 0");
        }
        if (batchSize == 0) {
            return DirectPriorityQueueBatch.empty();
        }
        EncodedDirectResult encoded = readPriorityQueueDirectResult(bucket, batchSize, true);
        if (encoded == null) {
            return DirectPriorityQueueBatch.empty();
        }
        return DirectPriorityQueueBatch.decode(encoded.buffer, encoded.length, encoded.releaser);
    }

    /**
     * Return and remove one physical-boundary-sized batch.
     *
     * <p>Internally this asks Cobble to stop after the next SST block, Parquet row group, or file
     * boundary when such a boundary exists. Sources without boundary semantics keep producing rows
     * normally.
     */
    public List<Entry> pollBatch(int bucket) {
        return decodeBatch(priorityQueuePollBatch(nativeHandle, bucket, -1));
    }

    /**
     * Return one physical-boundary-sized batch without advancing the queue cursor.
     *
     * <p>Internally this asks Cobble to stop after the next SST block, Parquet row group, or file
     * boundary when such a boundary exists. Sources without boundary semantics keep producing rows
     * normally.
     */
    public List<Entry> peekBatch(int bucket) {
        return decodeBatch(priorityQueuePeekBatch(nativeHandle, bucket, -1));
    }

    /**
     * Return and remove one physical-boundary-sized batch through direct buffers.
     *
     * <p>The returned entry views share one native payload, so callers should close the batch when
     * finished.
     */
    public DirectPriorityQueueBatch pollBatchDirect(int bucket) {
        EncodedDirectResult encoded = readPriorityQueueDirectResult(bucket, -1, false);
        if (encoded == null) {
            return DirectPriorityQueueBatch.empty();
        }
        return DirectPriorityQueueBatch.decode(encoded.buffer, encoded.length, encoded.releaser);
    }

    /**
     * Return one physical-boundary-sized batch through direct buffers without advancing the queue
     * cursor.
     *
     * <p>The returned entry views share one native payload, so callers should close the batch when
     * finished.
     */
    public DirectPriorityQueueBatch peekBatchDirect(int bucket) {
        EncodedDirectResult encoded = readPriorityQueueDirectResult(bucket, -1, true);
        if (encoded == null) {
            return DirectPriorityQueueBatch.empty();
        }
        return DirectPriorityQueueBatch.decode(encoded.buffer, encoded.length, encoded.releaser);
    }

    /**
     * Advance the queue cursor to {@code key}, making that key and earlier keys invisible.
     *
     * <p>Queue cursors move forward monotonically, so advancing to an older key has no effect.
     * Items subsequently offered at or before the cursor remain invisible.
     */
    public void advance(int bucket, byte[] key) {
        priorityQueueAdvance(nativeHandle, bucket, key);
    }

    /**
     * Advance the queue cursor from a caller-owned direct key buffer.
     *
     * <p>The bytes in {@code keyBuffer[0..limit)} are consumed. Items subsequently offered at or
     * before the cursor remain invisible.
     */
    public void advance(int bucket, ByteBuffer keyBuffer) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        advance(bucket, keyBuffer, ((Buffer) keyBuffer).limit());
    }

    /**
     * Advance the queue cursor from a caller-owned direct key buffer.
     *
     * <p>The bytes in {@code keyBuffer[0..keyLength)} are consumed directly without JNI heap-array
     * materialization. Items subsequently offered at or before the cursor remain invisible.
     */
    public void advance(int bucket, ByteBuffer keyBuffer, int keyLength) {
        validateDirectSlice("keyBuffer", keyBuffer, keyLength);
        priorityQueueAdvanceDirect(
                nativeHandle,
                bucket,
                DirectIoUtils.directAddress(keyBuffer),
                keyBuffer.capacity(),
                keyLength);
    }

    /** Return the current queue cursor, or {@code null} if this bucket has not consumed items. */
    public byte[] cursor(int bucket) {
        return priorityQueueCursor(nativeHandle, bucket);
    }

    private static void validateDirectSlice(String name, ByteBuffer buffer, int length) {
        if (buffer == null || !buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct ByteBuffer");
        }
        if (length < 0 || length > buffer.capacity()) {
            throw new IllegalArgumentException(name + " length out of range: " + length);
        }
    }

    private static List<Entry> decodeBatch(byte[][] flatPairs) {
        if (flatPairs == null || flatPairs.length == 0) {
            return Collections.emptyList();
        }
        if ((flatPairs.length & 1) != 0) {
            throw new IllegalStateException(
                    "priority queue batch returned malformed pair length " + flatPairs.length);
        }
        List<Entry> entries = new ArrayList<Entry>(flatPairs.length / 2);
        for (int i = 0; i < flatPairs.length; i += 2) {
            entries.add(new Entry(flatPairs[i], flatPairs[i + 1]));
        }
        return entries;
    }

    private EncodedDirectResult readPriorityQueueDirectResult(
            int bucket, int batchSize, boolean peek) {
        ByteBuffer pooled = directBufferPool.acquire();
        boolean pooledReleased = false;
        boolean usingPooledForResult = false;
        try {
            int encodedLength =
                    peek
                            ? priorityQueuePeekBatchDirect(
                                    nativeHandle,
                                    bucket,
                                    batchSize,
                                    DirectIoUtils.directAddress(pooled),
                                    pooled.capacity())
                            : priorityQueuePollBatchDirect(
                                    nativeHandle,
                                    bucket,
                                    batchSize,
                                    DirectIoUtils.directAddress(pooled),
                                    pooled.capacity());
            if (encodedLength == 0) {
                directBufferPool.release(pooled);
                return null;
            }
            ByteBuffer resultBuffer =
                    DirectIoUtils.resolveEncodedBuffer(
                            pooled, encodedLength, PriorityQueue::getLastDirectOverflowBuffer);
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                usingPooledForResult = true;
                AtomicBoolean released = new AtomicBoolean(false);
                Runnable releaser =
                        () -> {
                            if (released.compareAndSet(false, true)) {
                                directBufferPool.release(pooled);
                            }
                        };
                return new EncodedDirectResult(resultBuffer, decodedLength, releaser);
            }
            // JNI overflow buffers are thread-local scratch space. Copy before returning a
            // close-scoped Java view that may outlive the next JNI call on this thread.
            resultBuffer = DirectIoUtils.copyDirectPrefix(resultBuffer, decodedLength);
            directBufferPool.release(pooled);
            pooledReleased = true;
            return new EncodedDirectResult(resultBuffer, decodedLength, () -> {});
        } catch (RuntimeException e) {
            if (!pooledReleased && !usingPooledForResult) {
                directBufferPool.release(pooled);
            }
            throw e;
        }
    }

    private static final class EncodedDirectResult {
        private final ByteBuffer buffer;
        private final int length;
        private final Runnable releaser;

        private EncodedDirectResult(ByteBuffer buffer, int length, Runnable releaser) {
            this.buffer = buffer;
            this.length = length;
            this.releaser = releaser;
        }
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native String columnFamily(long nativeHandle);

    private static native int[] directBufferPoolConfig(long nativeHandle);

    private static native void priorityQueueOffer(
            long nativeHandle, int bucket, byte[] key, byte[] value);

    private static native void priorityQueueOfferDirect(
            long nativeHandle,
            int bucket,
            long keyAddress,
            int keyCapacity,
            int keyLength,
            long valueAddress,
            int valueCapacity,
            int valueLength);

    private static native void priorityQueueDelete(long nativeHandle, int bucket, byte[] key);

    private static native void priorityQueueAdvance(long nativeHandle, int bucket, byte[] key);

    private static native void priorityQueueAdvanceDirect(
            long nativeHandle, int bucket, long keyAddress, int keyCapacity, int keyLength);

    private static native byte[] priorityQueueCursor(long nativeHandle, int bucket);

    private static native byte[][] priorityQueuePeek(long nativeHandle, int bucket);

    private static native byte[][] priorityQueuePeekBatch(
            long nativeHandle, int bucket, int batchSize);

    private static native byte[][] priorityQueuePoll(long nativeHandle, int bucket);

    private static native byte[][] priorityQueuePollBatch(
            long nativeHandle, int bucket, int batchSize);

    private static native int priorityQueuePeekBatchDirect(
            long nativeHandle, int bucket, int batchSize, long ioAddress, int ioCapacity);

    private static native int priorityQueuePollBatchDirect(
            long nativeHandle, int bucket, int batchSize, long ioAddress, int ioCapacity);

    static native ByteBuffer getLastDirectOverflowBuffer();

    /** One queue item returned by the heap-array-backed peek/poll APIs. */
    public static final class Entry {
        private final byte[] key;
        private final byte[] value;

        Entry(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        /** Returns this item's queue key. */
        public byte[] getKey() {
            return key;
        }

        /** Returns this item's bytes value. */
        public byte[] getValue() {
            return value;
        }
    }
}
