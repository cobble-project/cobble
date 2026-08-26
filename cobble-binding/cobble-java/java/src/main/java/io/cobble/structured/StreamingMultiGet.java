package io.cobble.structured;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Streams lookup keys into direct buffers and exposes results without per-value byte arrays. */
public final class StreamingMultiGet implements AutoCloseable {
    private final Db db;
    private final ReadOptions options;
    private final DirectChunkedOutputStream chunks;
    private final DataOutputStream keyOutput;
    private ByteBuffer resultBuffer;

    private int[] buckets = new int[16];
    private int[] keyLengths = new int[16];
    private int keyCount;
    private int keyStart;
    private boolean resultOpen;
    private boolean closed;

    StreamingMultiGet(Db db, ReadOptions options, int bufferSizeBytes) {
        this.db = db;
        this.options = options;
        this.chunks =
                new DirectChunkedOutputStream(
                        Math.min(bufferSizeBytes, DirectChunkedOutputStream.MAX_CHUNK_SIZE_BYTES));
        this.keyOutput = new DataOutputStream(chunks);
        this.resultBuffer = ByteBuffer.allocateDirect(bufferSizeBytes);
    }

    /** Returns the stream used to serialize the current key. Do not close it directly. */
    public DataOutputStream keyOutput() {
        ensureWritable();
        return keyOutput;
    }

    /** Marks the current key complete and associates it with a Cobble bucket. */
    public void finishKey(int bucket) {
        ensureWritable();
        if (bucket < 0 || bucket > 0xffff) {
            throw new IllegalArgumentException("bucket out of range: " + bucket);
        }
        ensureKeyCapacity(keyCount + 1);
        buckets[keyCount] = bucket;
        keyLengths[keyCount] = chunks.size() - keyStart;
        keyCount++;
        keyStart = chunks.size();
    }

    /** Executes the accumulated keys and returns an ordered stream of optional byte values. */
    public StreamingMultiGetResult execute() {
        ensureWritable();
        if (chunks.size() != keyStart) {
            throw new IllegalStateException("the current streaming lookup key is incomplete");
        }
        if (keyCount == 0) {
            throw new IllegalStateException("streaming multi-get contains no keys");
        }
        long[] addresses = chunks.addresses();
        int[] chunkLengths = chunks.lengths();
        int encodedLength =
                db.executeStreamingMultiGet(
                        addresses,
                        chunkLengths,
                        buckets,
                        keyLengths,
                        keyCount,
                        resultBuffer,
                        options);
        if (encodedLength < 0) {
            resultBuffer = ByteBuffer.allocateDirect(Math.negateExact(encodedLength));
            encodedLength =
                    db.executeStreamingMultiGet(
                            addresses,
                            chunkLengths,
                            buckets,
                            keyLengths,
                            keyCount,
                            resultBuffer,
                            options);
        }
        if (encodedLength <= 0) {
            throw new IllegalStateException(
                    "Cobble streaming multi-get could not fit its result after resizing: "
                            + encodedLength);
        }
        StreamingMultiGetResult result =
                new StreamingMultiGetResult(this, resultBuffer, encodedLength, keyCount);
        chunks.reset();
        keyCount = 0;
        keyStart = 0;
        resultOpen = true;
        return result;
    }

    /** Discards lookup keys that have not been executed. */
    public void clear() {
        ensureWritable();
        chunks.reset();
        keyCount = 0;
        keyStart = 0;
    }

    @Override
    public void close() {
        if (resultOpen) {
            throw new IllegalStateException("close the streaming multi-get result first");
        }
        closed = true;
    }

    void releaseResult() {
        resultOpen = false;
    }

    private void ensureKeyCapacity(int required) {
        if (required <= buckets.length) {
            return;
        }
        int next = Math.max(required, buckets.length << 1);
        buckets = Arrays.copyOf(buckets, next);
        keyLengths = Arrays.copyOf(keyLengths, next);
    }

    private void ensureWritable() {
        if (closed) {
            throw new IllegalStateException("streaming multi-get is closed");
        }
        if (resultOpen) {
            throw new IllegalStateException("close the previous streaming multi-get result first");
        }
    }
}
