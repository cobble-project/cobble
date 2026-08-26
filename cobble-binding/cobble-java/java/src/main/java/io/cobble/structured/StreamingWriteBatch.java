package io.cobble.structured;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Streams byte-column key/value pairs into fixed-size direct buffers and writes completed entries
 * in batches.
 *
 * <p>Write one key to {@link #output()}, call {@link #finishKey()}, write its value to the same
 * stream, then call {@link #finishElement()}. The batch submits automatically after completed
 * entries reach the configured buffer size. A key or value may span any number of buffers.
 */
public final class StreamingWriteBatch implements AutoCloseable {
    private final Db db;
    private final int bucket;
    private final int column;
    private final WriteOptions options;
    private final int flushThresholdBytes;
    private final DirectChunkedOutputStream chunks;
    private final DataOutputStream output;

    private int[] keyLengths = new int[16];
    private int[] valueLengths = new int[16];
    private int entryCount;
    private int fieldStart;
    private boolean writingValue;
    private boolean closed;

    StreamingWriteBatch(Db db, int bucket, int column, WriteOptions options, int bufferSizeBytes) {
        this.db = db;
        this.bucket = bucket;
        this.column = column;
        this.options = options;
        this.flushThresholdBytes = bufferSizeBytes;
        this.chunks =
                new DirectChunkedOutputStream(
                        Math.min(bufferSizeBytes, DirectChunkedOutputStream.MAX_CHUNK_SIZE_BYTES));
        this.output = new DataOutputStream(chunks);
    }

    /** Returns the stream used for the current key or value. Do not close this stream directly. */
    public DataOutputStream output() {
        ensureOpen();
        return output;
    }

    /** Marks the current key complete and switches the stream to its value. */
    public void finishKey() {
        ensureOpen();
        if (writingValue) {
            throw new IllegalStateException("the current key is already complete");
        }
        ensureEntryCapacity(entryCount + 1);
        keyLengths[entryCount] = chunks.size() - fieldStart;
        fieldStart = chunks.size();
        writingValue = true;
    }

    /** Marks the current value complete and automatically flushes a full batch. */
    public void finishElement() {
        ensureOpen();
        if (!writingValue) {
            throw new IllegalStateException("finishKey() must be called before finishElement()");
        }
        valueLengths[entryCount] = chunks.size() - fieldStart;
        entryCount++;
        fieldStart = chunks.size();
        writingValue = false;
        if (chunks.size() >= flushThresholdBytes) {
            flush();
        }
    }

    /** Writes all completed entries and retains reusable buffers for the next batch. */
    public void flush() {
        ensureOpen();
        ensureAtEntryBoundary();
        if (entryCount == 0) {
            return;
        }
        db.putStreamingBatchWithOptions(
                bucket,
                column,
                chunks.addresses(),
                chunks.lengths(),
                keyLengths,
                valueLengths,
                entryCount,
                options);
        resetAfterFlush();
    }

    /** Discards entries that have not yet been submitted. */
    public void clear() {
        ensureOpen();
        chunks.reset();
        entryCount = 0;
        fieldStart = 0;
        writingValue = false;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            ensureAtEntryBoundary();
            flush();
        } catch (RuntimeException error) {
            throw new IOException("Failed to close streaming write batch", error);
        } finally {
            closed = true;
        }
    }

    private void resetAfterFlush() {
        chunks.reset();
        entryCount = 0;
        fieldStart = 0;
    }

    private void ensureEntryCapacity(int required) {
        if (required <= keyLengths.length) {
            return;
        }
        int next = Math.max(required, keyLengths.length << 1);
        keyLengths = Arrays.copyOf(keyLengths, next);
        valueLengths = Arrays.copyOf(valueLengths, next);
    }

    private void ensureAtEntryBoundary() {
        if (writingValue || chunks.size() != fieldStart) {
            throw new IllegalStateException("the current streaming write entry is incomplete");
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("streaming write batch is closed");
        }
    }
}
