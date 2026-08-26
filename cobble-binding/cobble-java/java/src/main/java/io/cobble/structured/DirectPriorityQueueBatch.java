package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One direct-buffer-backed priority queue batch returned by {@link PriorityQueue} direct batch
 * peek/poll APIs.
 *
 * <p>All entries share one payload. Entries obtained from this batch are borrowed views: close the
 * batch only after all of those views are no longer needed. Closing an individual borrowed entry
 * does not release the batch payload.
 */
public final class DirectPriorityQueueBatch
        implements AutoCloseable, Iterable<DirectPriorityQueueEntry> {
    private static final Runnable NO_OP =
            new Runnable() {
                @Override
                public void run() {}
            };

    private final List<DirectPriorityQueueEntry> entries;
    private final Runnable onClose;
    private final AtomicBoolean closed;

    private DirectPriorityQueueBatch(List<DirectPriorityQueueEntry> entries, Runnable onClose) {
        this.entries = entries;
        this.onClose = onClose == null ? NO_OP : onClose;
        this.closed = new AtomicBoolean(false);
    }

    static DirectPriorityQueueBatch empty() {
        return new DirectPriorityQueueBatch(Collections.emptyList(), NO_OP);
    }

    static DirectPriorityQueueBatch decode(
            ByteBuffer encoded, int encodedLength, Runnable onClose) {
        Runnable releaser = onClose == null ? NO_OP : onClose;
        try {
            ByteBuffer view = encoded.duplicate();
            ((Buffer) view).clear();
            if (encodedLength < Integer.BYTES || encodedLength > view.capacity()) {
                throw new IllegalStateException(
                        "invalid encoded direct priority queue batch length: " + encodedLength);
            }
            ((Buffer) view).limit(encodedLength);
            int entryCount = readLength(view, "entry count");
            if (entryCount > view.remaining() / (Integer.BYTES * 2)) {
                throw new IllegalStateException(
                        "invalid direct priority queue batch entry count: " + entryCount);
            }
            List<DirectPriorityQueueEntry> decoded = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                int keyLength = readLength(view, "key length");
                ByteBuffer key = sliceCurrent(view, keyLength);
                int valueLength = readLength(view, "value length");
                ByteBuffer value = sliceCurrent(view, valueLength);
                decoded.add(new DirectPriorityQueueEntry(key, value, null));
            }
            if (view.hasRemaining()) {
                throw new IllegalStateException(
                        "unexpected trailing bytes in direct priority queue batch payload");
            }
            return new DirectPriorityQueueBatch(Collections.unmodifiableList(decoded), releaser);
        } catch (RuntimeException e) {
            releaseAfterDecodeFailure(releaser, e);
            throw e;
        }
    }

    DirectPriorityQueueEntry takeSingleEntryOrNull() {
        if (entries.isEmpty()) {
            close();
            return null;
        }
        if (entries.size() != 1) {
            close();
            throw new IllegalStateException(
                    "expected exactly one direct priority queue entry, got " + entries.size());
        }
        DirectPriorityQueueEntry entry = entries.get(0);
        return new DirectPriorityQueueEntry(
                entry.keyBufferView().duplicate(),
                entry.valueBufferView().duplicate(),
                takeReleaser());
    }

    /** Returns the number of entries in this batch. */
    public int size() {
        return entries.size();
    }

    /** Returns whether this batch is empty. */
    public boolean isEmpty() {
        return entries.isEmpty();
    }

    /** Returns one borrowed entry view that stays valid until this batch is closed. */
    public DirectPriorityQueueEntry get(int index) {
        return entries.get(index);
    }

    /**
     * Returns an immutable list of borrowed entry views that stay valid until this batch closes.
     */
    public List<DirectPriorityQueueEntry> asList() {
        return entries;
    }

    @Override
    public Iterator<DirectPriorityQueueEntry> iterator() {
        return entries.iterator();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }

    private Runnable takeReleaser() {
        if (closed.compareAndSet(false, true)) {
            return onClose;
        }
        return NO_OP;
    }

    private static void releaseAfterDecodeFailure(Runnable releaser, RuntimeException failure) {
        try {
            releaser.run();
        } catch (RuntimeException releaseFailure) {
            failure.addSuppressed(releaseFailure);
        }
    }

    private static void ensureRemaining(ByteBuffer view, int size) {
        if (size < 0 || view.remaining() < size) {
            throw new IllegalStateException("malformed direct priority queue batch payload");
        }
    }

    private static int readLength(ByteBuffer view, String fieldName) {
        ensureRemaining(view, Integer.BYTES);
        int len = view.getInt();
        if (len < 0) {
            throw new IllegalStateException("invalid " + fieldName + ": " + len);
        }
        return len;
    }

    private static ByteBuffer sliceCurrent(ByteBuffer view, int len) {
        ensureRemaining(view, len);
        ByteBuffer slice = view.slice();
        ((Buffer) slice).limit(len);
        ((Buffer) view).position(view.position() + len);
        return slice;
    }
}
