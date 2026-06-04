package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One direct-buffer-backed priority queue entry returned by {@link PriorityQueue} direct peek/poll
 * APIs.
 *
 * <p>The key and value are exposed as direct {@link ByteBuffer} views over one payload. Standalone
 * entries returned by direct single-entry APIs own that payload and must be closed when no longer
 * needed. Entries obtained from a {@link DirectPriorityQueueBatch} are borrowed views and remain
 * valid only until their parent batch is closed.
 */
public final class DirectPriorityQueueEntry implements AutoCloseable {
    private static final Runnable NO_OP =
            new Runnable() {
                @Override
                public void run() {}
            };

    private final ByteBuffer key;
    private final ByteBuffer value;
    private final Runnable onClose;
    private final AtomicBoolean closed;

    DirectPriorityQueueEntry(ByteBuffer key, ByteBuffer value, Runnable onClose) {
        if (key == null) {
            throw new NullPointerException("key");
        }
        if (value == null) {
            throw new NullPointerException("value");
        }
        this.key = key;
        this.value = value;
        this.onClose = onClose == null ? NO_OP : onClose;
        this.closed = new AtomicBoolean(false);
    }

    static DirectPriorityQueueEntry decode(
            ByteBuffer encoded, int encodedLength, Runnable onClose) {
        Runnable releaser = onClose == null ? NO_OP : onClose;
        try {
            ByteBuffer view = encoded.duplicate();
            ((Buffer) view).clear();
            if (encodedLength < Integer.BYTES * 2 || encodedLength > view.capacity()) {
                throw new IllegalStateException(
                        "invalid encoded direct priority queue entry length: " + encodedLength);
            }
            ((Buffer) view).limit(encodedLength);
            int keyLength = readLength(view, "key length");
            ByteBuffer key = sliceCurrent(view, keyLength);
            int valueLength = readLength(view, "value length");
            ByteBuffer value = sliceCurrent(view, valueLength);
            if (view.hasRemaining()) {
                throw new IllegalStateException(
                        "unexpected trailing bytes in direct priority queue entry payload");
            }
            return new DirectPriorityQueueEntry(key, value, releaser);
        } catch (RuntimeException e) {
            releaseAfterDecodeFailure(releaser, e);
            throw e;
        }
    }

    ByteBuffer keyBufferView() {
        return key;
    }

    ByteBuffer valueBufferView() {
        return value;
    }

    /** Returns one direct key view that stays valid until the owning entry or batch is closed. */
    public ByteBuffer getKey() {
        return key.duplicate();
    }

    /** Returns one direct value view that stays valid until the owning entry or batch is closed. */
    public ByteBuffer getValue() {
        return value.duplicate();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
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
            throw new IllegalStateException("malformed direct priority queue entry payload");
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
