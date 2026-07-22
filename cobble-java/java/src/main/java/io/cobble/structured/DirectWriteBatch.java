package io.cobble.structured;

import java.nio.Buffer;
import java.nio.ByteBuffer;

/** A reusable direct-buffer batch for byte-column puts. */
public final class DirectWriteBatch {
    private static final int ENTRY_HEADER_BYTES = Integer.BYTES * 2;

    private ByteBuffer encoded;
    private int size;

    public DirectWriteBatch(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be >= 0");
        }
        encoded = ByteBuffer.allocateDirect(Math.max(1, initialCapacity));
    }

    /** Removes all entries while retaining the allocated direct buffer. */
    public void clear() {
        ((Buffer) encoded).clear();
        size = 0;
    }

    /** Appends one key/value pair by copying the readable prefixes of the supplied buffers. */
    public void put(ByteBuffer key, int keyLength, ByteBuffer value, int valueLength) {
        validateDirectSlice("key", key, keyLength);
        validateDirectSlice("value", value, valueLength);
        int entryLength;
        try {
            entryLength = Math.addExact(ENTRY_HEADER_BYTES, Math.addExact(keyLength, valueLength));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("direct write batch entry is too large", overflow);
        }
        ensureCapacity(entryLength);
        encoded.putInt(keyLength);
        encoded.putInt(valueLength);
        putPrefix(encoded, key, keyLength);
        putPrefix(encoded, value, valueLength);
        size++;
    }

    /** Returns the number of entries currently in the batch. */
    public int size() {
        return size;
    }

    /** Returns whether this batch contains no entries. */
    public boolean isEmpty() {
        return size == 0;
    }

    ByteBuffer encodedBuffer() {
        return encoded;
    }

    int encodedLength() {
        return encoded.position();
    }

    private void ensureCapacity(int additionalBytes) {
        int required;
        try {
            required = Math.addExact(encoded.position(), additionalBytes);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("direct write batch is too large", overflow);
        }
        if (required <= encoded.capacity()) {
            return;
        }
        int nextCapacity = encoded.capacity();
        while (nextCapacity < required) {
            int doubled = nextCapacity << 1;
            nextCapacity = doubled > nextCapacity ? Math.max(required, doubled) : required;
        }
        ByteBuffer replacement = ByteBuffer.allocateDirect(nextCapacity);
        ByteBuffer source = encoded.duplicate();
        ((Buffer) source).clear();
        ((Buffer) source).limit(encoded.position());
        replacement.put(source);
        encoded = replacement;
    }

    private static void validateDirectSlice(String name, ByteBuffer buffer, int length) {
        if (buffer == null || !buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct ByteBuffer");
        }
        if (length < 0 || length > buffer.capacity()) {
            throw new IllegalArgumentException(name + "Length out of range: " + length);
        }
    }

    private static void putPrefix(ByteBuffer target, ByteBuffer source, int length) {
        ByteBuffer view = source.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).limit(length);
        target.put(view);
    }
}
