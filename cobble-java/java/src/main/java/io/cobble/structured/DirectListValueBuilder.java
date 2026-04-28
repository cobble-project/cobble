package io.cobble.structured;

import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Builds one Cobble core list payload directly into a reusable direct {@link ByteBuffer}. */
public final class DirectListValueBuilder {
    private final boolean preserveElementTtl;
    private final ElementOutputStream elementOutputStream;
    private ByteBuffer buffer;
    private int elementCount;
    private int currentElementLengthOffset = -1;
    private int currentElementDataOffset = -1;

    public DirectListValueBuilder() {
        this(256, false);
    }

    public DirectListValueBuilder(int initialCapacityBytes) {
        this(initialCapacityBytes, false);
    }

    public DirectListValueBuilder(int initialCapacityBytes, boolean preserveElementTtl) {
        this.preserveElementTtl = preserveElementTtl;
        this.elementOutputStream = new ElementOutputStream();
        this.buffer = allocate(Math.max(8, initialCapacityBytes));
        clear();
    }

    public void clear() {
        ((Buffer) buffer).clear();
        buffer.putInt(0);
        elementCount = 0;
        currentElementLengthOffset = -1;
        currentElementDataOffset = -1;
    }

    public void append(byte[] value) {
        if (value == null) {
            throw new IllegalArgumentException("value must not be null");
        }
        append(ByteBuffer.wrap(value), value.length, 0);
    }

    public void append(ByteBuffer valueBuffer, int valueLength) {
        append(valueBuffer, valueLength, 0);
    }

    public void append(ByteBuffer valueBuffer, int valueLength, int expiresAtSeconds) {
        if (valueBuffer == null) {
            throw new IllegalArgumentException("valueBuffer must not be null");
        }
        if (valueLength < 0 || valueLength > valueBuffer.capacity()) {
            throw new IllegalArgumentException("valueLength out of range: " + valueLength);
        }
        beginElement(expiresAtSeconds);
        ByteBuffer view = valueBuffer.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).limit(valueLength);
        elementOutputStream.write(view);
        finishElement();
    }

    public void beginElement() {
        beginElement(0);
    }

    public void beginElement(int expiresAtSeconds) {
        if (currentElementLengthOffset >= 0) {
            throw new IllegalStateException("Previous element has not been finished");
        }
        ensureCapacity(Integer.BYTES + (preserveElementTtl ? Integer.BYTES : 0));
        if (preserveElementTtl) {
            buffer.putInt(expiresAtSeconds);
        }
        currentElementLengthOffset = buffer.position();
        buffer.putInt(0);
        currentElementDataOffset = buffer.position();
    }

    public void finishElement() {
        if (currentElementLengthOffset < 0 || currentElementDataOffset < 0) {
            throw new IllegalStateException("No element is currently open");
        }
        buffer.putInt(currentElementLengthOffset, buffer.position() - currentElementDataOffset);
        elementCount++;
        buffer.putInt(0, elementCount);
        currentElementLengthOffset = -1;
        currentElementDataOffset = -1;
    }

    public OutputStream outputStream() {
        return elementOutputStream;
    }

    public int elementCount() {
        return elementCount;
    }

    public int length() {
        return buffer.position();
    }

    public ByteBuffer buffer() {
        ByteBuffer view = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        ((Buffer) view).clear();
        ((Buffer) view).limit(length());
        return view;
    }

    private void ensureCapacity(int additionalBytes) {
        int required = buffer.position() + additionalBytes;
        if (required <= buffer.capacity()) {
            return;
        }
        int newCapacity = buffer.capacity();
        while (newCapacity < required) {
            newCapacity = Math.max(required, newCapacity << 1);
        }
        ByteBuffer replacement = allocate(newCapacity);
        ByteBuffer copy = buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        ((Buffer) copy).clear();
        ((Buffer) copy).limit(buffer.position());
        replacement.put(copy);
        buffer = replacement;
    }

    private static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    private final class ElementOutputStream extends OutputStream {
        @Override
        public void write(int value) {
            ensureElementOpen();
            ensureCapacity(1);
            buffer.put((byte) value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || offset + length > bytes.length) {
                throw new IndexOutOfBoundsException("invalid offset/length");
            }
            ensureElementOpen();
            ensureCapacity(length);
            buffer.put(bytes, offset, length);
        }

        private void write(ByteBuffer source) {
            ensureElementOpen();
            ensureCapacity(source.remaining());
            buffer.put(source);
        }

        private void ensureElementOpen() {
            if (currentElementLengthOffset < 0) {
                throw new IllegalStateException("Call beginElement() before writing element bytes");
            }
        }
    }
}
