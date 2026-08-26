package io.cobble;

import java.io.InputStream;

/** Reusable InputStream view over a direct-memory address range. */
final class ReusableDirectAddressInputStream extends InputStream {
    private long currentAddress;
    private int remaining;

    void reset(long startAddress, int length) {
        this.currentAddress = startAddress;
        this.remaining = length;
    }

    @Override
    public int read() {
        if (remaining <= 0) {
            return -1;
        }
        int value = UnsafeAccess.getByte(currentAddress) & 0xFF;
        currentAddress += 1;
        remaining -= 1;
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new NullPointerException("bytes");
        }
        if ((offset | length | (offset + length) | (bytes.length - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException("invalid byte[] range");
        }
        if (length == 0) {
            return 0;
        }
        if (remaining <= 0) {
            return -1;
        }
        int readable = Math.min(length, remaining);
        UnsafeAccess.copyMemory(currentAddress, bytes, offset, readable);
        currentAddress += readable;
        remaining -= readable;
        return readable;
    }
}
