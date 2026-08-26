package io.cobble;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

/** Internal unsafe helper for direct-memory access in hot paths. */
public final class UnsafeAccess {
    private static final Unsafe UNSAFE = loadUnsafe();
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private UnsafeAccess() {}

    public static long byteArrayBaseOffset() {
        return BYTE_ARRAY_BASE_OFFSET;
    }

    public static byte getByte(long address) {
        return UNSAFE.getByte(address);
    }

    public static int getIntBigEndian(long address) {
        int raw = UNSAFE.getInt(address);
        return LITTLE_ENDIAN ? Integer.reverseBytes(raw) : raw;
    }

    public static void copyMemory(byte[] source, int sourceOffset, long targetAddress, int length) {
        if (length < 0 || sourceOffset < 0 || sourceOffset > source.length - length) {
            throw new IllegalArgumentException("invalid byte[] source range");
        }
        if (length == 0) {
            return;
        }
        UNSAFE.copyMemory(
                source, BYTE_ARRAY_BASE_OFFSET + sourceOffset, null, targetAddress, (long) length);
    }

    public static void copyMemory(long sourceAddress, byte[] target, int targetOffset, int length) {
        if (length < 0 || targetOffset < 0 || targetOffset > target.length - length) {
            throw new IllegalArgumentException("invalid byte[] target range");
        }
        if (length == 0) {
            return;
        }
        UNSAFE.copyMemory(
                null, sourceAddress, target, BYTE_ARRAY_BASE_OFFSET + targetOffset, (long) length);
    }

    public static void copyMemory(long sourceAddress, long targetAddress, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0");
        }
        if (length == 0) {
            return;
        }
        UNSAFE.copyMemory(null, sourceAddress, null, targetAddress, (long) length);
    }

    private static Unsafe loadUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe) field.get(null);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("failed to initialize sun.misc.Unsafe", e);
        }
    }
}
