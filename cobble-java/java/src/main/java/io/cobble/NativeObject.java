package io.cobble;

import java.util.concurrent.atomic.AtomicBoolean;

/** Base class for Java objects that hold a native pointer. */
public abstract class NativeObject implements AutoCloseable {
    protected volatile long nativeHandle;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    protected NativeObject(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    @Override
    public void close() {
        if (disposed.compareAndSet(false, true)) {
            long handle = nativeHandle;
            nativeHandle = 0L;
            if (handle != 0L) {
                disposeInternal(handle);
            }
        }
    }

    public boolean isDisposed() {
        return disposed.get();
    }

    protected abstract void disposeInternal(long nativeHandle);
}
