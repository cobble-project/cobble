package io.cobble;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** Base class for Java objects that hold a native pointer. */
public abstract class NativeObject implements AutoCloseable {
    protected volatile long nativeHandle;
    private final AtomicBoolean disposed = new AtomicBoolean(false);

    protected NativeObject(long nativeHandle) {
        this.nativeHandle = nativeHandle;
    }

    /** Returns the raw native handle. For cross-package internal use. */
    public long getNativeHandle() {
        return nativeHandle;
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

    /** Parses metric samples returned by native binding methods. */
    protected static List<MetricSample> parseMetricSamples(String json) {
        return MetricSamples.fromJson(json);
    }

    protected abstract void disposeInternal(long nativeHandle);
}
