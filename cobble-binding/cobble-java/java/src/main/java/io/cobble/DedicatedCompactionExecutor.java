package io.cobble;

import java.nio.file.Path;

/** Executes portable dedicated compaction plans independently from monitoring and planning. */
public final class DedicatedCompactionExecutor extends NativeObject {
    public enum Outcome {
        RESULT_PUBLISHED,
        WAITING_FOR_RESULT,
        STALE
    }

    private DedicatedCompactionExecutor(long nativeHandle) {
        super(nativeHandle);
    }

    public static DedicatedCompactionExecutor open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        return newExecutor(openHandle(config.toJson()));
    }

    public static DedicatedCompactionExecutor open(Path configPath) {
        if (configPath == null) {
            throw new IllegalArgumentException("configPath must not be null");
        }
        NativeLoader.load();
        return newExecutor(openHandleFromPath(configPath.toAbsolutePath().toString()));
    }

    private static DedicatedCompactionExecutor newExecutor(long handle) {
        if (handle == 0L) {
            throw new IllegalStateException("failed to open dedicated compaction executor");
        }
        return new DedicatedCompactionExecutor(handle);
    }

    /** Revalidates and executes one queued plan. */
    public synchronized Outcome execute(DedicatedCompactionPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }
        return decodeOutcome(executeInternal(nativeHandle, plan.encodedUnsafe()));
    }

    private static Outcome decodeOutcome(int outcome) {
        switch (outcome) {
            case 0:
                return Outcome.RESULT_PUBLISHED;
            case 1:
                return Outcome.WAITING_FOR_RESULT;
            case 2:
                return Outcome.STALE;
            default:
                throw new IllegalStateException("unknown dedicated compaction outcome " + outcome);
        }
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configJson);

    private static native long openHandleFromPath(String configPath);

    private static native int executeInternal(long nativeHandle, byte[] encodedPlan);
}
