package io.cobble;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Discovers Cobble DBs and produces compaction plans without executing them. */
public final class DedicatedCompactionMonitor extends NativeObject {
    private DedicatedCompactionMonitor(long nativeHandle) {
        super(nativeHandle);
    }

    /** Recursively scans one prefix using a local config file. */
    public static DedicatedCompactionMonitor scan(Path configPath, String root) {
        validateConfigPath(configPath);
        validateRoot(root);
        NativeLoader.load();
        return newMonitor(openScanHandle(configPath.toAbsolutePath().toString(), root));
    }

    /** Recursively scans one local directory or storage prefix. */
    public static DedicatedCompactionMonitor scan(Config config, String root) {
        validateConfig(config);
        validateRoot(root);
        NativeLoader.load();
        return newMonitor(openScanHandleFromJson(config.toJson(), root));
    }

    /** Watches exact DB directories using a local config file. */
    public static DedicatedCompactionMonitor watchDatabases(Path configPath, List<String> paths) {
        validateConfigPath(configPath);
        String[] values = validatePaths(paths);
        NativeLoader.load();
        return newMonitor(openWatchHandle(configPath.toAbsolutePath().toString(), values));
    }

    /** Watches only the supplied DB directories; child or sibling DBs are not discovered. */
    public static DedicatedCompactionMonitor watchDatabases(Config config, List<String> paths) {
        validateConfig(config);
        String[] values = validatePaths(paths);
        NativeLoader.load();
        return newMonitor(openWatchHandleFromJson(config.toJson(), values));
    }

    private static String[] validatePaths(List<String> paths) {
        if (paths == null || paths.isEmpty()) {
            throw new IllegalArgumentException("paths must not be empty");
        }
        String[] values = paths.toArray(new String[0]);
        for (String path : values) {
            if (path == null || path.trim().isEmpty()) {
                throw new IllegalArgumentException("paths must not contain blank values");
            }
        }
        return values;
    }

    private static DedicatedCompactionMonitor newMonitor(long handle) {
        if (handle == 0L) {
            throw new IllegalStateException("failed to open dedicated compaction monitor");
        }
        return new DedicatedCompactionMonitor(handle);
    }

    /** Returns newly planned work. Plans remain outstanding until published, stale, or released. */
    public synchronized List<DedicatedCompactionPlan> poll() {
        byte[][] encoded = pollEncoded(nativeHandle);
        List<DedicatedCompactionPlan> plans =
                new ArrayList<DedicatedCompactionPlan>(encoded.length);
        for (byte[] bytes : encoded) {
            plans.add(new DedicatedCompactionPlan(bytes));
        }
        return plans;
    }

    /** Releases a plan after a caller-owned execution attempt that produced no durable result. */
    public synchronized void complete(DedicatedCompactionPlan plan) {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }
        completeInternal(nativeHandle, plan.jobId());
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static void validateConfig(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
    }

    private static void validateConfigPath(Path configPath) {
        if (configPath == null) {
            throw new IllegalArgumentException("configPath must not be null");
        }
    }

    private static void validateRoot(String root) {
        if (root == null || root.trim().isEmpty()) {
            throw new IllegalArgumentException("root must not be blank");
        }
    }

    private static native long openScanHandle(String configPath, String root);

    private static native long openScanHandleFromJson(String configJson, String root);

    private static native long openWatchHandle(String configPath, String[] paths);

    private static native long openWatchHandleFromJson(String configJson, String[] paths);

    private static native byte[][] pollEncoded(long nativeHandle);

    private static native void completeInternal(long nativeHandle, String jobId);
}
