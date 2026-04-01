package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

/**
 * Serializable distributed scan split for raw Db scanning.
 *
 * <p>A split binds one shard snapshot and optional start/end bounds. It can open a scan cursor on
 * any node that can access snapshot storage.
 */
public final class ScanSplit implements Serializable {
    private static final Gson GSON = new GsonBuilder().create();

    @SerializedName("shard")
    public final ShardSnapshot shard;

    @SerializedName("start")
    public final byte[] start;

    @SerializedName("end")
    public final byte[] end;

    ScanSplit(ShardSnapshot shard, byte[] start, byte[] end) {
        if (shard == null) {
            throw new IllegalArgumentException("shard must not be null");
        }
        this.shard = shard;
        this.start = copyOrNull(start);
        this.end = copyOrNull(end);
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public static ScanSplit fromJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("json must not be empty");
        }
        ScanSplit split = GSON.fromJson(json, ScanSplit.class);
        if (split == null || split.shard == null) {
            throw new IllegalArgumentException("invalid scan split json");
        }
        return split;
    }

    public ScanCursor openScanner(String configPath) {
        return openScannerWithOptions(configPath, null);
    }

    public ScanCursor openScannerWithOptions(String configPath, ScanOptions options) {
        if (configPath == null || configPath.trim().isEmpty()) {
            throw new IllegalArgumentException("configPath must not be empty");
        }
        NativeLoader.load();
        long soh = options == null ? 0L : options.getNativeHandle();
        long h = openSplitScanCursor(configPath, toJson(), soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open split scan cursor");
        }
        return new ScanCursor(h);
    }

    public ScanCursor openScanner(Config config) {
        return openScannerWithOptions(config, null);
    }

    public ScanCursor openScannerWithOptions(Config config, ScanOptions options) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long soh = options == null ? 0L : options.getNativeHandle();
        long h = openSplitScanCursorFromJson(config.toJson(), toJson(), soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open split scan cursor");
        }
        return new ScanCursor(h);
    }

    private static byte[] copyOrNull(byte[] value) {
        if (value == null) {
            return null;
        }
        byte[] copied = new byte[value.length];
        System.arraycopy(value, 0, copied, 0, value.length);
        return copied;
    }

    private static native long openSplitScanCursor(
            String configPath, String splitJson, long scanOptionsHandle);

    private static native long openSplitScanCursorFromJson(
            String configJson, String splitJson, long scanOptionsHandle);
}
