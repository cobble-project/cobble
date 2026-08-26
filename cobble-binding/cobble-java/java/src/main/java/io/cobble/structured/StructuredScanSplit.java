package io.cobble.structured;

import io.cobble.Config;
import io.cobble.NativeLoader;
import io.cobble.ShardSnapshot;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.lang.reflect.Type;

/**
 * Serializable structured distributed scan split.
 *
 * <p>A split binds one shard snapshot and optional start/end bounds. It can open a structured scan
 * cursor on any node that can access snapshot storage.
 */
public final class StructuredScanSplit implements Serializable {
    private static final Gson GSON =
            new GsonBuilder()
                    .registerTypeHierarchyAdapter(byte[].class, new UnsignedByteArrayAdapter())
                    .create();

    @SerializedName("shard")
    public final ShardSnapshot shard;

    @SerializedName("start")
    public final byte[] start;

    @SerializedName("end")
    public final byte[] end;

    @SerializedName("start_bucket")
    public final Integer startBucket;

    @SerializedName("start_key_exclusive")
    public final byte[] startKeyExclusive;

    @SerializedName("end_bucket")
    public final Integer endBucket;

    @SerializedName("end_key_inclusive")
    public final byte[] endKeyInclusive;

    StructuredScanSplit(ShardSnapshot shard, byte[] start, byte[] end) {
        this(shard, start, end, null, null, null, null);
    }

    StructuredScanSplit(
            ShardSnapshot shard,
            byte[] start,
            byte[] end,
            Integer startBucket,
            byte[] startKeyExclusive,
            Integer endBucket,
            byte[] endKeyInclusive) {
        if (shard == null) {
            throw new IllegalArgumentException("shard must not be null");
        }
        if ((startBucket == null) != (startKeyExclusive == null)) {
            throw new IllegalArgumentException(
                    "startBucket and startKeyExclusive must be set together");
        }
        if ((endBucket == null) != (endKeyInclusive == null)) {
            throw new IllegalArgumentException(
                    "endBucket and endKeyInclusive must be set together");
        }
        this.shard = shard;
        this.start = copyOrNull(start);
        this.end = copyOrNull(end);
        this.startBucket = startBucket;
        this.startKeyExclusive = copyOrNull(startKeyExclusive);
        this.endBucket = endBucket;
        this.endKeyInclusive = copyOrNull(endKeyInclusive);
    }

    /** Result of partitioning one split into the rows before and after a bucket/key boundary. */
    public static final class Partition {
        public final StructuredScanSplit before;
        public final StructuredScanSplit after;

        Partition(StructuredScanSplit before, StructuredScanSplit after) {
            this.before = before;
            this.after = after;
        }
    }

    /** Splits this scan into the rows up to {@code keyInclusive} and the rows after it. */
    public Partition splitAfter(int bucket, byte[] keyInclusive) {
        if (bucket < 0) {
            throw new IllegalArgumentException("bucket must be >= 0");
        }
        if (keyInclusive == null || keyInclusive.length == 0) {
            throw new IllegalArgumentException("keyInclusive must not be empty");
        }
        return new Partition(
                new StructuredScanSplit(
                        shard,
                        start,
                        end,
                        startBucket,
                        startKeyExclusive,
                        Integer.valueOf(bucket),
                        keyInclusive),
                new StructuredScanSplit(
                        shard,
                        start,
                        end,
                        Integer.valueOf(bucket),
                        keyInclusive,
                        endBucket,
                        endKeyInclusive));
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public static StructuredScanSplit fromJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("json must not be empty");
        }
        StructuredScanSplit split = GSON.fromJson(json, StructuredScanSplit.class);
        if (split == null || split.shard == null) {
            throw new IllegalArgumentException("invalid structured scan split json");
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
        long h = openStructuredSplitScanCursor(configPath, toJson(), soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured split scan cursor");
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
        long h = openStructuredSplitScanCursorFromJson(config.toJson(), toJson(), soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured split scan cursor");
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

    private static native long openStructuredSplitScanCursor(
            String configPath, String splitJson, long scanOptionsHandle);

    private static native long openStructuredSplitScanCursorFromJson(
            String configJson, String splitJson, long scanOptionsHandle);

    private static final class UnsignedByteArrayAdapter
            implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {

        @Override
        public JsonElement serialize(
                byte[] src, Type typeOfSrc, JsonSerializationContext jsonSerializationContext) {
            if (src == null) {
                return JsonNull.INSTANCE;
            }
            JsonArray array = new JsonArray(src.length);
            for (byte value : src) {
                array.add(Integer.valueOf(value & 0xFF));
            }
            return array;
        }

        @Override
        public byte[] deserialize(
                JsonElement json,
                Type typeOfT,
                JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            if (json == null || json.isJsonNull()) {
                return null;
            }
            JsonArray array = json.getAsJsonArray();
            byte[] value = new byte[array.size()];
            for (int i = 0; i < array.size(); i++) {
                int current = array.get(i).getAsInt();
                if (current < 0 || current > 255) {
                    throw new JsonParseException("byte value out of range: " + current);
                }
                value[i] = (byte) current;
            }
            return value;
        }
    }
}
