package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure Java shard snapshot payload.
 *
 * <p>This object mirrors Rust {@code cobble::ShardSnapshotMetadata} and is intended for passing
 * snapshot metadata from {@link Db#getShardSnapshot(long)} to {@link
 * DbCoordinator#materializeGlobalSnapshot(int, long, List)}.
 */
public final class ShardSnapshot implements Serializable {
    private static final long serialVersionUID = 874398237491L;
    private static final Gson GSON = new GsonBuilder().create();

    /** Covered bucket ranges for this shard snapshot. */
    @SerializedName("ranges")
    public List<Range> ranges = new ArrayList<Range>();

    /**
     * Column-family IDs from a persisted global-manifest shard reference.
     *
     * <p>Coordinator reports serialize {@link #columnFamilies} instead; this reference-only mapping
     * is deliberately omitted from their JSON.
     */
    @SerializedName("column_family_ids")
    public Map<String, Integer> columnFamilyIds;

    /** Source DB id. */
    @SerializedName("db_id")
    public String dbId;

    /** Source snapshot id in the shard DB. */
    @SerializedName("snapshot_id")
    public long snapshotId;

    /** Full manifest path to shard snapshot manifest. */
    @SerializedName("manifest_path")
    public String manifestPath;

    /** Timestamp (seconds) when the shard snapshot was initiated. */
    @SerializedName("timestamp_seconds")
    public long timestampSeconds;

    /** Total logical snapshot data bytes referenced by this shard snapshot. */
    @SerializedName("data_size_bytes")
    public long dataSizeBytes;

    /** Incremental data bytes newly contributed by this shard snapshot. */
    @SerializedName("incremental_data_size_bytes")
    public long incrementalDataSizeBytes;

    /** Schema version captured at the snapshot boundary. */
    @SerializedName("schema_id")
    public long schemaId;

    /** Captured column-family schema metadata. */
    @SerializedName("column_families")
    public Map<String, SnapshotColumnFamily> columnFamilies =
            new LinkedHashMap<String, SnapshotColumnFamily>();

    public static ShardSnapshot fromJson(String json) {
        return GSON.fromJson(json, ShardSnapshot.class);
    }

    static String listToJson(List<ShardSnapshot> snapshots) {
        return GSON.toJson(snapshots);
    }

    /** Inclusive bucket range. */
    public static final class Range implements Serializable {
        private static final long serialVersionUID = 1L;

        /** Range start bucket. */
        @SerializedName("start")
        public int start;

        /** Range end bucket. */
        @SerializedName("end")
        public int end;
    }

    /** One captured column-family definition. */
    public static final class SnapshotColumnFamily implements Serializable {
        private static final long serialVersionUID = 1L;

        @SerializedName("id")
        public int id;

        @SerializedName("num_columns")
        public int numColumns;

        @SerializedName("options")
        public ColumnFamilyOptions options = new ColumnFamilyOptions();
    }

    /** Column-family options with metadata retained as exact JSON text. */
    public static final class ColumnFamilyOptions implements Serializable {
        private static final long serialVersionUID = 1L;

        @SerializedName("value_has_ttl")
        public boolean valueHasTtl = true;

        @JsonAdapter(RawJsonAdapter.class)
        @SerializedName("metadata")
        public String metadata;
    }

    /** Writes a serializable raw JSON string as its original JSON value. */
    private static final class RawJsonAdapter extends TypeAdapter<String> {
        @Override
        public void write(JsonWriter out, String value) throws java.io.IOException {
            if (value == null) {
                out.nullValue();
                return;
            }
            out.jsonValue(value);
        }

        @Override
        public String read(JsonReader in) throws java.io.IOException {
            if (in.peek() == JsonToken.NULL) {
                in.nextNull();
                return null;
            }
            return JsonParser.parseReader(in).toString();
        }
    }
}
