package io.cobble.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.ShardSnapshot;
import io.cobble.SnapshotTools;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Hydrated, fixed snapshot metadata for one selected column family. */
public final class TableReadSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;
    private final GlobalSnapshot globalSnapshot;
    private final List<ShardDescriptor> shards;
    private final String columnFamily;
    private final String formatId;

    private TableReadSnapshot(
            GlobalSnapshot globalSnapshot,
            List<ShardDescriptor> shards,
            String columnFamily,
            String formatId) {
        this.globalSnapshot = Objects.requireNonNull(globalSnapshot, "globalSnapshot").copy();
        this.shards = Collections.unmodifiableList(new ArrayList<ShardDescriptor>(shards));
        this.columnFamily = TableScanSplit.requireText(columnFamily, "columnFamily");
        this.formatId = TableScanSplit.requireText(formatId, "formatId");
        if (this.shards.isEmpty()) {
            throw new IllegalArgumentException(
                    "fixed snapshot has no selected column-family shard metadata");
        }
    }

    /** Hydrates persisted global-shard references and selects one column family. */
    public static TableReadSnapshot forGlobal(
            Config config, GlobalSnapshot globalSnapshot, String columnFamily) {
        if (config == null) throw new IllegalArgumentException("config must not be null");
        if (globalSnapshot == null)
            throw new IllegalArgumentException("globalSnapshot must not be null");
        return create(config, globalSnapshot, globalSnapshot.shardSnapshots, columnFamily);
    }

    private static TableReadSnapshot create(
            Config config,
            GlobalSnapshot globalSnapshot,
            List<ShardSnapshot> shardSnapshots,
            String columnFamily) {
        String family = TableScanSplit.requireText(columnFamily, "columnFamily");
        if (shardSnapshots == null || shardSnapshots.isEmpty()) {
            throw new IllegalArgumentException("fixed snapshot has no shard snapshots");
        }
        List<ShardDescriptor> selected = new ArrayList<ShardDescriptor>();
        String selectedFormat = null;
        for (ShardSnapshot reference : shardSnapshots) {
            ShardSnapshot shard =
                    hydrate(config, Objects.requireNonNull(reference, "shardSnapshot"));
            ShardSnapshot.SnapshotColumnFamily familyMetadata = shard.columnFamilies.get(family);
            if (familyMetadata == null) {
                continue;
            }
            if (familyMetadata.options == null || familyMetadata.options.metadata == null) {
                throw new IllegalArgumentException(
                        "fixed shard snapshot "
                                + shard.snapshotId
                                + " has column family '"
                                + family
                                + "' without embedded table read metadata");
            }
            String metadata = normalizeMetadata(family, familyMetadata.options.metadata);
            String format = formatId(family, metadata);
            if (selectedFormat == null) {
                selectedFormat = format;
            } else if (!selectedFormat.equals(format)) {
                throw new IllegalArgumentException(
                        "fixed snapshot column family '"
                                + family
                                + "' has conflicting format ids '"
                                + selectedFormat
                                + "' and '"
                                + format
                                + "'");
            }
            selected.add(new ShardDescriptor(shard, metadata));
        }
        if (selectedFormat == null) {
            throw new IllegalArgumentException(
                    "fixed snapshot has no column family named '" + family + "'");
        }
        return new TableReadSnapshot(globalSnapshot, selected, family, selectedFormat);
    }

    private static ShardSnapshot hydrate(Config config, ShardSnapshot shard) {
        if (shard.columnFamilies != null && !shard.columnFamilies.isEmpty()) {
            return shard;
        }
        if (shard.dbId == null || shard.dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("thin shard snapshot is missing dbId");
        }
        if (shard.manifestPath == null || shard.manifestPath.trim().isEmpty()) {
            throw new IllegalArgumentException("thin shard snapshot is missing manifestPath");
        }
        ShardSnapshot loaded =
                SnapshotTools.loadShardSnapshot(config, shard.dbId, shard.manifestPath);
        if (!shard.dbId.equals(loaded.dbId) || shard.snapshotId != loaded.snapshotId) {
            throw new IllegalArgumentException(
                    "fixed shard snapshot reference does not match its persisted manifest");
        }
        // A global snapshot may trim a shard's physical range during rescale. The global manifest
        // is the assignment authority; the shard manifest only supplies immutable CF metadata.
        if (shard.ranges != null && !shard.ranges.isEmpty()) {
            loaded.ranges = shard.copy().ranges;
        }
        return loaded;
    }

    private static String normalizeMetadata(String family, String metadata) {
        try {
            JsonElement parsed = JsonParser.parseString(metadata);
            if (!parsed.isJsonObject()) {
                throw new IllegalArgumentException("metadata is not an object");
            }
            return parsed.toString();
        } catch (RuntimeException error) {
            throw new IllegalArgumentException(
                    "fixed snapshot column family '"
                            + family
                            + "' has invalid embedded table read metadata",
                    error);
        }
    }

    private static String formatId(String family, String metadata) {
        JsonObject object = JsonParser.parseString(metadata).getAsJsonObject();
        JsonElement format = object.get("format");
        if (format == null
                || !format.isJsonPrimitive()
                || !format.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException(
                    "fixed snapshot column family '"
                            + family
                            + "' metadata is missing string format id");
        }
        return TableScanSplit.requireText(format.getAsString(), "snapshot format id");
    }

    /** Fixed global routing metadata for this read. */
    public GlobalSnapshot globalSnapshot() {
        return globalSnapshot.copy();
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String formatId() {
        return formatId;
    }

    /**
     * Only shards that carry the selected family; absent-family shards are intentionally skipped.
     */
    public List<ShardDescriptor> shards() {
        return shards;
    }

    /** One hydrated shard plus its own captured descriptor JSON. */
    public static final class ShardDescriptor implements Serializable {
        private static final long serialVersionUID = 1L;
        private final ShardSnapshot snapshot;
        private final String metadataJson;

        private ShardDescriptor(ShardSnapshot snapshot, String metadataJson) {
            this.snapshot = snapshot.copy();
            this.metadataJson = metadataJson;
        }

        public ShardSnapshot snapshot() {
            return snapshot.copy();
        }

        public String metadataJson() {
            return metadataJson;
        }
    }
}
