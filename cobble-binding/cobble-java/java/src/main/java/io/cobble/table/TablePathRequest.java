package io.cobble.table;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable generic input for resolving a readable table location before format selection. */
public final class TablePathRequest {
    private final String path;
    private final String tableName;
    private final Long snapshotId;
    private final Map<String, String> options;

    public TablePathRequest(
            String path, String tableName, Long snapshotId, Map<String, String> options) {
        this.path = TableScanSplit.requireText(path, "path");
        this.tableName = TableScanSplit.requireText(tableName, "tableName");
        if (snapshotId != null && snapshotId.longValue() < 0L) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
        this.snapshotId = snapshotId;
        LinkedHashMap<String, String> copy = new LinkedHashMap<String, String>();
        if (options != null) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                copy.put(
                        TableScanSplit.requireText(entry.getKey(), "option name"),
                        Objects.requireNonNull(entry.getValue(), "option value"));
            }
        }
        this.options = Collections.unmodifiableMap(copy);
    }

    public String path() {
        return path;
    }

    public String tableName() {
        return tableName;
    }

    /** Nullable native global snapshot selection. Format resolvers retain their own identities. */
    public Long snapshotId() {
        return snapshotId;
    }

    /** Opaque resolver options. The core never interprets plugin-specific values. */
    public Map<String, String> options() {
        return options;
    }

    public String option(String name) {
        return options.get(name);
    }
}
