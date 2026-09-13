package io.cobble.table;

import io.cobble.Config;

import java.io.Serializable;

/** Serializable portable writer plan pinned to one catalog schema version. */
public final class TableWritePlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String planJson;

    private TableWritePlan(String planJson) {
        this.planJson = planJson;
    }

    String nativePlanJson() {
        return planJson;
    }

    /** Starts a catalog-independent writer builder using this fixed table definition. */
    public TableWriterBuilder writerBuilder(Config runtime) {
        return TableWriterBuilder.fromPlan(this, runtime);
    }

    static TableWritePlan fromNativeJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalStateException("failed to build table write plan");
        }
        return new TableWritePlan(json);
    }

    static native Table writerOpenNative(
            String planJson,
            String runtimeJson,
            String dbId,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive,
            int mode,
            long snapshotId);
}
