package io.cobble.table;

import java.util.Objects;

/** Builds a portable writer initialization plan from one fixed catalog table version. */
public final class TableWriteBuilder {
    private final CatalogTable table;
    private Integer totalBuckets;

    TableWriteBuilder(CatalogTable table) {
        this.table = Objects.requireNonNull(table, "table");
    }

    /** Sets the table-wide bucket count carried by the plan. */
    public TableWriteBuilder totalBuckets(int value) {
        if (value < 1 || value > 65536) {
            throw new IllegalArgumentException("totalBuckets must be in range 1..=65536");
        }
        totalBuckets = value;
        return this;
    }

    /** Freezes the captured catalog schema and shared storage locations for worker use. */
    public TableWritePlan build() {
        synchronized (table) {
            return TableWritePlan.fromNativeJson(
                    CatalogTable.buildWritePlanNative(
                            table.nativeHandleForBuilder(),
                            totalBuckets == null ? -1 : totalBuckets));
        }
    }
}
